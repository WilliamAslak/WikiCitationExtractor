import asyncio
import logging
import traceback
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException, Depends
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker, selectinload
from sqlalchemy import select, func
from sqlalchemy.exc import IntegrityError
import httpx
from collections import defaultdict
import re

from models import Base, Article, Reference, EntityLink
from scraper import scrape_targets
from qlever import (
    resolve_references_batch,
    get_author_works,
    get_work_authors,
    get_citations_to_work,
    get_citations_for_author,
    get_female_dtu_researchers,
    get_entity_classification,
    get_entity_context
)
from config import settings

engine = create_async_engine(settings.database_url, echo=False)
AsyncSessionLocal = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)


@asynccontextmanager
async def lifespan(app: FastAPI):
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    yield

# Concurrency state trackers
active_scrapes = set()

app = FastAPI(title="Wiki Citation Extractor", lifespan=lifespan)


async def get_db():
    async with AsyncSessionLocal() as session:
        yield session

@app.get("/")
async def root():
    return {
        "message": "Welcome to the Wiki Citation Extractor API",
        "endpoints": {
            "/ok/": "Health check to verify the API is running.",
            "/refresh/": "Triggers a scrape of each item in the database.",
            "/scrape/{q_id}/": "Triggers a full Wikipedia scrape and database sync for a specific Wikidata Q-ID.",
            "/database/dump/": "Returns a summary of all articles currently stored in the local database.",
            "/database/{q_id}/": "Returns the complete stored reference data for a specific Q-ID.",
            "/stats/": "Provides database statistics of 'cite q' template usage.",
            "/author/{q_id}/": "Fetches all works by an author from Wikidata and checks if they exist in our local references.",
            "/referenced/{q_id}/": "Finds where a specific entity (author or work) is cited and cross-references with our local database.",
            "/example/1/": "Fetches a list of the most wikipedia referenced female employees of DTU"
        }
    }

async def sync_article_by_qid(q_id: str, db: AsyncSession, rescrape = False):
    q_id = q_id.strip().upper()

    global active_scrapes
    if q_id in active_scrapes:
        raise HTTPException(status_code=409, detail=f"A scrape for {q_id} is already in progress.")

    active_scrapes.add(q_id)
    try:
        if not re.match(r"^Q\d+$", q_id):
            raise HTTPException(status_code=400, detail="Invalid Q-ID format.")

        try:
            context = await get_entity_context(q_id)
            if not context.get("labels") and not context.get("works") and not context.get("doi"):
                raise HTTPException(status_code=404, detail="Wikidata entity does not exist.")

            entity_type_raw = await get_entity_classification(q_id)
            if entity_type_raw in ["scholarly article", "book", "publication", "article"]:
                mapped_type = "work"
            elif entity_type_raw == "person":
                mapped_type = "person"
            else:
                mapped_type = "unknown"

            primary_label = q_id
            if context.get("labels"):
                en_label = next((l["title"] for l in context["labels"] if l["lang"] == "en"), None)
                primary_label = en_label if en_label else context["labels"][0]["title"]

            targets = [{
                "q_id": q_id,
                "label": primary_label,
                "type": mapped_type,
                "doi": context.get("doi"),
                "pmid": context.get("pmid"),
                "arxiv": context.get("arxiv")
            }]

            for w in context.get("works", []):
                targets.append({
                    "q_id": w["work_qid"],
                    "label": w.get("label", ""),
                    "type": "work",
                    "doi": w.get("doi"),
                    "pmid": w.get("pmid"),
                    "arxiv": w.get("arxiv")
                })

            all_target_qids = [t["q_id"] for t in targets]
            existing_res = await db.execute(select(Article.q_id).where(Article.q_id.in_(all_target_qids)))
            existing_qids = set(existing_res.scalars().all())

            # a simple check to determine if everything should be scraped or not.
            targets_to_scrape = targets if rescrape else [t for t in targets if t["q_id"] == q_id or t["q_id"] not in existing_qids]


            scraped_refs = await scrape_targets(targets_to_scrape, q_id)

        except HTTPException:
        # Allow intentional HTTP exceptions (like a 404) to pass through without printing a crash trace
            raise
        except Exception as e:
            print("\n--- CRASH IN SCRAPER ---")
            traceback.print_exc()
            print("------------------------\n")
            raise HTTPException(status_code=400, detail=f"Scraping failed: {e}")

        # Scraping takes time. If the user refreshed the page, a parallel request
        # might have already inserted this article. Committing here closes our old
        # read-snapshot and forces the DB to give us the freshest data.
        await db.commit()

        dois_to_resolve = list(set(r["doi"] for r in scraped_refs if not r.get("q_id") and r.get("doi")))
        qids_to_resolve = list(set(
            r["q_id"] for r in scraped_refs if r.get("q_id") and not any([r.get("doi"), r.get("pmid"), r.get("arxiv")])))

        resolved_dois, resolved_qids = await resolve_references_batch(dois_to_resolve, qids_to_resolve)

        refs_by_target = defaultdict(list)
        for ref_data in scraped_refs:
            ref_q_id = ref_data.get("q_id")
            if not ref_q_id and ref_data.get("doi"):
                ref_q_id = resolved_dois.get(ref_data["doi"])

            if ref_q_id:
                if ref_q_id in resolved_qids:
                    identifiers = resolved_qids[ref_q_id]
                    ref_data["doi"] = ref_data.get("doi") or identifiers.get("doi")
                    ref_data["pmid"] = ref_data.get("pmid") or identifiers.get("pmid")
                    ref_data["arxiv"] = ref_data.get("arxiv") or identifiers.get("arxiv")
                refs_by_target[ref_q_id].append(ref_data)

        new_refs_count = 0
        updated_refs_count = 0

        for t in targets:
            t_qid = t["q_id"]

            stmt = select(Article).options(selectinload(Article.references)).where(Article.q_id == t_qid)
            article = (await db.execute(stmt)).scalars().first()

            if not article:
                article = Article(q_id=t_qid, name=t["label"], entity_type=t["type"])
                db.add(article)
                await db.flush()
                existing_refs_map = {}
            else:
                article.name = t["label"]
                if article.entity_type == "unknown" and t["type"] != "unknown":
                    article.entity_type = t["type"]
                existing_refs_map = {(r.raw_text, r.language, r.context_text): r for r in article.references}

            # Only modify references if this specific target was fetched in this run
            if t_qid in [ts["q_id"] for ts in targets_to_scrape]:
                target_refs = refs_by_target.get(t_qid, [])
                for ref_data in target_refs:
                    map_key = (ref_data["raw_text"], ref_data["language"], ref_data["context_text"])
                    if map_key in existing_refs_map:
                        ref = existing_refs_map[map_key]
                        ref.ref_type = ref_data["ref_type"]
                        ref.ref_name = ref_data.get("ref_name", "")
                        ref.doi = ref_data["doi"]
                        ref.pmid = ref_data["pmid"]
                        ref.arxiv = ref_data["arxiv"]
                        ref.context_text = ref_data["context_text"]
                        updated_refs_count += 1
                    else:
                        ref = Reference(
                            article_q_id=article.q_id,
                            q_id=t_qid,
                            language=ref_data["language"],
                            source_url=ref_data["source_url"],
                            raw_text=ref_data["raw_text"],
                            context_text=ref_data["context_text"],
                            ref_type=ref_data["ref_type"],
                            ref_name=ref_data.get("ref_name", ""),
                            doi=ref_data["doi"],
                            pmid=ref_data["pmid"],
                            arxiv=ref_data["arxiv"],
                        )
                        db.add(ref)
                        new_refs_count += 1

            if t_qid != q_id:
                link_stmt = select(EntityLink).where(EntityLink.parent_q_id == q_id, EntityLink.child_q_id == t_qid)
                if not (await db.execute(link_stmt)).scalars().first():
                    db.add(EntityLink(parent_q_id=q_id, child_q_id=t_qid))

        await db.commit()

        return {
            "message": "Article sync complete.",
            "article_q_id": q_id,
            "targets_scraped": len(targets_to_scrape),
            "new_references_added": new_refs_count,
            "existing_references_updated": updated_refs_count,
        }
    finally:
        active_scrapes.discard(q_id)

@app.get("/ok/")
async def health_check():
    return {"status": "ok"}

@app.get("/refresh/")
async def refresh_all_articles(db: AsyncSession = Depends(get_db)):
    global active_scrapes
    if "refresh" in active_scrapes:
        raise HTTPException(status_code=409, detail="A global database refresh is already in progress.")

    active_scrapes.add("refresh")
    try:
        """Refreshes all records. Prioritizes works so people syncs can skip previously scraped works."""
        stmt_works = select(Article.q_id).where(Article.entity_type == 'work')
        stmt_people = select(Article.q_id).where(Article.entity_type == 'person')
        stmt_unknown = select(Article.q_id).where(Article.entity_type == 'unknown')

        works_qids = (await db.execute(stmt_works)).scalars().all()
        people_qids = (await db.execute(stmt_people)).scalars().all()
        unknown_qids = (await db.execute(stmt_unknown)).scalars().all()

        all_qids = list(works_qids) + list(unknown_qids) + list(people_qids)
        total_articles = len(all_qids)
        results = []

        for index, q_id in enumerate(all_qids):
            try:
                sync_result = await sync_article_by_qid(q_id, db)
                results.append({
                    "q_id": q_id,
                    "status": "success",
                    "new_references": sync_result["new_references_added"],
                    "updated_references": sync_result["existing_references_updated"]
                })
            except Exception as e:
                logging.error(f"Failed to refresh {q_id}: {e}")
                results.append({"q_id": q_id, "status": "failed", "error": str(e)})

            if index % 5 == 0:
                print(f"{total_articles - index} q_ids left to scrape")

        count = [0,0]
        for item in results:
            count[0] += item["new_references"]
            count[1] += item["updated_references"]

        return {
            "message": "Database refresh complete.",
            "total_processed": total_articles,
            "details": f"{count[0]} new references has been added and {count[1]} references has been updated"
        }
    finally:
        active_scrapes.discard("refresh")


@app.get("/scrape/{q_id}/")
async def scrape_article_endpoint(q_id: str, db: AsyncSession = Depends(get_db)):
    return await sync_article_by_qid(q_id, db, rescrape = True)


@app.get("/database/dump/")
async def dump_entire_database(db: AsyncSession = Depends(get_db)):
    # Fetch all articles with their references
    stmt = select(Article).options(selectinload(Article.references))
    result = await db.execute(stmt)
    articles = result.scalars().all()

    # Create a dictionary for quick lookup of articles by their Q-ID
    article_map = {article.q_id: article for article in articles}

    # Fetch all parent-child links
    links_stmt = select(EntityLink)
    links_result = await db.execute(links_stmt)

    # Map linked works (children) to their author (parent Q-ID)
    children_map = defaultdict(list)
    for link in links_result.scalars().all():
        children_map[link.parent_q_id].append(link.child_q_id)

    database_dump = []
    for article in articles:
        #if article.entity_type == "person":

            # Start with the person's own direct references
            total_refs = len(article.references)

            # Add references from all their linked works
            for child_q_id in children_map.get(article.q_id, []):
                if child_q_id in article_map:
                    total_refs += len(article_map[child_q_id].references)

            # only display items that have been referenced in a wikipedia article
            if total_refs == 0:
                continue

            database_dump.append({
                "q_id": article.q_id,
                "entity_name": article.name,
                "entity_type": article.entity_type,
                "references_count": total_refs
            })

    return {"total_entities": len(database_dump), "data": database_dump}

@app.get("/database/{q_id}/")
async def get_article_by_qid(q_id: str, db: AsyncSession = Depends(get_db)):
    """Local-only check. Returns the requested entity and aggregates references for any linked works."""
    q_id = q_id.strip().upper()

    stmt = select(Article).options(selectinload(Article.references)).where(Article.q_id == q_id)
    result = await db.execute(stmt)
    article = result.scalars().first()

    if not article:
        raise HTTPException(status_code=404, detail=f"Q-ID {q_id} not found in the local database.")

    child_stmt = select(EntityLink.child_q_id).where(EntityLink.parent_q_id == q_id)
    child_qids = (await db.execute(child_stmt)).scalars().all()

    all_references = []

    def format_ref(reference):
        return {
            "source_url": reference.source_url,
            "language": reference.language,
            "raw_text": reference.raw_text,
            "context_text": reference.context_text,
            "ref_type": reference.ref_type,
            "ref_name": reference.ref_name,
            "doi": reference.doi,
            "pmid": reference.pmid,
            "arxiv": reference.arxiv,
            "q_id": reference.q_id,
        }

    for ref in article.references:
        all_references.append(format_ref(ref))

    if child_qids:
        children_stmt = select(Article).options(selectinload(Article.references)).where(Article.q_id.in_(child_qids))
        children = (await db.execute(children_stmt)).scalars().all()
        for child in children:
            for ref in child.references:
                all_references.append(format_ref(ref))

    return {
        "q_id": article.q_id,
        "name": article.name,
        "entity_type": article.entity_type,
        "total_references": len(all_references),
        "references": all_references
    }

@app.get("/stats/")
async def get_stats(db: AsyncSession = Depends(get_db)):
    result = await db.execute(
        select(func.count(Reference.id)).where(Reference.ref_type == "cite q")
    )
    cite_q_count = result.scalar()
    return {"cite_q_usage_count": cite_q_count}


@app.get("/author/{q_id}/")
async def search_author(q_id: str, db: AsyncSession = Depends(get_db)):
    q_id = q_id.strip().upper()
    works_data = await get_author_works(q_id)

    if not works_data:
        authors_data = await get_work_authors(q_id)
        if authors_data:
            author_q_ids = {author["label"]: author["author_qid"] for author in authors_data}
            return {
                "work_q_id": q_id,
                "resolved_as": "article",
                "author_q_ids": author_q_ids
            }

    work_qids = [work["work_qid"] for work in works_data]

    local_refs = []
    if work_qids:
        stmt = (select(Reference).where(Reference.q_id.in_(work_qids)))
        result = await db.execute(stmt)
        local_refs = result.scalars().all()

    combined_works = []
    for work in works_data:
        work_qid = work["work_qid"]
        matching_local_refs = [
            {
                "raw_text": ref.raw_text,
                "context_text": ref.context_text,
                "language": ref.language,
                "source_url": ref.source_url,
            }
            for ref in local_refs
            if ref.q_id == work_qid
        ]
        combined_works.append({
            "work_q_id": work_qid,
            "work_label": work["label"],
            "has_local_matches": len(matching_local_refs) > 0,
            "local_references": matching_local_refs,
        })

    return {
        "author_q_id": q_id,
        "resolved_as": "author",
        "total_works": len(combined_works),
        "works": combined_works,
    }


@app.get("/referenced/{q_id}/")
async def get_references_to_entity(q_id: str, db: AsyncSession = Depends(get_db)):
    q_id = q_id.strip().upper()

    # Try treating the Q-ID as an Author first
    entity_type = "author"
    citations_data = await get_citations_for_author(q_id)

    # Fallback: treat the Q-ID as a piece of work
    if not citations_data:
        entity_type = "work"
        citations_data = await get_citations_to_work(q_id)

    citing_qids = list(set(citation["citing_work_qid"] for citation in citations_data))

    local_refs = []
    if citing_qids:
        stmt = (
            select(Reference).where(Reference.q_id.in_(citing_qids))
        )
        result = await db.execute(stmt)
        local_refs = result.scalars().all()

    combined_citations = []
    for citation in citations_data:
        citing_qid = citation["citing_work_qid"]
        matching_local_refs = [
            {
                "raw_text": ref.raw_text,
                "context_text": ref.context_text,
                "source_url": ref.source_url,
                "language": ref.language,
            }
            for ref in local_refs
            if ref.q_id == citing_qid
        ]
        combined_citations.append({
            "citing_work_q_id": citing_qid,
            "citing_work_label": citation["citing_work_label"],
            "original_work_q_id": citation["original_work_qid"],
            "original_work_label": citation["original_work_label"],
            "has_local_matches": len(matching_local_refs) > 0,
            "local_references": matching_local_refs,
        })

    return {
        "entity_q_id": q_id,
        "resolved_as": entity_type,
        "total_citations": len(combined_citations),
        "citations": combined_citations,
    }


@app.get("/example/1/")
async def example_most_referenced_female_dtu(db: AsyncSession = Depends(get_db)):
    """
    Finds all female researchers at DTU. Checks the local database for their citations.
    If missing, scrapes Wikipedia and saves to DB continually.
    Prints progress to the console and resumes where it left off if interrupted.
    """

    global active_scrapes
    if "example_1" in active_scrapes:
        raise HTTPException(status_code=409, detail="The DTU example scrape is already in progress.")

    active_scrapes.add("example_1")

    try:
        # Fetch all researchers by allowing the default 10000 limit
        researchers = await get_female_dtu_researchers()
        total_researchers = len(researchers)

        print(f"\n--- Starting bulk scrape of {total_researchers} female DTU researchers ---")

        results = []
        for index, researcher in enumerate(researchers):
            q_id = researcher["q_id"]
            researcher_name = researcher["name"]

            stmt = select(Article).options(selectinload(Article.references)).where(Article.q_id == q_id)
            result = await db.execute(stmt)
            article = result.scalars().first()

            if not article:
                try:
                    # Scrapes Wikipedia and immediately commits to the database
                    await sync_article_by_qid(q_id, db)

                    result = await db.execute(stmt)
                    article = result.scalars().first()
                    #print("Done.")
                except Exception as e:
                    logging.error(f"Failed! Error: {e}")

            if article:
                ref_count = (await db.execute(select(func.count(Reference.id)).where((Reference.article_q_id == q_id) |
                    Reference.article_q_id.in_(select(EntityLink.child_q_id).where(EntityLink.parent_q_id == q_id))))).scalar()
            else:
                ref_count = 0

            #print(f"scraped {article.q_id}:{article.name} and found {ref_count} reference{"s" if ref_count!=1 else ""}.")

            results.append({
                "q_id": q_id,
                "name": article.name if article else researcher_name,
                "reference_count": ref_count
            })

            if index % 5 == 0:
                print(f"{(max(1,index)/total_researchers)*100:.2f}% complete. ({total_researchers-index} researchers remaining)")

        print("--- Bulk scrape finished ---\n")

        results.sort(key=lambda x: x["reference_count"], reverse=True)

        return {
            "query": "Most referenced female researchers at DTU",
            "total_processed": len(results),
            "results": results
        }
    finally:
        active_scrapes.discard("example_1")