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

from models import Base, Article, Reference, WikidataMapping
from scraper import scrape_all_languages_for_qid
from qlever import (
    resolve_references_batch,
    get_author_works,
    get_citations_to_work,
    get_citations_for_author, get_female_dtu_researchers,
)
from config import settings

engine = create_async_engine(settings.database_url, echo=False)
AsyncSessionLocal = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)


@asynccontextmanager
async def lifespan(app: FastAPI):
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    yield


app = FastAPI(title="Wiki Citation Extractor", lifespan=lifespan)


async def get_db():
    async with AsyncSessionLocal() as session:
        yield session


@app.get("/ok/")
async def health_check():
    return {"status": "ok"}

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

async def sync_article_by_qid(q_id: str, db: AsyncSession):
    q_id = q_id.strip().upper()
    try:
        scraped_data = await scrape_all_languages_for_qid(q_id)
    except Exception as e:
        print("\n--- CRASH IN SCRAPER ---")
        traceback.print_exc()
        print("------------------------\n")
        raise HTTPException(status_code=400, detail=f"Scraping failed: {e}")

    # Scraping takes time. If the user refreshed the page, a parallel request
    # might have already inserted this article. Committing here closes our old
    # read-snapshot and forces the DB to give us the freshest data.
    await db.commit()

    stmt = (
        select(Article)
        .options(selectinload(Article.references).selectinload(Reference.wikidata_mapping))
        .where(Article.q_id == q_id)
    )

    result = await db.execute(stmt)
    article = result.scalars().first()

    existing_refs_map = {}

    if not article:
        # Pass the scraped name to the new Article
        article = Article(q_id=q_id, name=scraped_data["name"])
        db.add(article)
        try:
            await db.flush()
        except IntegrityError:
            await db.rollback()
            result = await db.execute(stmt)
            article = result.scalars().first()
            article.name = scraped_data["name"]
            for ref in article.references:
                existing_refs_map[(ref.raw_text, ref.language)] = ref
    else:
        article.name = scraped_data["name"]
        for ref in article.references:
            existing_refs_map[(ref.raw_text, ref.language)] = ref


    new_refs_count = 0
    updated_refs_count = 0

    dois_to_resolve = list(set(
        r["doi"] for r in scraped_data["references"]
        if not r["q_id"] and r["doi"]
    ))
    qids_to_resolve = list(set(
        r["q_id"] for r in scraped_data["references"]
        if r["q_id"] and not all([r.get("doi"), r.get("pmid"), r.get("arxiv")])
    ))

    # Single QLever round-trip replaces the two sequential calls that were here.
    resolved_dois, resolved_qids = await resolve_references_batch(
        dois_to_resolve, qids_to_resolve
    )

    for ref_data in scraped_data["references"]:
        raw_text = ref_data["raw_text"]
        context_text = ref_data["context_text"]
        ref_q_id = ref_data["q_id"]
        language = ref_data["language"]

        if ref_q_id and ref_q_id in resolved_qids:
            identifiers = resolved_qids[ref_q_id]
            ref_data["doi"] = ref_data["doi"] or identifiers.get("doi")
            ref_data["pmid"] = ref_data["pmid"] or identifiers.get("pmid")
            ref_data["arxiv"] = ref_data["arxiv"] or identifiers.get("arxiv")

        if not ref_q_id and ref_data["doi"]:
            ref_q_id = resolved_dois.get(ref_data["doi"])

        map_key = (raw_text, language)

        if map_key in existing_refs_map:
            ref = existing_refs_map[map_key]
            ref.ref_type = ref_data["ref_type"]
            ref.ref_name = ref_data["ref_name"]
            ref.doi = ref_data["doi"]
            ref.pmid = ref_data["pmid"]
            ref.arxiv = ref_data["arxiv"]
            ref.context_text = context_text

            if ref_q_id:
                if ref.wikidata_mapping:
                    ref.wikidata_mapping.q_id = ref_q_id
                else:
                    mapping = WikidataMapping(reference_id=ref.id, q_id=ref_q_id)
                    db.add(mapping)

            updated_refs_count += 1
        else:
            ref = Reference(
                article_q_id=article.q_id,
                language=language,
                source_url=ref_data["source_url"],
                raw_text=raw_text,
                context_text=context_text,
                ref_type=ref_data["ref_type"],
                ref_name=ref_data["ref_name"],
                doi=ref_data["doi"],
                pmid=ref_data["pmid"],
                arxiv=ref_data["arxiv"],
            )
            db.add(ref)
            await db.flush()

            if ref_q_id:
                mapping = WikidataMapping(reference_id=ref.id, q_id=ref_q_id)
                db.add(mapping)

            new_refs_count += 1

    await db.commit()

    return {
        "message": "Article sync complete.",
        "article_q_id": article.q_id,
        "new_references_added": new_refs_count,
        "existing_references_updated": updated_refs_count,
    }


@app.get("/scrape/{q_id}/")
async def scrape_article_endpoint(q_id: str, db: AsyncSession = Depends(get_db)):
    """Explicit GET trigger to scrape and sync an article by Q-ID."""
    return await sync_article_by_qid(q_id, db)


@app.get("/database/dump/")
async def dump_entire_database(db: AsyncSession = Depends(get_db)):
    stmt = select(Article).options(selectinload(Article.references))
    result = await db.execute(stmt)
    articles = result.scalars().all()

    database_dump = [
        {"q_id": article.q_id, "references_count": len(article.references)}
        for article in articles
    ]
    return {"total_articles": len(database_dump), "data": database_dump}


@app.get("/database/{q_id}/")
async def get_article_by_qid(q_id: str, db: AsyncSession = Depends(get_db)):
    """Returns a complete nested dump of a specific article. Strictly local DB only."""
    q_id = q_id.strip().upper()

    stmt = (
        select(Article)
        .options(selectinload(Article.references).selectinload(Reference.wikidata_mapping))
        .where(Article.q_id == q_id)
    )
    result = await db.execute(stmt)
    article = result.scalars().first()

    if not article:
        raise HTTPException(
            status_code=404,
            detail=f"Q-ID {q_id} not found in local database. Please run /scrape/{q_id}/ first.",
        )

    article_data = {
        "q_id": article.q_id,
        "name": article.name,
        "references": [
            {
                "id": ref.id,
                "language": ref.language,
                "source_url": ref.source_url,
                "raw_text": ref.raw_text,
                "context_text": ref.context_text,
                "ref_type": ref.ref_type,
                "ref_name": ref.ref_name,
                "doi": ref.doi,
                "pmid": ref.pmid,
                "arxiv": ref.arxiv,
                "q_id": ref.wikidata_mapping.q_id if ref.wikidata_mapping else None,
            }
            for ref in article.references
        ],
    }
    return article_data


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
    work_qids = [work["work_qid"] for work in works_data]

    local_refs = []
    if work_qids:
        stmt = (
            select(Reference)
            .join(WikidataMapping)
            .where(WikidataMapping.q_id.in_(work_qids))
            .options(selectinload(Reference.wikidata_mapping))
        )
        result = await db.execute(stmt)
        local_refs = result.scalars().all()

    combined_works = []
    for work in works_data:
        work_qid = work["work_qid"]
        matching_local_refs = [
            {
                "reference_id": ref.id,
                "raw_text": ref.raw_text,
                "context_text": ref.context_text,
                "language": ref.language,
                "source_url": ref.source_url,
            }
            for ref in local_refs
            if ref.wikidata_mapping.q_id == work_qid
        ]
        combined_works.append({
            "work_q_id": work_qid,
            "work_label": work["label"],
            "has_local_matches": len(matching_local_refs) > 0,
            "local_references": matching_local_refs,
        })

    return {
        "author_q_id": q_id,
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
            select(Reference)
            .join(WikidataMapping)
            .where(WikidataMapping.q_id.in_(citing_qids))
            .options(selectinload(Reference.wikidata_mapping))
        )
        result = await db.execute(stmt)
        local_refs = result.scalars().all()

    combined_citations = []
    for citation in citations_data:
        citing_qid = citation["citing_work_qid"]
        matching_local_refs = [
            {
                "reference_id": ref.id,
                "raw_text": ref.raw_text,
                "context_text": ref.context_text,
                "language": ref.language,
                "source_url": ref.source_url,
            }
            for ref in local_refs
            if ref.wikidata_mapping.q_id == citing_qid
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
    # Fetch all researchers by allowing the default 10000 limit
    researchers = await get_female_dtu_researchers()
    total_researchers = len(researchers)

    #print(f"\n--- Starting bulk scrape of {total_researchers} female DTU researchers ---")

    results = []
    for index, researcher in enumerate(researchers):
        q_id = researcher["q_id"]
        researcher_name = researcher["name"]

        # Calculate progress stats
        current_step = index + 1
        percent_complete = (current_step / total_researchers) * 100
        items_left = total_researchers - current_step

        stmt = select(Article).options(selectinload(Article.references)).where(Article.q_id == q_id)
        result = await db.execute(stmt)
        article = result.scalars().first()

        if article:
            #print(f"[{current_step}/{total_researchers}] {q_id} ({researcher_name}) already in DB. Skipping.")
            pass
        else:
            #print(f"[{current_step}/{total_researchers}] Scraping {q_id} ({researcher_name})...", end=" ", flush=True)
            try:
                # Scrapes Wikipedia and immediately commits to the database
                await sync_article_by_qid(q_id, db)

                result = await db.execute(stmt)
                article = result.scalars().first()
                #print("Done.")
            except Exception as e:
                logging.error(f"Failed! Error: {e}")

        #print(f" ---> Progress: {percent_complete:.1f}% complete. ({items_left} remaining)")

        ref_count = len(article.references) if article else 0

        results.append({
            "q_id": q_id,
            "name": article.name if article else researcher_name,
            "reference_count": ref_count
        })

    #print("--- Bulk scrape finished ---\n")

    results.sort(key=lambda x: x["reference_count"], reverse=True)

    return {
        "query": "Most referenced female researchers at DTU",
        "total_processed": total_researchers,
        "results": results
    }

@app.get("/refresh/")
async def refresh_all_articles(db: AsyncSession = Depends(get_db)):
    """
    Iterates through all articles currently in the database and re-scrapes them
    to update their references.
    """
    # Fetch all existing Q-IDs from the database
    stmt = select(Article.q_id)
    result = await db.execute(stmt)
    all_qids = result.scalars().all()

    total_articles = len(all_qids)
    results = []

    for index, q_id in enumerate(all_qids):
        try:
            # We reuse the exact same logic used by the individual scrape endpoint
            sync_result = await sync_article_by_qid(q_id, db)
            results.append({
                "q_id": q_id,
                "status": "success",
                "new_references": sync_result["new_references_added"],
                "updated_references": sync_result["existing_references_updated"]
            })
        except Exception as e:
            logging.error(f"Failed to refresh {q_id}: {e}")
            results.append({
                "q_id": q_id,
                "status": "failed",
                "error": str(e)
            })

    return {
        "message": "Database refresh complete.",
        "total_processed": total_articles,
        "details": results
    }