import asyncio
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
from qlever import resolve_doi_to_qid, resolve_qid_to_identifiers, get_author_works, get_citations_to_work, get_citations_for_author
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

    stmt = (select(Article)
            .options(selectinload(Article.references).selectinload(Reference.wikidata_mapping))
            .where(Article.q_id == q_id))

    result = await db.execute(stmt)
    article = result.scalars().first()

    existing_refs_map = {}

    if not article:
        article = Article(q_id=q_id)
        db.add(article)
        try:
            # Try to push the new article to the database
            await db.flush()
        except IntegrityError:
            # Absolute worst-case scenario: Another parallel request inserted Q938
            # in the exact millisecond between our commit and our flush.
            await db.rollback()

            # Fetch the article that the other request just created
            result = await db.execute(stmt)
            article = result.scalars().first()

            # Populate our references map so we update instead of inserting duplicates
            for ref in article.references:
                existing_refs_map[(ref.raw_text, ref.language)] = ref
    else:
        # Populate our references map normally
        for ref in article.references:
            existing_refs_map[(ref.raw_text, ref.language)] = ref

    new_refs_count = 0
    updated_refs_count = 0

    dois_to_resolve = list(set([r["doi"] for r in scraped_data["references"] if not r["q_id"] and r["doi"]]))
    qids_to_resolve = list(set([r["q_id"] for r in scraped_data["references"] if
                                r["q_id"] and not all([r.get("doi"), r.get("pmid"), r.get("arxiv")])]))

    resolved_dois = await resolve_doi_to_qid(dois_to_resolve) if dois_to_resolve else {}
    resolved_qids = await resolve_qid_to_identifiers(qids_to_resolve) if qids_to_resolve else {}

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
                doi=ref_data["doi"],
                pmid=ref_data["pmid"],
                arxiv=ref_data["arxiv"]
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
        "existing_references_updated": updated_refs_count
    }

@app.get("/scrape/{q_id}/")
async def scrape_article_endpoint(q_id: str, db: AsyncSession = Depends(get_db)):
    # Explicit GET trigger to scrape and sync an article by Q-ID.
    return await sync_article_by_qid(q_id, db)


@app.get("/database/dump/")
async def dump_entire_database(db: AsyncSession = Depends(get_db)):
    stmt = (
        select(Article)
        .options(
            selectinload(Article.references)
        )
    )

    result = await db.execute(stmt)
    articles = result.scalars().all()

    database_dump = []
    for article in articles:
        database_dump.append({
            "q_id": article.q_id,
            "references_count": len(article.references)
        })

    return {
        "total_articles": len(database_dump),
        "data": database_dump
    }


@app.get("/database/{q_id}/")
async def get_article_by_qid(q_id: str, db: AsyncSession = Depends(get_db)):
    # Returns a complete nested dump of a specific article by its Q-ID.
    # Will strictly ONLY check the local database.
    q_id = q_id.strip().upper()

    stmt = (
        select(Article)
        .options(
            selectinload(Article.references).selectinload(Reference.wikidata_mapping)
        )
        .where(Article.q_id == q_id)
    )

    result = await db.execute(stmt)
    article = result.scalars().first()

    if not article:
        # Fails fast if it doesn't exist locally
        raise HTTPException(
            status_code=404,
            detail=f"Q-ID {q_id} not found in local database. Please run /scrape/{q_id}/ first."
        )

    article_data = {
        "q_id": article.q_id,
        "references": []
    }

    for ref in article.references:
        ref_data = {
            "id": ref.id,
            "language": ref.language,
            "source_url": ref.source_url,
            "raw_text": ref.raw_text,
            "context_text": ref.context_text,
            "ref_type": ref.ref_type,
            "doi": ref.doi,
            "pmid": ref.pmid,
            "arxiv": ref.arxiv,
            "q_id": ref.wikidata_mapping.q_id if ref.wikidata_mapping else None
        }
        article_data["references"].append(ref_data)

    return article_data

@app.get("/stats/")
async def get_stats(db: AsyncSession = Depends(get_db)):
    result = await db.execute(
        select(func.count(Reference.id)).where(Reference.ref_type == 'cite q')
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

    # Combine the data based on Q-ID
    combined_works = []
    for work in works_data:
        work_qid = work["work_qid"]

        # Find any local references that match this specific work's Q-ID
        matching_local_refs = [
            {
                "reference_id": ref.id,
                "raw_text": ref.raw_text,
                "context_text": ref.context_text,
                "language": ref.language,
                "source_url": ref.source_url
            }
            for ref in local_refs if ref.wikidata_mapping.q_id == work_qid
        ]

        combined_works.append({
            "work_q_id": work_qid,
            "work_label": work["label"],
            "has_local_matches": len(matching_local_refs) > 0,
            "local_references": matching_local_refs
        })

    return {
        "author_q_id": q_id,
        "total_works": len(combined_works),
        "works": combined_works
    }


@app.get("/referenced/{q_id}/")
async def get_references_to_entity(q_id: str, db: AsyncSession = Depends(get_db)):
    q_id = q_id.strip().upper()

    # Try treating the Q-ID as an Author first
    entity_type = "author"
    citations_data = await get_citations_for_author(q_id)

    # Fallback: If empty, try treating the Q-ID as a piee of work
    if not citations_data:
        entity_type = "work"
        citations_data = await get_citations_to_work(q_id)

    citing_qids = list(set([citation["citing_work_qid"] for citation in citations_data]))

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

    # Combine the data based on Q-ID
    combined_citations = []
    for citation in citations_data:
        citing_qid = citation["citing_work_qid"]

        # Find any local references that match the citing work's Q-ID
        matching_local_refs = [
            {
                "reference_id": ref.id,
                "raw_text": ref.raw_text,
                "context_text": ref.context_text,
                "language": ref.language,
                "source_url": ref.source_url
            }
            for ref in local_refs if ref.wikidata_mapping.q_id == citing_qid
        ]

        combined_citations.append({
            "citing_work_q_id": citing_qid,
            "citing_work_label": citation["citing_work_label"],
            "original_work_q_id": citation["original_work_qid"],
            "original_work_label": citation["original_work_label"],
            "has_local_matches": len(matching_local_refs) > 0,
            "local_references": matching_local_refs
        })

    return {
        "entity_q_id": q_id,
        "resolved_as": entity_type,
        "total_citations": len(combined_citations),
        "citations": combined_citations
    }