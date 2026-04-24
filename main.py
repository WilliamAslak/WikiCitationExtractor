from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException, Depends
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker, selectinload
from sqlalchemy import select, func
import httpx

from models import Base, Article, Reference, WikidataMapping
from scraper import scrape_wikipedia_references
from qlever import resolve_doi_to_qid, resolve_qid_to_identifiers
from config import settings

# Database configuration
engine = create_async_engine(settings.database_url, echo=False)
AsyncSessionLocal = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)


@asynccontextmanager
async def lifespan(app: FastAPI):
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    yield


app = FastAPI(title="Wiki Citation Extractor", lifespan=lifespan)


# Dependency
async def get_db():
    async with AsyncSessionLocal() as session:
        yield session


class ScrapeRequest(BaseModel):
    url: str


@app.get("/ok/")
async def health_check():
    return {"status": "ok"}


@app.post("/scrape/")
async def scrape_article(request: ScrapeRequest, db: AsyncSession = Depends(get_db)):
    try:
        scraped_data = await scrape_wikipedia_references(request.url)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Scraping failed: {e}")

    # Check if article exists and load its existing references and mappings
    stmt = (
        select(Article)
        .options(selectinload(Article.references).selectinload(Reference.wikidata_mapping))
        .where(Article.url == request.url)
    )
    result = await db.execute(stmt)
    article = result.scalars().first()

    existing_refs_map = {}

    if not article:
        # Create a new article if it doesn't exist
        article = Article(
            url=scraped_data["url"],
            title=scraped_data["title"],
            language=scraped_data["language"],
            q_id=scraped_data["q_id"]
        )
        db.add(article)
        await db.flush()
    else:
        # Update article data if it changed
        article.title = scraped_data["title"]
        article.language = scraped_data["language"]
        article.q_id = scraped_data["q_id"]
        # Build a dictionary of existing references using the raw_text as the key
        for ref in article.references:
            existing_refs_map[ref.raw_text] = ref

    new_refs_count = 0
    updated_refs_count = 0

    # Process scraped references
    for ref_data in scraped_data["references"]:
        raw_text = ref_data["raw_text"]
        q_id = ref_data["q_id"]

        # Fetch missing identifiers from Wikidata if we have a Q-ID
        if q_id and not all([ref_data["doi"], ref_data["pmid"], ref_data["arxiv"]]):
            identifiers = await resolve_qid_to_identifiers(q_id)
            ref_data["doi"] = ref_data["doi"] or identifiers.get("doi")
            ref_data["pmid"] = ref_data["pmid"] or identifiers.get("pmid")
            ref_data["arxiv"] = ref_data["arxiv"] or identifiers.get("arxiv")

        if raw_text in existing_refs_map:
            ref = existing_refs_map[raw_text]

            ref.ref_type = ref_data["ref_type"]
            ref.doi = ref_data["doi"]
            ref.pmid = ref_data["pmid"]
            ref.arxiv = ref_data["arxiv"]

            # Check if we need to resolve a new Q-ID for an existing reference
            if not q_id and ref_data["doi"] and not ref.wikidata_mapping:
                q_id = await resolve_doi_to_qid(ref_data["doi"])

            # Sync the Wikidata mapping
            if q_id:
                if ref.wikidata_mapping:
                    ref.wikidata_mapping.q_id = q_id
                else:
                    mapping = WikidataMapping(reference_id=ref.id, q_id=q_id)
                    db.add(mapping)

            updated_refs_count += 1

        # If the reference is new
        else:
            ref = Reference(
                article_id=article.id,
                raw_text=raw_text,
                ref_type=ref_data["ref_type"],
                doi=ref_data["doi"],
                pmid=ref_data["pmid"],
                arxiv=ref_data["arxiv"]
            )
            db.add(ref)
            await db.flush()

            if not q_id and ref_data["doi"]:
                q_id = await resolve_doi_to_qid(ref_data["doi"])

            if q_id:
                mapping = WikidataMapping(reference_id=ref.id, q_id=q_id)
                db.add(mapping)

            new_refs_count += 1

    await db.commit()

    return {
        "message": "Article sync complete.",
        "article_id": article.id,
        "article_q_id": article.q_id,
        "new_references_added": new_refs_count,
        "existing_references_updated": updated_refs_count
    }


@app.get("/database/dump/")
async def dump_entire_database(db: AsyncSession = Depends(get_db)):
    """
    Returns a complete nested dump of all articles, their references,
    and associated Wikidata mappings.
    """
    stmt = (
        select(Article)
        .options(
            selectinload(Article.references).selectinload(Reference.wikidata_mapping)
        )
    )

    result = await db.execute(stmt)
    articles = result.scalars().all()

    database_dump = []
    for article in articles:
        article_data = {
            "id": article.id,
            "q_id": article.q_id,
            "url": article.url,
            "title": article.title,
            "language": article.language,
            "references": len(article.references)
        }
        """
        for ref in article.references:
            ref_data = {
                "id": ref.id,
                "raw_text": ref.raw_text,
                "ref_type": ref.ref_type,
                "doi": ref.doi,
                "pmid": ref.pmid,
                "arxiv": ref.arxiv,
                "q_id": ref.wikidata_mapping.q_id if ref.wikidata_mapping else None
            }
            article_data["references"].append(ref_data)
        """
        database_dump.append(article_data)

    return {
        "total_articles": len(database_dump),
        "data": database_dump
    }


@app.get("/database/{article_id}/")
async def get_article_by_id(article_id: int, db: AsyncSession = Depends(get_db)):
    """
    Returns a complete nested dump of a specific article by its ID,
    including its references and associated Wikidata mappings.
    """
    stmt = (
        select(Article)
        .options(
            selectinload(Article.references).selectinload(Reference.wikidata_mapping)
        )
        .where(Article.id == article_id)
    )

    result = await db.execute(stmt)
    article = result.scalars().first()

    if not article:
        raise HTTPException(status_code=404, detail=f"Article with ID {article_id} not found")

    # Serialize the nested SQLAlchemy objects into a dictionary
    article_data = {
        "id": article.id,
        "q_id": article.q_id,
        "url": article.url,
        "title": article.title,
        "language": article.language,
        "references": []
    }

    for ref in article.references:
        ref_data = {
            "id": ref.id,
            "raw_text": ref.raw_text,
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
    # Count how many references use the `Cite Q` template
    result = await db.execute(
        select(func.count(Reference.id)).where(Reference.ref_type == 'cite q')
    )
    cite_q_count = result.scalar()
    return {"cite_q_usage_count": cite_q_count}

