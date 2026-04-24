import httpx
import logging
from config import settings

async def resolve_doi_to_qid(doi: str) -> str | None:
    if not doi:
        return None

    # wdt:P356 is the property for DOI
    query = f"""
    PREFIX wdt: <http://www.wikidata.org/prop/direct/>
    SELECT ?item WHERE {{
      ?item wdt:P356 "{doi}" .
    }}
    LIMIT 1
    """

    headers = {
        "Accept": "application/sparql-results+json",
        "User-Agent": f"{settings.bot_name}/{settings.bot_version} (Contact: {settings.contact_email})"
    }

    async with httpx.AsyncClient() as client:
        try:
            response = await client.post(
                settings.qlever_url,
                data={"query": query},
                headers=headers,
                timeout=10.0
            )
            response.raise_for_status()
            data = response.json()

            results = data.get("results", {}).get("bindings", [])
            if results:
                item_uri = results[0]["item"]["value"]
                # Extract Q-ID from the URI (e.g., http://www.wikidata.org/entity/Q12345)
                return item_uri.split("/")[-1]

        except Exception as e:
            logging.error(f"QLever query failed for DOI {doi}: {e}")

    return None

async def resolve_qid_to_identifiers(q_id: str) -> dict:
    #Fetches DOI, PMID, and arXiv for a given Wikidata Q-ID.
    if not q_id:
        return {}

    query = f"""
    PREFIX wdt: <http://www.wikidata.org/prop/direct/>
    PREFIX wd: <http://www.wikidata.org/entity/>
    SELECT ?doi ?pmid ?arxiv WHERE {{
      BIND(wd:{q_id} AS ?item)
      OPTIONAL {{ ?item wdt:P356 ?doi . }}
      OPTIONAL {{ ?item wdt:P698 ?pmid . }}
      OPTIONAL {{ ?item wdt:P818 ?arxiv . }}
    }}
    LIMIT 1
    """

    headers = {
        "Accept": "application/sparql-results+json",
        "User-Agent": f"{settings.bot_name}/{settings.bot_version} (Contact: {settings.contact_email})"
    }

    async with httpx.AsyncClient() as client:
        try:
            response = await client.post(
                settings.qlever_url,
                data={"query": query},
                headers=headers,
                timeout=10.0
            )
            response.raise_for_status()
            data = response.json()

            results = data.get("results", {}).get("bindings", [])
            if results:
                binding = results[0]
                return {
                    "doi": binding.get("doi", {}).get("value"),
                    "pmid": binding.get("pmid", {}).get("value"),
                    "arxiv": binding.get("arxiv", {}).get("value")
                }
        except Exception as e:
            logging.error(f"QLever query failed for Q-ID {q_id}: {e}")

    return {}