import httpx
import logging
from config import settings


async def execute_sparql(query: str) -> dict:
    #Executor for SPARQL queries to QLever.
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

            return response.json()
        except Exception as e:
            print("---------ERROR---------")
            logging.error(f"QLever query failed: {e}")
            print("-----------------------")
            return {}


async def resolve_doi_to_qid(doi_input: str | list[str]) -> dict[str, str]:
    # Accepts a single DOI or a list of DOIs, returns a dict mapping DOI to Q-ID.
    if not doi_input:
        return {}

    # Normalize input to a list
    dois = [doi_input] if isinstance(doi_input, str) else doi_input
    values_str = " ".join([f'"{doi}"' for doi in dois])

    query = f"""
    PREFIX wdt: <http://www.wikidata.org/prop/direct/>
    SELECT ?doi ?item WHERE {{
      VALUES ?doi {{ {values_str} }}
      ?item wdt:P356 ?doi .
    }}
    """
    data = await execute_sparql(query)
    results = data.get("results", {}).get("bindings", [])

    mapping = {}
    for row in results:
        doi_val = row.get("doi", {}).get("value")
        item_uri = row.get("item", {}).get("value", "")
        if doi_val and item_uri:
            mapping[doi_val] = item_uri.split("/")[-1]

    return mapping


async def resolve_qid_to_identifiers(qid_input: str | list[str]) -> dict[str, dict]:
    # Accepts a single Q-ID or a list of Q-IDs, returns a dict mapping Q-ID to identifiers.
    if not qid_input:
        return {}

    # Normalize input to a list
    qids = [qid_input] if isinstance(qid_input, str) else qid_input
    values_str = " ".join([f"wd:{qid}" for qid in qids])

    query = f"""
    PREFIX wdt: <http://www.wikidata.org/prop/direct/>
    PREFIX wd: <http://www.wikidata.org/entity/>
    SELECT ?item ?doi ?pmid ?arxiv WHERE {{
      VALUES ?item {{ {values_str} }}
      OPTIONAL {{ ?item wdt:P356 ?doi . }}
      OPTIONAL {{ ?item wdt:P698 ?pmid . }}
      OPTIONAL {{ ?item wdt:P818 ?arxiv . }}
    }}
    """
    data = await execute_sparql(query)
    results = data.get("results", {}).get("bindings", [])

    mapping = {}
    for row in results:
        item_uri = row.get("item", {}).get("value", "")
        qid = item_uri.split("/")[-1] if item_uri else None
        if qid:
            mapping[qid] = {
                "doi": row.get("doi", {}).get("value"),
                "pmid": row.get("pmid", {}).get("value"),
                "arxiv": row.get("arxiv", {}).get("value")
            }

    return mapping


async def get_author_works(author_qid: str) -> list[dict]:
    # Find works authored by a specific QID using P50 (author).
    if not author_qid:
        return []
    query = f"""
    PREFIX wdt: <http://www.wikidata.org/prop/direct/>
    PREFIX wd: <http://www.wikidata.org/entity/>
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
    SELECT ?work ?workLabel WHERE {{
      ?work wdt:P50 wd:{author_qid.capitalize()} .
      OPTIONAL {{ ?work rdfs:label ?workLabel . FILTER(LANG(?workLabel) = "en") }}
    }}
    """
    data = await execute_sparql(query)
    results = data.get("results", {}).get("bindings", [])

    works = []
    for row in results:
        work_uri = row.get("work", {}).get("value", "")
        work_qid = work_uri.split("/")[-1] if work_uri else None
        if work_qid:
            works.append({
                "work_qid": work_qid,
                "label": row.get("workLabel", {}).get("value", "Unknown Label")
            })
    return works


async def get_publication_citations(pub_qid: str) -> list[dict]:
    # Find all items that this publication cites using P2860 (cites work).
    if not pub_qid:
        return []
    query = f"""
    PREFIX wdt: <http://www.wikidata.org/prop/direct/>
    PREFIX wd: <http://www.wikidata.org/entity/>
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
    SELECT ?citedWork ?citedWorkLabel WHERE {{
      wd:{pub_qid} wdt:P2860 ?citedWork .
      OPTIONAL {{ ?citedWork rdfs:label ?citedWorkLabel . FILTER(LANG(?citedWorkLabel) = "en") }}
    }}
    """
    data = await execute_sparql(query)
    results = data.get("results", {}).get("bindings", [])

    citations = []
    for row in results:
        cited_uri = row.get("citedWork", {}).get("value", "")
        cited_qid = cited_uri.split("/")[-1] if cited_uri else None
        if cited_qid:
            citations.append({
                "cited_qid": cited_qid,
                "label": row.get("citedWorkLabel", {}).get("value", "Unknown Label")
            })
    return citations


async def get_citations_for_author(author_qid: str) -> list[dict]:
    # Find works that cite the publications of a specific author.
    if not author_qid:
        return []

    # Using the exact logic from your provided SPARQL query, enhanced with labels
    query = f"""
    PREFIX wdt: <http://www.wikidata.org/prop/direct/>
    PREFIX wd: <http://www.wikidata.org/entity/>
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
    SELECT ?citingWork ?citingWorkLabel ?work ?workLabel WHERE {{
      ?work wdt:P50 wd:{author_qid} .
      ?citingWork wdt:P2860 ?work .
      OPTIONAL {{ ?citingWork rdfs:label ?citingWorkLabel . FILTER(LANG(?citingWorkLabel) = "en") }}
      OPTIONAL {{ ?work rdfs:label ?workLabel . FILTER(LANG(?workLabel) = "en") }}
    }}
    """
    data = await execute_sparql(query)
    results = data.get("results", {}).get("bindings", [])

    citations = []
    for row in results:
        citing_uri = row.get("citingWork", {}).get("value", "")
        citing_qid = citing_uri.split("/")[-1] if citing_uri else None

        work_uri = row.get("work", {}).get("value", "")
        work_qid = work_uri.split("/")[-1] if work_uri else None

        if citing_qid:
            citations.append({
                "citing_work_qid": citing_qid,
                "citing_work_label": row.get("citingWorkLabel", {}).get("value", "Unknown Label"),
                "original_work_qid": work_qid,
                "original_work_label": row.get("workLabel", {}).get("value", "Unknown Label")
            })

    return citations


async def get_citations_to_work(work_qid: str) -> list[dict]:
    if not work_qid:
        return []

    query = f"""
    PREFIX wdt: <http://www.wikidata.org/prop/direct/>
    PREFIX wd: <http://www.wikidata.org/entity/>
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
    SELECT ?citingWork ?citingWorkLabel ?workLabel WHERE {{
      ?citingWork wdt:P2860 wd:{work_qid} .
      OPTIONAL {{ ?citingWork rdfs:label ?citingWorkLabel . FILTER(LANG(?citingWorkLabel) = "en") }}
      OPTIONAL {{ wd:{work_qid} rdfs:label ?workLabel . FILTER(LANG(?workLabel) = "en") }}
    }}
    """
    data = await execute_sparql(query)
    results = data.get("results", {}).get("bindings", [])

    citations = []
    for row in results:
        citing_uri = row.get("citingWork", {}).get("value", "")
        citing_qid = citing_uri.split("/")[-1] if citing_uri else None

        if citing_qid:
            citations.append({
                "citing_work_qid": citing_qid,
                "citing_work_label": row.get("citingWorkLabel", {}).get("value", "Unknown Label"),
                "original_work_qid": work_qid,
                "original_work_label": row.get("workLabel", {}).get("value", "Unknown Label")
            })

    return citations


async def get_wikidata_labels(q_id: str) -> list[dict]:
    """Fetches all localized name labels for a given Q-ID."""
    if not q_id:
        return []

    query = f"""
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
    PREFIX wd: <http://www.wikidata.org/entity/>

    SELECT ?lang ?label WHERE {{
      wd:{q_id} rdfs:label ?label .
      BIND(LANG(?label) AS ?lang)
    }}
    """
    data = await execute_sparql(query)

    results = []
    for item in data.get("results", {}).get("bindings", []):
        lang_code = item.get("lang", {}).get("value")
        label = item.get("label", {}).get("value")
        if lang_code and label:
            results.append({"lang": lang_code, "title": label})

    return results