import httpx
import logging
import asyncio

from config import settings


async def execute_sparql(query: str, max_retries: int = 5) -> dict:
    """Executor for SPARQL queries to QLever with exponential backoff."""
    headers = {
        "Accept": "application/sparql-results+json",
        "User-Agent": f"{settings.bot_name}/{settings.bot_version} (Contact: {settings.contact_email})"
    }
    #print("executing query:\n", query)
    async with httpx.AsyncClient() as client:
        retries = 0
        while True:
            try:
                response = await client.get(
                    settings.qlever_url,
                    params={"query": query},
                    headers=headers,
                    timeout=30.0
                )

                # Explicitly handle 429 Too Many Requests
                if response.status_code == 429:
                    wait = 2 ** retries
                    logging.warning(f"429 Too Many Requests from QLever. Backing off {wait}s")
                    await asyncio.sleep(wait)
                    retries += 1
                    if retries > max_retries:
                        response.raise_for_status()
                    continue

                response.raise_for_status()
                return response.json()

            except httpx.TimeoutException as e:
                wait = 2 ** retries
                logging.warning(f"QLever Timeout ({type(e)}). Backing off {wait}s")
                await asyncio.sleep(wait)
                retries += 1
                if retries > max_retries:
                    logging.error(f"QLever connection failed after {max_retries} retries: {repr(e)}")
                    raise e

            except httpx.HTTPStatusError as e:
                # Retry on 5xx Server Errors
                if e.response.status_code >= 500:
                    wait = 2 ** retries
                    logging.warning(f"QLever Server Error {e.response.status_code}. Backing off {wait}s")
                    await asyncio.sleep(wait)
                    retries += 1
                    if retries > max_retries:
                        logging.error(f"QLever HTTP {e.response.status_code} Error: {e.response.text}")
                        raise e
                else:
                    # Fail immediately on 400 Bad Request, etc.
                    logging.error(f"QLever HTTP {e.response.status_code} Error: {e.response.text}")
                    raise e

            except Exception as e:
                logging.error(f"QLever connection failed unexpectedly: {repr(e)}")
                raise e

async def resolve_references_batch(
        dois: list[str], qids: list[str]
) -> tuple[dict[str, str], dict[str, dict]]:
    if not dois and not qids:
        return {}, {}

    union_parts: list[str] = []

    if dois:
        values_str = " ".join(f'"{doi}"' for doi in dois)
        union_parts.append(f"""
        {{
          BIND("doi" AS ?queryType)
          VALUES ?inputDoi {{ {values_str} }}
          ?item wdt:P356 ?inputDoi .
        }}""")

    if qids:
        values_str = " ".join(f"wd:{qid}" for qid in qids)
        union_parts.append(f"""
        {{
          BIND("qid" AS ?queryType)
          VALUES ?item {{ {values_str} }}
          OPTIONAL {{ ?item wdt:P356 ?doi . }}
          OPTIONAL {{ ?item wdt:P698 ?pmid . }}
          OPTIONAL {{ ?item wdt:P818 ?arxiv . }}
        }}""")

    union_str = " UNION ".join(union_parts)

    query = f"""
    PREFIX wdt: <http://www.wikidata.org/prop/direct/>
    PREFIX wd: <http://www.wikidata.org/entity/>
    SELECT ?queryType ?inputDoi ?item ?doi ?pmid ?arxiv WHERE {{
      {union_str}
    }} LIMIT 10000
    """
    data = await execute_sparql(query)
    results = data.get("results", {}).get("bindings", [])

    doi_to_qid: dict[str, str] = {}
    qid_to_identifiers: dict[str, dict] = {}

    for row in results:
        query_type = row.get("queryType", {}).get("value")
        item_uri = row.get("item", {}).get("value", "")
        qid = item_uri.split("/")[-1] if item_uri else None

        if query_type == "doi" and qid:
            input_doi = row.get("inputDoi", {}).get("value")
            if input_doi:
                doi_to_qid[input_doi] = qid

        elif query_type == "qid" and qid:
            qid_to_identifiers[qid] = {
                "doi": row.get("doi", {}).get("value"),
                "pmid": row.get("pmid", {}).get("value"),
                "arxiv": row.get("arxiv", {}).get("value"),
            }

    return doi_to_qid, qid_to_identifiers


async def get_entity_context(q_id: str) -> dict:
    if not q_id:
        return {"labels": [], "works": [], "doi": None, "pmid": None, "arxiv": None}

    query = f"""
    PREFIX wdt: <http://www.wikidata.org/prop/direct/>
    PREFIX wd: <http://www.wikidata.org/entity/>
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
    SELECT ?resultType ?lang ?label ?work ?workLabel ?doi ?pmid ?arxiv WHERE {{
      {{
        BIND("entity" AS ?resultType)
        wd:{q_id} rdfs:label ?label .
        BIND(LANG(?label) AS ?lang)
        OPTIONAL {{ wd:{q_id} wdt:P356 ?doi . }}
        OPTIONAL {{ wd:{q_id} wdt:P698 ?pmid . }}
        OPTIONAL {{ wd:{q_id} wdt:P818 ?arxiv . }}
      }} UNION {{
        BIND("work" AS ?resultType)
        ?work wdt:P50 wd:{q_id} .
        OPTIONAL {{ ?work rdfs:label ?workLabel . FILTER(LANG(?workLabel) = "en") }}
        OPTIONAL {{ ?work wdt:P356 ?doi . }}
        OPTIONAL {{ ?work wdt:P698 ?pmid . }}
        OPTIONAL {{ ?work wdt:P818 ?arxiv . }}
      }}
    }} LIMIT 10000
    """
    data = await execute_sparql(query)
    results = data.get("results", {}).get("bindings", [])

    labels: list[dict] = []
    works: list[dict] = []
    seen_work_qids: set[str] = set()

    entity_doi = None
    entity_pmid = None
    entity_arxiv = None

    for row in results:
        result_type = row.get("resultType", {}).get("value")

        if result_type == "entity":
            lang_code = row.get("lang", {}).get("value")
            label_val = row.get("label", {}).get("value")
            if lang_code and label_val:
                labels.append({"lang": lang_code, "title": label_val})

            # Grab identifiers for the primary entity itself
            if row.get("doi") and not entity_doi:
                entity_doi = row.get("doi").get("value")
            if row.get("pmid") and not entity_pmid:
                entity_pmid = row.get("pmid").get("value")
            if row.get("arxiv") and not entity_arxiv:
                entity_arxiv = row.get("arxiv").get("value")

        elif result_type == "work":
            work_uri = row.get("work", {}).get("value", "")
            work_qid = work_uri.split("/")[-1] if work_uri else None

            if work_qid and work_qid not in seen_work_qids:
                seen_work_qids.add(work_qid)
                works.append({
                    "work_qid": work_qid,
                    "label": row.get("workLabel", {}).get("value", "Unknown Label"),
                    "doi": row.get("doi", {}).get("value"),
                    "pmid": row.get("pmid", {}).get("value"),
                    "arxiv": row.get("arxiv", {}).get("value"),
                })

    return {
        "labels": labels,
        "works": works,
        "doi": entity_doi,
        "pmid": entity_pmid,
        "arxiv": entity_arxiv
    }

async def get_author_works(author_qid: str) -> list[dict]:
    if not author_qid:
        return []

    query = f"""
    PREFIX wdt: <http://www.wikidata.org/prop/direct/>
    PREFIX wd: <http://www.wikidata.org/entity/>
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
    SELECT ?work ?workLabel WHERE {{
      ?work wdt:P50 wd:{author_qid.upper()} .
      OPTIONAL {{ ?work rdfs:label ?workLabel . FILTER(LANG(?workLabel) = "en") }}
    }} LIMIT 10000
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
                "label": row.get("workLabel", {}).get("value", "Unknown Label"),
            })
    return works


async def get_work_authors(work_qid: str) -> list[dict]:
    """Fetches the authors for a specific work (article)."""
    if not work_qid:
        return []

    query = f"""
    PREFIX wdt: <http://www.wikidata.org/prop/direct/>
    PREFIX wd: <http://www.wikidata.org/entity/>
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
    SELECT ?author ?authorLabel WHERE {{
      wd:{work_qid.upper()} wdt:P50 ?author .
      OPTIONAL {{ ?author rdfs:label ?authorLabel . FILTER(LANG(?authorLabel) = "en") }}
    }} LIMIT 10000
    """
    data = await execute_sparql(query)
    results = data.get("results", {}).get("bindings", [])

    authors = []
    for row in results:
        author_uri = row.get("author", {}).get("value", "")
        author_qid = author_uri.split("/")[-1] if author_uri else None
        if author_qid:
            authors.append({
                "author_qid": author_qid,
                "label": row.get("authorLabel", {}).get("value", "Unknown Label"),
            })
    return authors


async def get_entity_classification(q_id: str) -> str:
    """Determines what kind of entity a Q-ID represents (e.g., person, article, book)."""
    if not q_id:
        return "unknown"

    query = f"""
    PREFIX wdt: <http://www.wikidata.org/prop/direct/>
    PREFIX wd: <http://www.wikidata.org/entity/>
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
    SELECT ?type ?typeLabel WHERE {{
      wd:{q_id.upper()} wdt:P31 ?type .
      OPTIONAL {{ ?type rdfs:label ?typeLabel . FILTER(LANG(?typeLabel) = "en") }}
    }} LIMIT 1
    """
    try:
        data = await execute_sparql(query)
        results = data.get("results", {}).get("bindings", [])
        if results:
            row = results[0]
            type_uri = row.get("type", {}).get("value", "")
            type_qid = type_uri.split("/")[-1] if type_uri else ""
            type_label = row.get("typeLabel", {}).get("value", "")

            # Provide clean names for common types
            if type_qid == "Q5":
                return "person"
            if type_qid == "Q13442814":
                return "scholarly article"
            if type_qid == "Q571":
                return "book"
            if type_qid == "Q732577":
                return "publication"

            # Fallback to the Wikidata label if available
            if type_label:
                return type_label
            return type_qid
    except Exception as e:
        logging.error(f"Failed to get entity classification for {q_id}: {e}")

    return "unknown"

async def get_citations_for_author(author_qid: str) -> list[dict]:
    if not author_qid:
        return []

    query = f"""
    PREFIX wdt: <http://www.wikidata.org/prop/direct/>
    PREFIX wd: <http://www.wikidata.org/entity/>
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
    SELECT ?citingWork ?citingWorkLabel ?work ?workLabel WHERE {{
      ?work wdt:P50 wd:{author_qid} .
      ?citingWork wdt:P2860 ?work .
      OPTIONAL {{ ?citingWork rdfs:label ?citingWorkLabel . FILTER(LANG(?citingWorkLabel) = "en") }}
      OPTIONAL {{ ?work rdfs:label ?workLabel . FILTER(LANG(?workLabel) = "en") }}
    }} LIMIT 10000
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
                "original_work_label": row.get("workLabel", {}).get("value", "Unknown Label"),
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
    }} LIMIT 10000
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
                "original_work_label": row.get("workLabel", {}).get("value", "Unknown Label"),
            })
    return citations


async def get_female_dtu_researchers(limit: int = 10000) -> list[dict]:
    """
    Queries Wikidata for female (Q6581072) individuals whose employer (P108)
    is currently the Technical University of Denmark (Q211115).
    Filters out past employees by checking the 'end time' qualifier (P582).
    """
    query = f"""
    PREFIX p: <http://www.wikidata.org/prop/>
    PREFIX ps: <http://www.wikidata.org/prop/statement/>
    PREFIX pq: <http://www.wikidata.org/prop/qualifier/>
    PREFIX wdt: <http://www.wikidata.org/prop/direct/>
    PREFIX wd: <http://www.wikidata.org/entity/>
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

    SELECT DISTINCT ?person ?personLabel WHERE {{
      # Use p: to get the specific employment statement node
      ?person p:P108 ?employmentStatement .

      # Use ps: to verify the statement is specifically for DTU
      ?employmentStatement ps:P108 wd:Q1269766 .

      # Must be female
      ?person wdt:P21 wd:Q6581072 .

      # Attempt to get the end time qualifier from the employment statement
      OPTIONAL {{ ?employmentStatement pq:P582 ?endTime . }}

      # Keep if there is no end time OR if the end time is in the future
      FILTER (!BOUND(?endTime) || ?endTime >= NOW())

      OPTIONAL {{ ?person rdfs:label ?personLabel . FILTER(LANG(?personLabel) = "en") }}
    }} LIMIT {limit}
    """
    data = await execute_sparql(query)
    results = data.get("results", {}).get("bindings", [])

    researchers = []
    for row in results:
        person_uri = row.get("person", {}).get("value", "")
        q_id = person_uri.split("/")[-1] if person_uri else None
        if q_id:
            researchers.append({
                "q_id": q_id,
                "name": row.get("personLabel", {}).get("value", "Unknown Name")
            })
    return researchers