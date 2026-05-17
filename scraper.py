import asyncio
import logging
import httpx
import mwparserfromhell
import urllib
from urllib.parse import urlparse
import re

from config import settings

_SEARCH_SEMAPHORE = asyncio.Semaphore(3)
_FETCH_SEMAPHORE = asyncio.Semaphore(10)


# ---------------------------------------------------------------------------
# Utility helpers
# ---------------------------------------------------------------------------
def extract_context(wikitext: str, tag_str: str, occurrence: int = 0, window: int = 200) -> str:
    # Find the specific nth occurrence of the tag string
    pos = -1
    for _ in range(occurrence + 1):
        pos = wikitext.find(tag_str, pos + 1)
        if pos == -1:
            return ""

    # Grab a larger initial chunk to account for very long preceding references
    search_window = window + 2000
    start = max(0, pos - search_window)
    left_text = wikitext[start:pos]

    # Stop at the most recent linebreak
    last_newline = left_text.rfind("\n")
    if last_newline != -1:
        left_text = left_text[last_newline + 1:]

    # Remove all complete <ref>...</ref> tags
    left_text = re.sub(r'<ref[^>]*>.*?</ref>', '', left_text, flags=re.DOTALL | re.IGNORECASE)

    # Remove all self-closing <ref ... /> tags
    left_text = re.sub(r'<ref[^>]*/>', '', left_text, flags=re.IGNORECASE)

    # Now take the final `window` characters of the cleaned text
    if len(left_text) > window:
        left_text = left_text[-window:]

        # Find the first space to remove any cut-off word at the beginning
        first_space = left_text.find(" ")
        if first_space != -1:
            left_text = left_text[first_space + 1:]

    return left_text.strip()


def normalize_lang_code(lang: str) -> str | None:
    lang = lang.lower()
    if lang == "mul":
        return None
    if "-" in lang:
        return lang.split("-")[0]
    return lang


# ---------------------------------------------------------------------------
# Wikidata / Wikipedia API calls
# ---------------------------------------------------------------------------

async def fetch_with_retry(method: str, url: str, semaphore: asyncio.Semaphore, max_retries: int = 4, params: dict | None = None,
        data: dict | None = None, timeout: float = 15.0) -> httpx.Response:
    """Generic HTTP request executor with explicit parameters and backoff."""

    headers = {"User-Agent": f"{settings.bot_name}/1.0 (Contact: {settings.contact_email})"}

    async with semaphore:
        async with httpx.AsyncClient(headers=headers, follow_redirects=True) as client:
            retries = 0
            while True:
                try:
                    response = await client.request(
                        method=method,
                        url=url,
                        params=params,
                        data=data,
                        timeout=timeout
                    )

                    if response.status_code == 429:
                        wait = 2 ** retries
                        logging.warning(f"429 Too Many Requests from {url}. Backing off {wait}s")
                        await asyncio.sleep(wait)
                        retries += 1
                        if retries > max_retries:
                            response.raise_for_status()
                        continue

                    response.raise_for_status()
                    return response

                except httpx.TimeoutException as e:
                    wait = 2 ** retries
                    logging.warning(f"Timeout ({type(e)}) on {url}. Backing off {wait}s")
                    await asyncio.sleep(wait)
                    retries += 1
                    if retries > max_retries:
                        logging.error(f"Connection failed after {max_retries} retries for {url}")
                        raise e

                except httpx.HTTPStatusError as e:
                    if e.response.status_code >= 500:
                        wait = 2 ** retries
                        logging.warning(f"Server Error {e.response.status_code} on {url}. Backing off {wait}s")
                        await asyncio.sleep(wait)
                        retries += 1
                        if retries > max_retries:
                            raise e
                    else:
                        raise e
                except Exception as e:
                    logging.error(f"Connection failed unexpectedly for {url}: {repr(e)}")
                    raise e


async def get_sitelinks_from_qid(q_id: str) -> dict[str, str]:
    """Used only to know which Wikipedia language editions are highly relevant to search."""
    q_id = q_id.strip().upper()
    url = "https://www.wikidata.org/w/api.php"
    params = {
        "action": "wbgetentities",
        "ids": q_id,
        "props": "sitelinks/urls",
        "format": "json",
    }

    try:
        data = (await fetch_with_retry("GET", url, _FETCH_SEMAPHORE, params=params, timeout=30.0)).json()
    except Exception as e:
        logging.error(f"Failed to fetch sitelinks for {q_id}: {e}")
        return {}

    entities = data.get("entities", {})
    entity = entities.get(q_id, {})
    sitelinks = entity.get("sitelinks", {})

    urls = {}
    for site, link_data in sitelinks.items():
        site_url = link_data.get("url", "")
        if "wikipedia.org" in site_url:
            parsed_url = urlparse(site_url)
            lang = parsed_url.netloc.split(".")[0]
            urls[lang] = site_url
    return urls


async def find_mentions_via_search_query(lang: str, query_str: str, max_total: int = 50) -> list[str]:
    endpoint = f"https://{lang}.wikipedia.org/w/api.php"
    all_titles: list[str] = []

    data = {
        "action": "query",
        "list": "search",
        "srsearch": query_str,
        "srnamespace": "0",
        "srlimit": "50",
        "format": "json",
    }

    while len(all_titles) < max_total:
        try:
            resp_json = (await fetch_with_retry("POST", endpoint, _SEARCH_SEMAPHORE, data=data, timeout=15.0)).json()
        except Exception as e:
            logging.warning(f"Could not reach {lang}.wikipedia.org for mentions: {e}")
            return all_titles

        batch = [item["title"] for item in resp_json.get("query", {}).get("search", [])]
        all_titles.extend(batch)

        if "continue" not in resp_json:
            break
        data.update(resp_json["continue"])

    return all_titles[:max_total]


# ---------------------------------------------------------------------------
# Per-page scraping
# ---------------------------------------------------------------------------

async def _fetch_and_parse_multiple_mentions(lang: str, candidate_title: str, targets: list[dict]) -> list[dict]:
    domain = f"{lang}.wikipedia.org"

    # Safely format the title for the Wikipedia API by replacing spaces with underscores
    encoded_title = urllib.parse.quote(candidate_title.replace(" ", "_"))

    raw_url = f"https://{domain}/w/index.php?title={encoded_title}&action=raw"

    try:
        wikitext = (await fetch_with_retry("GET", raw_url, _FETCH_SEMAPHORE, timeout=30.0)).text
    except Exception as e:
        logging.warning(f"Failed to fetch candidate '{candidate_title}' ({lang}): {repr(e)}")
        return []

    extracted: list[dict] = []
    wikicode = mwparserfromhell.parse(wikitext)
    tags = wikicode.filter_tags(matches=lambda node: node.tag == "ref")


    # Handles duplicate <ref> tags in the same article for example:
    tag_seen_counts = {}
    for tag in tags:
        tag_raw_str = str(tag)
        occurrence_index = tag_seen_counts.get(tag_raw_str, 0)
        tag_seen_counts[tag_raw_str] = occurrence_index + 1

        tag_text = str(tag.contents).strip() if tag.contents else ""
        if not tag_text:
            continue

        templates = tag.contents.filter_templates() if tag.contents else []
        ref_type = "text_mention"
        ref_q_id, ref_doi, ref_pmid, ref_arxiv = None, None, None, None

        for tpl in templates:
            tpl_name = str(tpl.name).strip().lower()[:50]
            if "cite" in tpl_name or "citation" in tpl_name or "literatur" in tpl_name:
                ref_type = tpl_name

            for param in tpl.params:
                p_name = str(param.name).strip().lower()

                if p_name == "doi":
                    ref_doi = str(param.value).strip()
                elif p_name == "pmid":
                    ref_pmid = str(param.value).strip()
                elif p_name == "arxiv":
                    ref_arxiv = str(param.value).strip()
                elif p_name == "q" or (tpl_name == "cite q" and p_name == "1"):
                    ref_q_id = str(param.value).strip().upper()

        # Normalizing the parsed reference strings once outside the loop to save processing power
        r_qid = ref_q_id.strip().upper() if ref_q_id else None
        r_doi = ref_doi.strip().lower() if ref_doi else None
        r_pmid = ref_pmid.strip() if ref_pmid else None
        r_arxiv = ref_arxiv.strip().lower() if ref_arxiv else None
        #print(r_qid, r_doi, r_pmid, r_arxiv)

        matched_targets = {}
        for t in targets:
            # Unpack the target variables
            t_qid = t["q_id"].strip().upper()
            t_doi = t.get("doi", "").strip().lower() if t.get("doi") else None
            t_pmid = t.get("pmid", "").strip() if t.get("pmid") else None
            t_arxiv = t.get("arxiv", "").strip().lower() if t.get("arxiv") else None

            # Evaluate all match conditions in a single boolean check instead of our previous if else ladder.
            if ((r_qid and r_qid == t_qid) or (r_doi and r_doi == t_doi) or
                (r_pmid and r_pmid == t_pmid) or (r_arxiv and r_arxiv == t_arxiv) or
                (t_qid in tag_text.upper()) or (t_doi and t_doi in tag_text.lower())):
                matched_targets[t["q_id"]] = t

        if not matched_targets:
            continue

        context_txt = extract_context(wikitext, tag_raw_str, occurrence=occurrence_index)

        for target in matched_targets.values():
            ref_data = {
                "raw_text": tag_text,
                "context_text": context_txt,
                "ref_type": ref_type,
                "ref_name": target.get("label", ""),
                "doi": ref_doi,
                "pmid": ref_pmid,
                "arxiv": ref_arxiv,
                "q_id": target["q_id"],
                "language": lang,
                "source_url": f"https://{domain}/wiki/{candidate_title.replace(' ', '_')}",
            }
            extracted.append(ref_data)

    return extracted

async def _scrape_mentions_batched(lang: str, targets: list[dict]) -> list[dict]:
    candidate_titles = set()

    # 1. Flatten all identifiers into a single list of search terms
    # We force everything that is case-insensitive (DOIs, arXivs) into lowercase here.
    search_terms = []
    for t in targets:
        if t.get("q_id"):
            search_terms.append(t["q_id"])
        if t.get("doi"):
            search_terms.append(f'"{t["doi"].strip().lower()}"')
        if t.get("pmid"):
            search_terms.append(t["pmid"].strip())
        if t.get("arxiv"):
            search_terms.append(f'"{t["arxiv"].strip().lower()}"')

    # 2. Wikipedia's CirrusSearch has a strict limit of 300 characters per query.
    # We dynamically chunk our terms so no single "OR" string exceeds 250 characters.
    queries = []
    current_chunk = []
    current_len = 0

    for term in search_terms:
        # Calculate added length (+4 accounts for the " OR " joiner)
        added_len = len(term) if not current_chunk else len(term) + 4

        if current_len + added_len > 250 and current_chunk:
            queries.append(" OR ".join(current_chunk))
            current_chunk = [term]
            current_len = len(term)
        else:
            current_chunk.append(term)
            current_len += added_len

    if current_chunk:
        queries.append(" OR ".join(current_chunk))

    # 3. Execute all dynamically sized search queries
    for query_str in queries:
        if not query_str:
            continue
        mentions = await find_mentions_via_search_query(lang, query_str)
        candidate_titles.update(mentions)

    if not candidate_titles:
        return []

    # 4. Fetch the candidate articles and parse them
    tasks = [
        _fetch_and_parse_multiple_mentions(lang, title, targets)
        for title in candidate_titles
    ]

    results = await asyncio.gather(*tasks, return_exceptions=True)

    extracted: list[dict] = []
    for result in results:
        if isinstance(result, Exception):
            logging.warning(f"Batched mention parse failed ({lang}): {result}")
        else:
            extracted.extend(result)

    return extracted

# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

async def scrape_targets(targets: list[dict], main_qid: str) -> list[dict]:
    """Takes a pre-filtered list of targets to strictly search Wikipedia for."""
    if not targets:
        return []

    main_qid = main_qid.strip().upper()
    sitelinks = await get_sitelinks_from_qid(main_qid)

    base_langs = {"en", "de", "da", "fr", "es", "sv", "no", "nl"}
    if sitelinks:
        base_langs.update(sitelinks.keys())

    mention_tasks = []
    for lang in base_langs:
        mention_tasks.append(_scrape_mentions_batched(lang, targets))

    all_references = []
    if mention_tasks:
        results = await asyncio.gather(*mention_tasks, return_exceptions=True)
        for result in results:
            if isinstance(result, Exception):
                logging.warning(f"Batched scraping task failed: {result}")
            else:
                all_references.extend(result)

    return all_references