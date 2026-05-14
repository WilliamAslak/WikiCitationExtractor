import asyncio
import logging
import httpx
import mwparserfromhell
import urllib
from urllib.parse import urlparse

from config import settings
from qlever import get_entity_context

_SEARCH_SEMAPHORE = asyncio.Semaphore(3)
_FETCH_SEMAPHORE = asyncio.Semaphore(10)


# ---------------------------------------------------------------------------
# Utility helpers
# ---------------------------------------------------------------------------
import re
def extract_context(wikitext: str, tag_str: str, window: int = 200) -> str:
    pos = wikitext.find(tag_str)
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
    headers = {"User-Agent": f"{settings.bot_name}/1.0 (Contact: {settings.contact_email})"}

    async with _FETCH_SEMAPHORE:
        async with httpx.AsyncClient(headers=headers, follow_redirects=True) as client:
            response = await client.get(url, params=params)
            response.raise_for_status()
            data = response.json()

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


async def find_mentions_via_search_query(
        lang: str, query_str: str, max_total: int = 50
) -> list[str]:
    endpoint = f"https://{lang}.wikipedia.org/w/api.php"
    headers = {"User-Agent": f"{settings.bot_name}/1.0 (Contact: {settings.contact_email})"}
    all_titles: list[str] = []

    data = {
        "action": "query",
        "list": "search",
        "srsearch": query_str,
        "srnamespace": "0",
        "srlimit": "50",
        "format": "json",
    }

    async with httpx.AsyncClient(headers=headers, follow_redirects=True) as client:
        while len(all_titles) < max_total:
            async with _SEARCH_SEMAPHORE:
                retries = 0
                while True:
                    try:
                        response = await client.post(endpoint, data=data, timeout=15.0)
                        if response.status_code == 429:
                            wait = 2 ** retries
                            logging.warning(
                                f"429 from {lang}.wikipedia.org search — backing off {wait}s"
                            )
                            await asyncio.sleep(wait)
                            retries += 1
                            if retries > 4:
                                return all_titles
                            continue
                        response.raise_for_status()
                        resp_json = response.json()
                        break
                    except httpx.HTTPError as e:
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

async def _fetch_and_parse_multiple_mentions(
        lang: str, candidate_title: str, targets: list[dict]
) -> list[dict]:
    domain = f"{lang}.wikipedia.org"

    # Safely format the title for the Wikipedia API by replacing spaces with underscores
    safe_title = candidate_title.replace(" ", "_")
    encoded_title = urllib.parse.quote(safe_title)

    raw_url = f"https://{domain}/w/index.php?title={encoded_title}&action=raw"
    headers = {"User-Agent": f"{settings.bot_name}/1.0 (Contact: {settings.contact_email})"}

    #print("fetching:",candidate_title)

    try:
        async with _FETCH_SEMAPHORE:
            async with httpx.AsyncClient(headers=headers, follow_redirects=True, timeout=30.0) as client:
                resp = await client.get(raw_url)
                resp.raise_for_status()
                wikitext = resp.text
    except Exception as e:
        logging.warning(f"Failed to fetch candidate '{candidate_title}' ({lang}): {repr(e)}")
        return []

    extracted: list[dict] = []
    wikicode = mwparserfromhell.parse(wikitext)
    tags = wikicode.filter_tags(matches=lambda node: node.tag == "ref")

    for tag in tags:
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


        matched_targets = {}
        for t in targets:
            match = False

            t_qid = t["q_id"].strip().upper()
            t_doi = t.get("doi").strip().lower() if t.get("doi") else None
            t_pmid = t.get("pmid").strip() if t.get("pmid") else None
            t_arxiv = t.get("arxiv").strip().lower() if t.get("arxiv") else None

            r_doi = ref_doi.strip().lower() if ref_doi else None
            r_pmid = ref_pmid.strip() if ref_pmid else None
            r_arxiv = ref_arxiv.strip().lower() if ref_arxiv else None
            r_qid = ref_q_id.strip().upper() if ref_q_id else None

            if r_qid and t_qid == r_qid:
                match = True
            elif r_doi and t_doi and t_doi == r_doi:
                match = True
            elif r_pmid and t_pmid and t_pmid == r_pmid:
                match = True
            elif r_arxiv and t_arxiv and t_arxiv == r_arxiv:
                match = True
            elif t_qid in tag_text.upper():
                match = True
            elif t_doi and t_doi in tag_text.lower():
                match = True

            if match:
                matched_targets[t["q_id"]] = t


        if not matched_targets:
            continue

        context_txt = extract_context(wikitext, str(tag))

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

async def scrape_all_languages_for_qid(q_id: str) -> dict:
    q_id = q_id.strip().upper()

    # Step 1: Network calls
    sitelinks, entity_context = await asyncio.gather(
        get_sitelinks_from_qid(q_id),
        get_entity_context(q_id),
    )

    # Note: We completely removed the direct page scraping here.
    # The scraper now strictly hunts for references to the QID / works.

    all_references: list[dict] = []
    targets_by_lang = {}
    works_targets = []

    primary_label = ""
    if entity_context.get("labels"):
        en_label = next((l["title"] for l in entity_context["labels"] if l["lang"] == "en"), None)
        primary_label = en_label if en_label else entity_context["labels"][0]["title"]

    # Target 1: The entity itself
    works_targets.append({
        "q_id": q_id,
        "label": primary_label,
        "doi": None,
        "pmid": None,
        "arxiv": None
    })

    # Target 2+: The entity's works
    for work in entity_context["works"]:
        works_targets.append({
            "q_id": work["work_qid"],
            "label": work.get("label", ""),
            "doi": work.get("doi"),
            "pmid": work.get("pmid"),
            "arxiv": work.get("arxiv")
        })

    if works_targets:
        # Base list to always search
        base_langs = {"en", "de", "da", "fr", "es", "sv", "no", "nl"}

        # Add any languages where the person actually has a biography
        if sitelinks:
            base_langs.update(sitelinks.keys())

        langs_to_search = list(base_langs)

        for lang in langs_to_search:
            targets_by_lang[lang] = works_targets


    mention_tasks = []
    for lang, targets in targets_by_lang.items():
        mention_tasks.append(_scrape_mentions_batched(lang, targets))

    if mention_tasks:
        mention_results = await asyncio.gather(*mention_tasks, return_exceptions=True)
        for result in mention_results:
            if isinstance(result, Exception):
                logging.warning(f"Batched scraping task failed: {result}")
            else:
                all_references.extend(result)

    return {
        "q_id": q_id,
        "name": primary_label,
        "references": all_references
    }