import logging
import httpx
import mwparserfromhell
import re
import urllib
from urllib.parse import urlparse
from config import settings
from qlever import get_wikidata_labels

def extract_context(wikitext: str, tag_str: str, window: int = 200) -> str:
    # Finds the tag in the raw wikitext and extracts only the preceding characters,
    # stopping if it hits the boundary of a previous reference.
    pos = wikitext.find(tag_str)
    if pos == -1:
        return ""

    start = max(0, pos - window)
    left_text = wikitext[start:pos]

    # Check if a standard reference ends in our left window
    last_ref_close = left_text.rfind("</ref>")
    if last_ref_close != -1:
        start += last_ref_close + 6  # length of "</ref>"
    else:
        # Check for self-closing references (e.g., <ref name="xyz" />)
        last_ref_open = left_text.rfind("<ref")
        if last_ref_open != -1:
            close_pos = left_text.find("/>", last_ref_open)
            if close_pos != -1:
                start += close_pos + 2  # length of "/>"

    return wikitext[start:pos].strip()

async def get_q_id_from_name(name: str) -> str | None:
    search_term = name.replace("_", " ")
    url = "https://www.wikidata.org/w/api.php"
    params = {
        "action": "wbsearchentities",
        "search": search_term,
        "language": "en",
        "format": "json"
    }
    headers = {"User-Agent": f"{settings.bot_name}/1.0 (Contact: {settings.contact_email})"}

    async with httpx.AsyncClient(headers=headers, follow_redirects=True) as client:
        response = await client.get(url, params=params)
        response.raise_for_status()
        data = response.json()
        if data.get("search"):
            return data["search"][0]["id"]
    return None


async def get_sitelinks_from_qid(q_id: str) -> dict[str, str]:
    q_id = q_id.strip().upper()
    url = "https://www.wikidata.org/w/api.php"
    params = {
        "action": "wbgetentities",
        "ids": q_id,
        "props": "sitelinks/urls",
        "format": "json"
    }
    headers = {"User-Agent": f"{settings.bot_name}/1.0 (Contact: {settings.contact_email})"}

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
                lang = parsed_url.netloc.split('.')[0]
                urls[lang] = site_url
        return urls


async def scrape_single_wikipedia_page(url: str, language: str) -> list[dict]:
    parsed_url = urlparse(url)

    domain = parsed_url.netloc
    title = parsed_url.path.split('/')[-1]
    raw_url = f"https://{domain}/w/index.php?title={title}&action=raw"
    headers = {"User-Agent": f"{settings.bot_name}/1.0 (Contact: {settings.contact_email})"}

    async with httpx.AsyncClient(headers=headers, follow_redirects=True) as client:
        response = await client.get(raw_url)
        response.raise_for_status()
        wikitext = response.text

    wikicode = mwparserfromhell.parse(wikitext)
    tags = wikicode.filter_tags(matches=lambda node: node.tag == "ref")
    extracted_refs = []

    for tag in tags:
        raw_text = str(tag.contents).strip() if tag.contents else ""
        if not raw_text:
            continue

        ref_data = {
            "raw_text": raw_text,
            "context_text": extract_context(wikitext, str(tag)),
            "ref_type": "unknown",
            "doi": None,
            "pmid": None,
            "arxiv": None,
            "q_id": None,
            "language": language,
            "source_url": url
        }

        templates = tag.contents.filter_templates() if tag.contents else []
        for tpl in templates:
            ref_data["ref_type"] = tpl.name.strip().lower()[:50]

            if ref_data["ref_type"] == "cite q" and tpl.has(1):
                ref_data["q_id"] = str(tpl.get(1).value).strip()

            if tpl.has("doi"):
                ref_data["doi"] = str(tpl.get("doi").value).strip()
            if tpl.has("pmid"):
                ref_data["pmid"] = str(tpl.get("pmid").value).strip()
            if tpl.has("arxiv"):
                ref_data["arxiv"] = str(tpl.get("arxiv").value).strip()

        extracted_refs.append(ref_data)

    return extracted_refs


async def scrape_all_languages_for_qid(q_id: str) -> dict:
    q_id = q_id.strip().upper()
    sitelinks = await get_sitelinks_from_qid(q_id)

    # BUG FIX: Removed the `raise ValueError` exception here.
    # If a Q-ID has no direct article, it will simply skip the direct scraping
    # and move on to searching for mentions.

    all_references = []

    # 1. Scrape direct references from the entity's own Wikipedia pages
    for lang, url in sitelinks.items():
        try:
            refs = await scrape_single_wikipedia_page(url, lang)
            all_references.extend(refs)
        except Exception as e:
            logging.warning(f"Failed to scrape {url} ({lang}): {e}")

    # 2. Scrape mentions from other articles using localized Wikidata labels
    labels = await get_wikidata_labels(q_id)

    for label_data in labels:
        lang = label_data["lang"]
        label = label_data["title"]

        mentions = await find_mentions_via_search(lang, label)

        for mention_title in mentions:
            try:
                domain = f"{lang}.wikipedia.org"
                encoded_title = urllib.parse.quote(mention_title)
                raw_url = f"https://{domain}/w/index.php?title={encoded_title}&action=raw"
                headers = {"User-Agent": f"{settings.bot_name}/1.0 (Contact: {settings.contact_email})"}

                async with httpx.AsyncClient(headers=headers, follow_redirects=True) as client:
                    resp = await client.get(raw_url)
                    resp.raise_for_status()
                    wikitext = resp.text

                # Extract context directly from the raw wikitext
                context = extract_text_context(wikitext, label)

                if context:
                    all_references.append({
                        "raw_text": label,
                        "context_text": context,
                        "ref_type": "text_mention",
                        "doi": None,
                        "pmid": None,
                        "arxiv": None,
                        "q_id": q_id,
                        "language": lang,
                        "source_url": f"https://{domain}/wiki/{encoded_title}"
                    })
            except Exception as e:
                logging.warning(f"Failed to scrape mention '{mention_title}' ({lang}): {e}")

    return {
        "q_id": q_id,
        "references": all_references
    }


def extract_text_context(text: str, search_term: str, window: int = 200) -> str:
    """Finds a plain text search term and extracts the surrounding context window."""
    pos = text.find(search_term)
    if pos == -1:
        # Fallback to case-insensitive match
        pos = text.lower().find(search_term.lower())
        if pos == -1:
            return ""

    start = max(0, pos - window)
    end = min(len(text), pos + len(search_term) + window)
    return text[start:end].strip()


async def find_mentions_via_search(lang: str, title: str, limit: int = 5) -> list[str]:
    """Searches a specific language Wikipedia for mentions of a title."""
    endpoint = f"https://{lang}.wikipedia.org/w/api.php"
    params = {
        "action": "query",
        "list": "search",
        "srsearch": f'"{title}"',
        "srlimit": str(limit),  # Kept low to avoid massive scraping delays
        "format": "json"
    }
    headers = {"User-Agent": f"{settings.bot_name}/1.0 (Contact: {settings.contact_email})"}

    async with httpx.AsyncClient(headers=headers, follow_redirects=True) as client:
        try:
            response = await client.get(endpoint, params=params, timeout=10)
            response.raise_for_status()
            data = response.json()
            return [item["title"] for item in data.get("query", {}).get("search", [])]
        except Exception as e:
            logging.warning(f"Could not reach {lang}.wikipedia.org for mentions: {e}")
            return []