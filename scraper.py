import httpx
import mwparserfromhell
import re
from urllib.parse import urlparse
from config import settings

async def scrape_wikipedia_references(url: str) -> dict:
    # Convert standard URL to raw wikitext URL
    # Example: https://en.wikipedia.org/wiki/Denmark -> https://en.wikipedia.org/w/index.php?title=Denmark&action=raw
    parsed_url = urlparse(url)
    domain = parsed_url.netloc
    title = parsed_url.path.split('/')[-1]
    language = domain.split('.')[0]

    raw_url = f"https://{domain}/w/index.php?title={title}&action=raw"

    headers = {
        "User-Agent": f"{settings.bot_name}/1.0 (Contact: {settings.contact_email})"
    }

    api_url = f"https://{domain}/w/api.php"
    api_params = {
        "action": "query",
        "prop": "pageprops",
        "titles": title,
        "format": "json"
    }



    async with httpx.AsyncClient(headers=headers) as client:
        response = await client.get(raw_url)
        response.raise_for_status()
        wikitext = response.text

        api_response = await client.get(api_url, params=api_params)
        api_data = api_response.json()

    article_q_id = None
    pages = api_data.get("query", {}).get("pages", {})
    for page_id, page_info in pages.items():
        if "pageprops" in page_info and "wikibase_item" in page_info["pageprops"]:
            article_q_id = page_info["pageprops"]["wikibase_item"]

    wikicode = mwparserfromhell.parse(wikitext)
    tags = wikicode.filter_tags(matches=lambda node: node.tag == "ref")
    #print(wikicode)
    #print("------------------")
    #print(wikicode.filter_templates)
    extracted_refs = []

    for tag in tags:
        # Extract text and strip whitespace
        raw_text = str(tag.contents).strip() if tag.contents else ""

        # Skip self-closing or completely empty reference tags
        if not raw_text:
            continue

        ref_data = {
            "raw_text": raw_text,
            "ref_type": "unknown",
            "doi": None,
            "pmid": None,
            "arxiv": None,
            "q_id": None
        }

        # Parse templates within the <ref> tag
        templates = tag.contents.filter_templates() if tag.contents else []
        for tpl in templates:
            ref_data["ref_type"] = tpl.name.strip().lower()

            #if "cite q" in tpl_name:
            #    ref_data["ref_type"] = "cite q"
            if ref_data["ref_type"] == "cite q" and tpl.has(1):
                ref_data["q_id"] = str(tpl.get(1).value).strip()

            #elif "cite journal" in tpl_name:
            #    ref_data["ref_type"] = "cite journal"
            if tpl.has("doi"):
                ref_data["doi"] = str(tpl.get("doi").value).strip()
            if tpl.has("pmid"):
                ref_data["pmid"] = str(tpl.get("pmid").value).strip()
            if tpl.has("arxiv"):
                ref_data["arxiv"] = str(tpl.get("arxiv").value).strip()

        extracted_refs.append(ref_data)

    return {
        "title": title,
        "q_id": article_q_id,
        "language": language,
        "url": url,
        "references": extracted_refs
    }