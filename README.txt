#Imports:
fastapi uvicorn sqlalchemy aiomysql httpx mwparserfromhell pydantic-settings cryptography

Can be installed with pip
pip install fastapi uvicorn sqlalchemy aiomysql httpx mwparserfromhell pydantic-settings cryptography

#start server
uvicorn main:app --reload

# Once the server is running, you can interact with the API (default: http://localhost:8000).
GET /: Root endpoint with a welcome message and directory of endpoints.
GET /ok/: Health check to verify the API is running.
GET /refresh/: Triggers a scrape of each item currently stored in the local database.
GET /scrape/{q_id}/: Triggers a Wikipedia scrape and database sync for a specific Wikidata Q-ID.
GET /database/dump/: Returns a summary of all articles currently stored in the local database.
GET /database/{q_id}/: Returns the complete stored reference data for a specific Q-ID.
GET /stats/: Provides 'cite q' template usage of items in the database.
GET /author/{q_id}/: Fetches all works by an author from Wikidata and checks if they exist in the local references.
GET /referenced/{q_id}/: Finds where a specific entity (author or work) is cited and cross-references it with the local database.
GET /example/1/: Fetches a list of the most Wikipedia-referenced female employees of DTU.
GET /institute/{institute_qid}/: Fetches a list of the most Wikipedia-referenced employees at a given institute.

# Example usage check database:
curl -X GET "http://localhost:8000/example/dump/"
# Example scrape of Albert Einstein:
curl -X GET "http://localhost:8000/scrape/Q937/"

#.ENV file structure (only thing to really change is the database_url, contact_email and potentially bot_name):
DATABASE_URL=mysql+aiomysql://username:password@localhost/wikidb
BOT_NAME=WikiCitationExtractor
BOT_VERSION=1.0
CONTACT_EMAIL=s205838@win.dtu.com
QLEVER_URL=https://qlever.dev/api/wikidata