#Imports:
fastapi uvicorn sqlalchemy aiomysql httpx beautifulsoup4 mwparserfromhell pydantic_settings

#start server
uvicorn main:app --reload

#eksempel get
curl -X GET "http://localhost:8000/database/2/"

#eksempel scrape
curl -H "Content-Type: application/json" -d "{\"url\":\"https://en.wikipedia.org/wiki/Jeffrey_T._Williams\"}" -X POST "http://localhost:8000/scrape/"



#.ENV file structure (only thing to really change is the database_url, contact_email and potentially bot_name):
DATABASE_URL=mysql+aiomysql://username:password@localhost/wikidb
BOT_NAME=WikiCitationExtractor
BOT_VERSION=1.0
CONTACT_EMAIL=s205838@win.dtu.com
QLEVER_URL=https://qlever.dev/api/wikidata