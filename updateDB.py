import asyncio
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy import text
from config import settings

engine = create_async_engine(settings.database_url)

async def fix_table():
    async with engine.begin() as conn:
        # This forces MySQL to update the column to match your Python code
        await conn.execute(text("ALTER TABLE articles MODIFY COLUMN name TEXT NOT NULL;"))
        print("Successfully updated the name column to TEXT!")
    await engine.dispose()
asyncio.run(fix_table())