import asyncio
import importlib

try:
    AsyncMongoClient = getattr(importlib.import_module("pymongo"), "AsyncMongoClient")
except (ImportError, AttributeError):
    raise ImportError(
        "Missing MongoDB dependency. Install PyMongo with: pip install 'pymongo>=4.13'"
    )

async def main():
    uri = "mongodb+srv://morolong_top:<@password>@cluster0.wddmqpw.mongodb.net/"
    client = AsyncMongoClient(uri)

    await client.admin.command("ping")
    print("Connected successfully")
    await client.close()

asyncio.run(main())