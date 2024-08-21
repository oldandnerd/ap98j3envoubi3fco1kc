import aiohttp
import asyncio
import logging
from datetime import datetime, timezone
from typing import AsyncGenerator
from exorde_data import Item, Content, Author, CreatedAt, Title, Url, Domain

API_ENDPOINT = "http://reddit_server:8081/fetch_reddit_posts"
DEFAULT_MAXIMUM_ITEMS = 25  # Default number of items to collect
DEFAULT_BATCH_SIZE = 20      # Default number of items to fetch per batch

# Configure logging
logging.basicConfig(level=logging.INFO)

class RedditScraperClient:
    def __init__(self, api_endpoint: str):
        self.api_endpoint = api_endpoint

    async def fetch_data(self, batch_size: int = DEFAULT_BATCH_SIZE) -> list:
        """
        Fetch data from the Reddit scraping server.
        """
        async with aiohttp.ClientSession() as session:
            async with session.get(f"{self.api_endpoint}?size={batch_size}") as response:
                if response.status == 200:
                    data = await response.json()
                    return data
                else:
                    logging.error(f"Failed to fetch data: {response.status}")
                    return []

    def parse_item(self, data: dict) -> Item:
        """
        Parse the dictionary data into an Item object using exorde_data classes.
        """
        content = Content(data.get("Content", ""))
        author = Author(data.get("Author", ""))  # Author is already hashed by the server
        created_at_raw = data.get("CreatedAt", "")
        title = Title(data.get("Title", ""))
        url = Url(data.get("Url", ""))
        domain = Domain(data.get("Domain", ""))

        # Convert created_at to the required format using CreatedAt class
        created_at_dt = datetime.strptime(created_at_raw, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
        created_at = CreatedAt(created_at_dt.isoformat())

        return Item(
            content=content,
            author=author,
            created_at=created_at,
            title=title,
            url=url,
            domain=domain,
        )

    async def scrape(self, batch_size: int) -> AsyncGenerator[Item, None]:
        """
        Main scraping logic that fetches and yields parsed Item objects.
        """
        raw_data = await self.fetch_data(batch_size=batch_size)
        for entry in raw_data:
            item = self.parse_item(entry)
            yield item

    async def query(self, parameters: dict) -> AsyncGenerator[Item, None]:
        """
        Main interface between the client core and the scraper. Yields items.
        """
        batch_size = parameters.get("size", DEFAULT_BATCH_SIZE)
        maximum_items_to_collect = parameters.get("maximum_items_to_collect", DEFAULT_MAXIMUM_ITEMS)

        items_collected = 0

        while items_collected < maximum_items_to_collect:
            async for item in self.scrape(batch_size=batch_size):
                yield item
                items_collected += 1
                logging.info(f"Collected {items_collected} items so far.")
                
                if items_collected >= maximum_items_to_collect:
                    break
