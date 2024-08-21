import aiohttp
import asyncio
import logging
from datetime import datetime, timezone
from typing import AsyncGenerator, Dict, Any
from exorde_data import Item, Content, Author, CreatedAt, Title, Url, Domain

# Configuration
API_ENDPOINT = "http://reddit_server:8000/fetch_reddit_posts"
DEFAULT_MAXIMUM_ITEMS = 25  # Default number of items to collect
DEFAULT_BATCH_SIZE = 20     # Default number of items to fetch per batch

# Configure logging
logging.basicConfig(level=logging.INFO)

async def fetch_data(api_endpoint: str, batch_size: int = DEFAULT_BATCH_SIZE) -> list:
    """
    Fetch data from the Reddit scraping server.
    """
    async with aiohttp.ClientSession() as session:
        async with session.get(f"{api_endpoint}?size={batch_size}") as response:
            if response.status == 200:
                data = await response.json()
                return data
            else:
                logging.error(f"Failed to fetch data: {response.status}")
                return []

def parse_item(data: dict) -> Item:
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
    try:
        # Parse the timestamp assuming it is in UTC
        created_at_dt = datetime.strptime(created_at_raw, "%Y-%m-%d %H:%M:%S")
        # Format to ISO8601 with Z suffix
        created_at_str = created_at_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
        created_at = CreatedAt(created_at_str)
    except ValueError as e:
        logging.error(f"Error parsing CreatedAt timestamp: {e}")
        # Handle the error as needed, maybe raise an exception or return a default value
        created_at = CreatedAt("1970-01-01T00:00:00Z")  # Default value if parsing fails

    return Item(
        content=content,
        author=author,
        created_at=created_at,
        title=title,
        url=url,
        domain=domain,
    )

async def scrape(api_endpoint: str, batch_size: int) -> AsyncGenerator[Item, None]:
    """
    Main scraping logic that fetches and yields parsed Item objects.
    """
    raw_data = await fetch_data(api_endpoint, batch_size=batch_size)
    for entry in raw_data:
        item = parse_item(entry)
        yield item

async def query(parameters: Dict[str, Any]) -> AsyncGenerator[Item, None]:
    """
    Main interface between the client core and the scraper. Yields items.
    """
    api_endpoint = API_ENDPOINT  # Default API endpoint, can be changed if needed
    batch_size = parameters.get("size", DEFAULT_BATCH_SIZE)
    maximum_items_to_collect = parameters.get("maximum_items_to_collect", DEFAULT_MAXIMUM_ITEMS)

    items_collected = 0

    while items_collected < maximum_items_to_collect:
        async for item in scrape(api_endpoint, batch_size=batch_size):
            yield item
            items_collected += 1
            logging.info(f"Collected {items_collected} items so far.")
            
            if items_collected >= maximum_items_to_collect:
                break

