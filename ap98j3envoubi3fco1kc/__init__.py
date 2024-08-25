import aiohttp
import asyncio
import logging
from datetime import datetime, timezone
from typing import AsyncGenerator, Dict, Any
from exorde_data import Item, Content, Author, CreatedAt, Title, Url, Domain

# Configuration
API_ENDPOINTS = [
    "http://192.168.10.142:8000/fetch_reddit_posts",
    "http://84.52.244.233:8000/fetch_reddit_posts"
]
DEFAULT_MAXIMUM_ITEMS = 25  # Default number of items to collect
RETRY_DELAY = 5             # Delay in seconds before retrying
REFILL_THRESHOLD_PERCENT = 0.50  # Threshold percentage for refilling (50%)
QUEUE_MAX_SIZE = 200        # Maximum size of the queue

# Configure logging
logging.basicConfig(level=logging.INFO)

# Create an asynchronous queue to hold fetched items
item_queue = asyncio.Queue(maxsize=QUEUE_MAX_SIZE)

async def fetch_data(api_endpoints: List[str], batch_size: int) -> list:
    """
    Fetch data from the Reddit scraping servers with retry on failure. Tries multiple endpoints.
    """
    async with aiohttp.ClientSession() as session:
        for endpoint in api_endpoints:
            for attempt in range(3):  # Number of retry attempts per endpoint
                try:
                    async with session.get(f"{endpoint}?size={batch_size}") as response:
                        if response.status == 200:
                            return await response.json()
                        elif response.status == 404:
                            logging.error(f"Data not found (404) at {endpoint}. Retrying...")
                        else:
                            logging.error(f"Failed to fetch data from {endpoint}: {response.status}")
                except aiohttp.ClientError as e:
                    logging.error(f"HTTP request failed at {endpoint}: {e}")
                await asyncio.sleep(RETRY_DELAY)  # Centralized sleep
            logging.error(f"Endpoint {endpoint} failed after multiple attempts.")
    logging.error("All endpoints failed. No data fetched.")
    return []

async def parse_item(data: dict) -> Item:
    """
    Parse the dictionary data into an Item object using exorde_data classes.
    If parsing is complex, this function could be made asynchronous.
    """
    content = Content(data.get("Content", ""))
    author = Author(data.get("Author", ""))  # Author is already hashed by the server
    created_at_raw = data.get("CreatedAt", "")
    title = Title(data.get("Title", ""))
    url = Url(data.get("Url", ""))
    domain = Domain(data.get("Domain", ""))

    # Skip item if CreatedAt cannot be parsed
    try:
        created_at = CreatedAt(created_at_raw)
    except ValueError as e:
        logging.error(f"Skipping item due to error parsing CreatedAt timestamp: {e}")
        return None

    return Item(
        content=content,
        author=author,
        created_at=created_at,
        title=title,
        url=url,
        domain=domain,
    )

async def refill_queue(api_endpoints: List[str], max_items_to_fetch: int):
    """
    Refill the queue if it is below the threshold percentage.
    """
    current_size = item_queue.qsize()
    threshold = QUEUE_MAX_SIZE * REFILL_THRESHOLD_PERCENT

    if current_size <= threshold:
        batch_size = min(QUEUE_MAX_SIZE - current_size, max_items_to_fetch)
        logging.info(f"Refilling queue. Current size: {current_size}, Batch size: {batch_size}")
        
        data = await fetch_data(api_endpoints, batch_size)
        for entry in data:
            parsed_item = await parse_item(entry)
            if parsed_item is not None:
                await item_queue.put(parsed_item)

        logging.info(f"Refilled queue. New size: {item_queue.qsize()}")

async def scrape(api_endpoints: List[str]) -> AsyncGenerator[Item, None]:
    """
    Main scraping logic that fetches and yields parsed Item objects.
    """
    # Ensure queue is filled initially
    await refill_queue(api_endpoints, max_items_to_fetch=QUEUE_MAX_SIZE)

    while True:
        # Refill the queue if it's running low
        if item_queue.qsize() <= QUEUE_MAX_SIZE * REFILL_THRESHOLD_PERCENT:
            await refill_queue(api_endpoints, max_items_to_fetch=QUEUE_MAX_SIZE)

        try:
            item = await item_queue.get()
            yield item
            item_queue.task_done()
        except asyncio.CancelledError:
            break

async def query(parameters: Dict[str, Any]) -> AsyncGenerator[Item, None]:
    """
    Main interface between the client core and the scraper. Yields items.
    """
    api_endpoints = API_ENDPOINTS  # Default API endpoints, can be changed if needed
    maximum_items_to_collect = parameters.get("maximum_items_to_collect", DEFAULT_MAXIMUM_ITEMS)

    items_collected = 0

    async for item in scrape(api_endpoints):
        yield item
        items_collected += 1
        logging.info(f"Collected {items_collected} items so far.")
        
        if items_collected >= maximum_items_to_collect:
            break
