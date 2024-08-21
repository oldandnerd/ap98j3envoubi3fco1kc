import aiohttp
import asyncio
import logging
from datetime import datetime, timezone
from typing import AsyncGenerator, Dict, Any, List
from exorde_data import Item, Content, Author, CreatedAt, Title, Url, Domain

# Configuration
API_ENDPOINT = "http://reddit_server:8000/fetch_reddit_posts"
DEFAULT_MAXIMUM_ITEMS = 25  # Default number of items to collect
DEFAULT_BATCH_SIZE = 60     # Default number of items to fetch per batch
RETRY_DELAY = 5             # Delay in seconds before retrying
REFILL_THRESHOLD_PERCENT = 0.10  # Threshold percentage for refilling

# Global list to hold items
global_item_list: List[Item] = []

# Configure logging
logging.basicConfig(level=logging.INFO)

async def fetch_data(api_endpoint: str, batch_size: int = DEFAULT_BATCH_SIZE) -> list:
    """
    Fetch data from the Reddit scraping server with retry on 404 error.
    """
    for attempt in range(3):  # Number of retry attempts
        async with aiohttp.ClientSession() as session:
            try:
                async with session.get(f"{api_endpoint}?size={batch_size}") as response:
                    if response.status == 200:
                        data = await response.json()
                        return data
                    elif response.status == 404:
                        logging.error(f"Data not found (404). Retrying in {RETRY_DELAY} seconds...")
                        await asyncio.sleep(RETRY_DELAY)
                    else:
                        logging.error(f"Failed to fetch data: {response.status}")
                        return []
            except aiohttp.ClientError as e:
                logging.error(f"HTTP request failed: {e}")
                await asyncio.sleep(RETRY_DELAY)
    logging.error("Max retries reached. No data fetched.")
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
        created_at_dt = datetime.strptime(created_at_raw, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
        created_at_str = created_at_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
        created_at = CreatedAt(created_at_str)
    except ValueError as e:
        logging.error(f"Error parsing CreatedAt timestamp: {e}")
        created_at = CreatedAt("1970-01-01T00:00:00Z")  # Default value if parsing fails

    return Item(
        content=content,
        author=author,
        created_at=created_at,
        title=title,
        url=url,
        domain=domain,
    )

async def refill_global_list(api_endpoint: str, batch_size: int, total_capacity: int):
    """
    Refill the global item list if it is below the threshold percentage.
    """
    global global_item_list
    current_size = len(global_item_list)
    threshold = total_capacity * REFILL_THRESHOLD_PERCENT

    if current_size <= threshold:
        logging.info(f"Refilling global item list. Current size: {current_size}, Threshold: {threshold}")
        data = await fetch_data(api_endpoint, batch_size)
        new_items = [parse_item(entry) for entry in data]
        global_item_list.extend(new_items)
        logging.info(f"Refilled global item list. New size: {len(global_item_list)}")

async def scrape(api_endpoint: str, batch_size: int) -> AsyncGenerator[Item, None]:
    """
    Main scraping logic that fetches and yields parsed Item objects.
    """
    global global_item_list

    # Ensure global list is filled initially
    await refill_global_list(api_endpoint, batch_size, DEFAULT_MAXIMUM_ITEMS)

    while True:
        if not global_item_list:
            await refill_global_list(api_endpoint, batch_size, DEFAULT_MAXIMUM_ITEMS)

        if global_item_list:
            item = global_item_list.pop(0)
            yield item
        else:
            # Break the loop if no items are available and cannot refill
            break

async def query(parameters: Dict[str, Any]) -> AsyncGenerator[Item, None]:
    """
    Main interface between the client core and the scraper. Yields items.
    """
    api_endpoint = API_ENDPOINT  # Default API endpoint, can be changed if needed
    batch_size = parameters.get("size", DEFAULT_BATCH_SIZE)
    maximum_items_to_collect = parameters.get("maximum_items_to_collect", DEFAULT_MAXIMUM_ITEMS)

    items_collected = 0

    async for item in scrape(api_endpoint, batch_size=batch_size):
        yield item
        items_collected += 1
        logging.info(f"Collected {items_collected} items so far.")
        
        if items_collected >= maximum_items_to_collect:
            break

