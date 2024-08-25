import aiohttp
import asyncio
import logging
from datetime import datetime, timezone
from typing import AsyncGenerator, Dict, Any, List
from exorde_data import Item, Content, Author, CreatedAt, Title, Url, Domain

# Configuration
API_ENDPOINTS = [
    "http://192.168.10.142:8000/fetch_reddit_posts",
    "http://84.52.244.233:8000/fetch_reddit_posts"
]
DEFAULT_MAXIMUM_ITEMS = 25  # Default number of items to collect
RETRY_DELAY = 5             # Delay in seconds before retrying
REFILL_THRESHOLD_PERCENT = 0.50  # Threshold percentage for refilling (now set to 50%)

# Global variables
GLOBAL_ITEM_LIST_MAX_SIZE = 200  # Maximum size of the global item list
global_item_list: List[Item] = []

# Configure logging
logging.basicConfig(level=logging.INFO)

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

async def refill_global_list(api_endpoints: List[str], total_capacity: int):
    """
    Refill the global item list if it is below the threshold percentage.
    """
    global global_item_list
    current_size = len(global_item_list)
    threshold = total_capacity * REFILL_THRESHOLD_PERCENT

    if current_size <= threshold:
        # Calculate the batch size dynamically based on the difference between max size and current size
        batch_size = GLOBAL_ITEM_LIST_MAX_SIZE - current_size
        logging.info(f"Refilling global item list. Current size: {current_size}, Batch size: {batch_size}")
        
        data = await fetch_data(api_endpoints, batch_size)
        new_items = []
        for entry in data:
            parsed_item = parse_item(entry)
            if parsed_item is not None:
                new_items.append(parsed_item)

        # Ensure the global item list doesn't exceed its maximum size
        global_item_list.extend(new_items)
        if len(global_item_list) > GLOBAL_ITEM_LIST_MAX_SIZE:
            # Trim the list to maintain the maximum size
            excess_items = len(global_item_list) - GLOBAL_ITEM_LIST_MAX_SIZE
            global_item_list = global_item_list[excess_items:]
            logging.info(f"Trimmed global item list to maximum size. New size: {len(global_item_list)}")
        else:
            logging.info(f"Refilled global item list. New size: {len(global_item_list)}")

async def scrape(api_endpoints: List[str]) -> AsyncGenerator[Item, None]:
    """
    Main scraping logic that fetches and yields parsed Item objects.
    """
    global global_item_list

    # Ensure global list is filled initially
    await refill_global_list(api_endpoints, DEFAULT_MAXIMUM_ITEMS)

    while True:
        if not global_item_list:
            await refill_global_list(api_endpoints, DEFAULT_MAXIMUM_ITEMS)

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
    api_endpoints = API_ENDPOINTS  # Default API endpoints, can be changed if needed
    maximum_items_to_collect = parameters.get("maximum_items_to_collect", DEFAULT_MAXIMUM_ITEMS)

    items_collected = 0

    async for item in scrape(api_endpoints):
        yield item
        items_collected += 1
        logging.info(f"Collected {items_collected} items so far.")
        
        if items_collected >= maximum_items_to_collect:
            break
