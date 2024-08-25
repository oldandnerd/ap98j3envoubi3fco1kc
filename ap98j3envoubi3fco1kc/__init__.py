import time
import heapq
import asyncio
import aiohttp
import logging
from typing import List, Dict, Any, AsyncGenerator
from exorde_data import Item, Content, Author, CreatedAt, Title, Url, Domain

# Configuration
API_ENDPOINTS = [
    "http://192.168.10.142:8000/fetch_reddit_posts",
    "http://84.52.244.233:8000/fetch_reddit_posts"
]
DEFAULT_MAXIMUM_ITEMS = 25  # Default number of items to collect
RETRY_DELAY = 5             # Delay in seconds before retrying
QUEUE_MAX_SIZE = 200        # Maximum size of the queue
QUEUE_REFILL_THRESHOLD = 20  # Threshold to trigger refill when queue size is below this value

# Configure logging
logging.basicConfig(level=logging.INFO)

class AgingPriorityQueue:
    def __init__(self):
        self.queue = []
        self.counter = 0

    def put(self, item, priority):
        heapq.heappush(self.queue, (priority, self.counter, time.time(), item))
        self.counter += 1

    def get(self):
        priority, _, timestamp, item = heapq.heappop(self.queue)
        return item

    def adjust_priorities(self):
        for i, (priority, counter, timestamp, item) in enumerate(self.queue):
            age = time.time() - timestamp
            new_priority = priority - age  # Increase priority with age
            self.queue[i] = (new_priority, counter, timestamp, item)
        heapq.heapify(self.queue)

# Create the aging priority queue
item_queue = AgingPriorityQueue()

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
    Refill the queue if it is empty or nearly empty.
    """
    current_size = len(item_queue.queue)

    if current_size <= QUEUE_REFILL_THRESHOLD:
        batch_size = min(QUEUE_MAX_SIZE - current_size, max_items_to_fetch)
        logging.info(f"Queue size {current_size} below threshold. Refilling queue with batch size: {batch_size}")
        
        data = await fetch_data(api_endpoints, batch_size)
        if data:
            for entry in data:
                parsed_item = await parse_item(entry)
                if parsed_item is not None:
                    item_queue.put(parsed_item, priority=0)  # Add new items with base priority
            logging.info(f"Refilled queue. New size: {len(item_queue.queue)}")
        else:
            logging.warning("No data fetched during refill attempt.")

async def scrape(api_endpoints: List[str]) -> AsyncGenerator[Item, None]:
    """
    Main scraping logic that fetches and yields parsed Item objects.
    """
    try:
        await refill_queue(api_endpoints, max_items_to_fetch=QUEUE_MAX_SIZE)

        while True:
            item_queue.adjust_priorities()  # Adjust priorities before every fetch

            if len(item_queue.queue) <= QUEUE_REFILL_THRESHOLD:
                await refill_queue(api_endpoints, max_items_to_fetch=QUEUE_MAX_SIZE)

            if item_queue.queue:
                item = item_queue.get()

                # Log the item before yielding
                logging.info(f"Yielding item: {item}")

                yield item
    except GeneratorExit:
        # Gracefully exit the generator without raising an error
        logging.info("GeneratorExit: Closing the scrape generator gracefully.")
        return  # Exit the generator without re-raising
    except Exception as e:
        logging.error(f"An error occurred in the scrape generator: {e}")
        raise  # Re-raise other exceptions to handle them properly

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
        #logging.info(f"Collected {items_collected} items so far.")
        
        if items_collected >= maximum_items_to_collect:
            break
