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
    async with aiohttp.ClientSession() as session:
        last_successful_proxy = None
        current_proxy_index = 0

        while True:
            if last_successful_proxy:
                # Use the last successful proxy
                endpoint = last_successful_proxy
            else:
                # Use the current proxy from the list
                if current_proxy_index >= len(api_endpoints):
                    logging.error("All proxies have failed. No data fetched.")
                    return []
                endpoint = api_endpoints[current_proxy_index]

            try:
                logging.info(f"Trying to fetch data from {endpoint}")
                async with session.get(f"{endpoint}?size={batch_size}", timeout=5) as response:
                    logging.debug(f"Response status from {endpoint}: {response.status}")
                    if response.status == 200:
                        logging.info(f"Successfully fetched data from {endpoint}")
                        last_successful_proxy = endpoint  # Remember the working proxy
                        return await response.json()
                    else:
                        logging.error(f"Failed to fetch data from {endpoint}: {response.status}")
                        if last_successful_proxy:
                            # If this fails but was the last successful proxy, we clear it
                            last_successful_proxy = None
                        else:
                            # If not, move to the next proxy in the list
                            current_proxy_index += 1
            except asyncio.TimeoutError:
                logging.error(f"Timeout occurred while trying to fetch data from {endpoint}")
                if last_successful_proxy:
                    last_successful_proxy = None
                else:
                    current_proxy_index += 1
            except aiohttp.ClientError as e:
                logging.error(f"HTTP request failed at {endpoint}: {e}")
                if last_successful_proxy:
                    last_successful_proxy = None
                else:
                    current_proxy_index += 1
            except Exception as e:
                logging.error(f"An unexpected error occurred at {endpoint}: {e}")
                if last_successful_proxy:
                    last_successful_proxy = None
                else:
                    current_proxy_index += 1

            # Small delay before trying the next proxy to avoid hitting them too quickly
            await asyncio.sleep(RETRY_DELAY)

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

async def scrape(api_endpoints: List[str]) -> AsyncGenerator[List[Item], None]:
    """
    Main scraping logic that fetches and yields lists of parsed Item objects in batches.
    """
    BATCH_SIZE = 80  # Desired batch size
    batch = []  # Initialize an empty list to collect items

    try:
        await refill_queue(api_endpoints, max_items_to_fetch=QUEUE_MAX_SIZE)

        while True:
            item_queue.adjust_priorities()  # Adjust priorities before every fetch

            if len(item_queue.queue) <= QUEUE_REFILL_THRESHOLD:
                await refill_queue(api_endpoints, max_items_to_fetch=QUEUE_MAX_SIZE)

            if item_queue.queue:
                item = item_queue.get()
                batch.append(item)  # Add item to the current batch

                if len(batch) >= BATCH_SIZE:  # When batch size is reached, yield the batch
                    yield batch
                    batch = []  # Reset the batch list after yielding

    except GeneratorExit:
        # Gracefully exit the generator without raising an error
        logging.info("GeneratorExit: Closing the scrape generator gracefully.")
    except Exception as e:
        logging.error(f"An error occurred in the scrape generator: {e}")
        raise  # Re-raise other exceptions to handle them properly
    finally:
        # If there are leftover items in the batch, yield them before exiting
        if batch:
            yield batch


async def query(parameters: Dict[str, Any]) -> AsyncGenerator[List[Item], None]:
    """
    Main interface between the client core and the scraper. Yields batches of items.
    """
    api_endpoints = API_ENDPOINTS  # Default API endpoints, can be changed if needed
    maximum_items_to_collect = parameters.get("maximum_items_to_collect", DEFAULT_MAXIMUM_ITEMS)

    items_collected = 0

    async for batch in scrape(api_endpoints):
        yield batch
        items_collected += len(batch)
        
        if items_collected >= maximum_items_to_collect:
            break
