import aiohttp
import asyncio
import hashlib
import logging
import re
from datetime import datetime, timezone
from typing import AsyncGenerator, Dict
from wordsegment import load, segment
from exorde_data import Item, Content, Author, CreatedAt, Url, Domain
from aiohttp import ClientConnectorError

logging.basicConfig(level=logging.INFO)

MANAGER_IP = "http://192.227.159.3:8000"
USER_AGENT = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36'
MAX_CONCURRENT_TASKS = 20
DEFAULT_NUMBER_SUBREDDIT_ATTEMPTS = 20  # default value if not provided

load()  # Load the wordsegment library data

class CommentCollector:
    def __init__(self, max_items):
        self.total_items_collected = 0
        self.max_items = max_items
        self.items = []
        self.processed_ids = set()  # To track processed comment IDs
        self.lock = asyncio.Lock()
        self.stop_fetching = False
        self.logged_stop_message = False

    async def add_item(self, item, item_id):
        async with self.lock:
            if self.total_items_collected < self.max_items and item_id not in self.processed_ids:
                self.items.append(item)
                self.processed_ids.add(item_id)
                self.total_items_collected += 1
                if self.total_items_collected >= self.max_items:
                    self.stop_fetching = True
                    if not self.logged_stop_message:
                        logging.info("Maximum items collected, stopping further fetches.")
                        self.logged_stop_message = True
                return True
            return False

    def should_stop_fetching(self):
        return self.stop_fetching

async def fetch_with_proxy(session, url):
    headers = {'User-Agent': USER_AGENT}
    try:
        async with session.get(f'{MANAGER_IP}/proxy?url={url}', headers=headers) as response:
            response.raise_for_status()
            return await response.json()
    except ClientConnectorError as e:
        logging.error(f"Error fetching URL {url}: Cannot connect to host {MANAGER_IP} ssl:default [{e}]")
        logging.info("Proxy servers are offline at the moment. Retrying in 10 seconds...")
        await asyncio.sleep(10)
        return None
    except aiohttp.ClientResponseError as e:
        error_message = await response.json()
        if e.status == 404 and 'reason' in error_message and error_message['reason'] == 'banned':
            logging.error(f"Error fetching URL {url}: Subreddit is banned.")
        elif e.status == 403 and 'reason' in error_message and error_message['reason'] == 'private':
            logging.error(f"Error fetching URL {url}: Subreddit is private.")
        else:
            logging.error(f"Error fetching URL {url}: {e.message}")
        return None
    except Exception as e:
        logging.error(f"Error fetching URL {url}: {e}")
        return None

def format_timestamp(timestamp):
    return datetime.fromtimestamp(timestamp, timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')

def is_within_timeframe_seconds(created_utc, max_oldness_seconds, current_time):
    return (current_time - created_utc) <= max_oldness_seconds

def get_age_string(created_utc, current_time):
    age_seconds = current_time - created_utc
    age_minutes = age_seconds // 60
    age_hours = age_minutes // 60
    age_days = age_hours // 24

    if age_days > 0:
        return f"{int(age_days)} days old"
    elif age_hours > 0:
        return f"{int(age_hours)} hours old"
    elif age_minutes > 0:
        return f"{int(age_minutes)} minutes old"
    else:
        return f"{int(age_seconds)} seconds old"

def correct_reddit_url(url):
    parts = url.split("https://reddit.comhttps://", 1)
    if len(parts) == 2:
        corrected_url = "https://" + parts[1]
        return corrected_url
    return url

def extract_subreddit_name(input_string):
    match = re.search(r'r/([^/]+)', input_string)
    if match:
        return match.group(1)
    return None

def post_process_item(item):
    try:
        if len(item.content) > 10:
            subreddit_name = extract_subreddit_name(item.url)
            if subreddit_name is None:
                return item
            segmented_subreddit_strs = segment(subreddit_name)
            segmented_subreddit_name = " ".join(segmented_subreddit_strs)
            item.content = Content(item.content + ". - " + segmented_subreddit_name + " ," + subreddit_name)
    except Exception as e:
        logging.exception(f"[Reddit post_process_item] Word segmentation failed: {e}, ignoring...")
    try:
        item.url = Url(correct_reddit_url(item.url))
    except:
        logging.warning(f"[Reddit] failed to correct the URL of item {item.url}")
    return item

async def fetch_comments(session, post_permalink, collector, max_oldness_seconds, min_post_length, current_time):
    comments_url = f"https://www.reddit.com{post_permalink}.json"
    comments_json = await fetch_with_proxy(session, comments_url)
    if not comments_json or len(comments_json) <= 1:
        return

    comments = comments_json[1]['data']['children']
    for comment in comments:
        if collector.should_stop_fetching():
            return

        if comment['kind'] != 't1':
            continue

        comment_data = comment['data']
        comment_created_at = comment_data['created_utc']

        # Skip comments by AutoModerator
        if comment_data.get('author') == 'AutoModerator':
            continue

        if not is_within_timeframe_seconds(comment_created_at, max_oldness_seconds, current_time):
            continue

        comment_content = comment_data.get('body', '[deleted]')
        comment_author = comment_data.get('author', '[unknown]')
        comment_url = f"https://reddit.com{comment_data['permalink']}"
        comment_id = comment_data['name']  # Extracting the comment ID

        if len(comment_content) >= min_post_length:
            item = Item(
                content=Content(comment_content),
                author=Author(hashlib.sha1(bytes(comment_author, encoding="utf-8")).hexdigest()),
                created_at=CreatedAt(format_timestamp(comment_created_at)),
                domain=Domain("reddit.com"),
                url=Url(comment_url),
            )

            if not await collector.add_item(item, comment_id):  # Using comment_id to avoid duplicates
                return

async def fetch_posts(session, subreddit_url, collector, max_oldness_seconds, min_post_length, current_time):
    response_json = await fetch_with_proxy(session, subreddit_url)
    if not response_json or 'data' not in response_json or 'children' not in response_json['data']:
        return

    posts = response_json['data']['children']
    tasks = []
    for post in posts:
        if collector.should_stop_fetching():
            return

        post_kind = post.get('kind')
        post_info = post.get('data', {})

        if post_kind != 't3':
            continue

        post_permalink = post_info.get('permalink')
        post_created_at = post_info.get('created_utc', 0)

        if not is_within_timeframe_seconds(post_created_at, max_oldness_seconds, current_time):
            continue

        tasks.append(fetch_comments(session, post_permalink, collector, max_oldness_seconds, min_post_length, current_time))

        if collector.should_stop_fetching():
            break

    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)

async def query(parameters: Dict) -> AsyncGenerator[Item, None]:
    max_oldness_seconds = parameters.get('max_oldness_seconds')
    maximum_items_to_collect = parameters.get('maximum_items_to_collect', 1000)
    min_post_length = parameters.get('min_post_length')
    batch_size = parameters.get('batch_size', 20)
    nb_subreddit_attempts = parameters.get('nb_subreddit_attempts', DEFAULT_NUMBER_SUBREDDIT_ATTEMPTS)

    # Log input parameters
    logging.info(f"[Reddit] Input parameters: max_oldness_seconds={max_oldness_seconds}, "
                 f"maximum_items_to_collect={maximum_items_to_collect}, min_post_length={min_post_length}, "
                 f"batch_size={batch_size}, nb_subreddit_attempts={nb_subreddit_attempts}")

    collector = CommentCollector(maximum_items_to_collect)
    current_time = datetime.now(timezone.utc).timestamp()

    session = aiohttp.ClientSession()

    try:
        url_response = await fetch_with_proxy(session, f'{MANAGER_IP}/get_urls?batch_size={batch_size}')
        if not url_response or 'urls' not in url_response:
            logging.error("Failed to get subreddit URLs from proxy")
            return

        subreddit_urls = url_response['urls']

        semaphore = asyncio.Semaphore(MAX_CONCURRENT_TASKS)
        async def limited_fetch(subreddit_url):
            for attempt in range(nb_subreddit_attempts):
                async with semaphore:
                    await fetch_posts(session, subreddit_url.rstrip('/') + '/.json' if not subreddit_url.endswith('.json') else subreddit_url, collector, max_oldness_seconds, min_post_length, current_time)
                if collector.should_stop_fetching():
                    break

        tasks = [limited_fetch(subreddit_url) for subreddit_url in subreddit_urls]
        await asyncio.gather(*tasks, return_exceptions=True)

        try:
            for index, item in enumerate(collector.items, start=1):
                created_at_timestamp = datetime.strptime(item.created_at, '%Y-%m-%dT%H:%M:%SZ').timestamp()
                age_string = get_age_string(created_at_timestamp, current_time)
                item = post_process_item(item)
                logging.info(f"Found comment {index} and it's {age_string}: {item}")
                yield item
        except GeneratorExit:
            logging.info("Async generator received GeneratorExit, performing cleanup.")
            raise
    finally:
        logging.info("Cleaning up: closing session.")
        await session.close()
        logging.info("Session closed.")
        logging.info("End of iterator - StopAsyncIteration")
