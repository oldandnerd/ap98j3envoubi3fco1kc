import aiohttp
import asyncio
import hashlib
import logging
from datetime import datetime, timezone
from typing import AsyncGenerator, Dict
from exorde_data import (
    Item,
    Content,
    Author,
    CreatedAt,
    Title,
    Url,
    Domain,
)

logging.basicConfig(level=logging.INFO)

MANAGER_IP = "http://192.227.159.3:8000"
USER_AGENT = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36'

class CommentCollector:
    def __init__(self, max_items):
        self.total_items_collected = 0
        self.max_items = max_items
        self.items = []
        self.lock = asyncio.Lock()
        self.stop_fetching = False
        self.logged_stop_message = False

    async def add_item(self, item):
        async with self.lock:
            if self.total_items_collected < self.max_items:
                self.items.append(item)
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
    except Exception as e:
        logging.error(f"Error fetching URL {url}: {e}")
        return None

def format_timestamp(timestamp):
    return datetime.fromtimestamp(timestamp, timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')

def is_within_timeframe_seconds(created_utc, max_oldness_seconds):
    current_time = datetime.now(timezone.utc).timestamp()
    return (current_time - created_utc) <= max_oldness_seconds

def get_age_string(created_utc):
    current_time = datetime.now(timezone.utc).timestamp()
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

async def fetch_comments(session, post_permalink, collector, max_oldness_seconds, min_post_length):
    comments_url = f"https://www.reddit.com{post_permalink}.json"
    comments_json = await fetch_with_proxy(session, comments_url)
    if comments_json and len(comments_json) > 1:
        comments = comments_json[1]['data']['children']
        for comment in comments:
            if comment['kind'] == 't1':
                comment_data = comment['data']
                comment_content = comment_data.get('body', '[deleted]')
                comment_author = comment_data.get('author', '[unknown]')
                comment_created_at = comment_data['created_utc']
                comment_url = f"https://reddit.com{comment_data['permalink']}"

                if (len(comment_content) >= min_post_length and
                    is_within_timeframe_seconds(comment_created_at, max_oldness_seconds)):

                    item = Item(
                        content=Content(comment_content),
                        author=Author(hashlib.sha1(bytes(comment_author, encoding="utf-8")).hexdigest()),
                        created_at=CreatedAt(format_timestamp(comment_created_at)),
                        title=Title(post_permalink),
                        domain=Domain("reddit.com"),
                        url=Url(comment_url),
                    )

                    if not await collector.add_item(item):
                        return

async def fetch_posts(session, subreddit_url, collector, max_oldness_seconds, min_post_length):
    response_json = await fetch_with_proxy(session, subreddit_url)

    if not response_json:
        logging.error("Response JSON is empty or invalid")
        return

    # Check the structure of the response JSON
    if 'data' not in response_json or 'children' not in response_json['data']:
        logging.error("Unexpected JSON structure")
        return

    posts = response_json['data']['children']

    for post in posts:
        if collector.should_stop_fetching():
            return

        post_kind = post.get('kind')
        post_info = post.get('data', {})

        if post_kind == 't3':
            post_permalink = post_info.get('permalink')
            if post_permalink:
                await fetch_comments(session, post_permalink, collector, max_oldness_seconds, min_post_length)
                if collector.should_stop_fetching():
                    return

async def query(parameters: Dict) -> AsyncGenerator[Item, None]:
    max_oldness_seconds = parameters.get('max_oldness_seconds')
    maximum_items_to_collect = parameters.get('maximum_items_to_collect', 1000)
    min_post_length = parameters.get('min_post_length')
    batch_size = parameters.get('batch_size', 20)

    collector = CommentCollector(maximum_items_to_collect)

    async with aiohttp.ClientSession() as session:
        url_response = await fetch_with_proxy(session, f'{MANAGER_IP}/get_urls?batch_size={batch_size}')
        if not url_response or 'urls' not in url_response:
            logging.error("Failed to get subreddit URLs from proxy")
            return

        subreddit_urls = url_response['urls']

        tasks = []
        for subreddit_url in subreddit_urls:
            if not subreddit_url.endswith('.json'):
                subreddit_url = subreddit_url.rstrip('/') + '/.json'
            tasks.append(fetch_posts(session, subreddit_url, collector, max_oldness_seconds, min_post_length))

        task_group = asyncio.gather(*tasks, return_exceptions=False)

        try:
            await task_group

            for index, item in enumerate(collector.items, start=1):
                created_at_timestamp = datetime.strptime(item.created_at, '%Y-%m-%dT%H:%M:%SZ').timestamp()
                age_string = get_age_string(created_at_timestamp)
                logging.info(f"Found comment {index} and it's {age_string}: {item}")
                yield item
        except (GeneratorExit, asyncio.CancelledError):
            logging.info("[Reddit] GeneratorExit/CancelledError caught, stopping the generator.")
            task_group.cancel()  # Cancel all ongoing tasks
            await asyncio.gather(task_group, return_exceptions=True)  # Ensure all tasks are properly cancelled
            raise  # Re-raise the exception to properly close the generator
        except Exception as e:
            logging.error(f"An error occurred: {e}")

# Example usage:
# parameters = {
#     'max_oldness_seconds': 86400,
#     'maximum_items_to_collect': 10,
#     'min_post_length': 10,
#     'batch_size': 20
# }
# asyncio.run(query(parameters))
