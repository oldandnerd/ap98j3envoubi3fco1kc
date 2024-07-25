import aiohttp
import asyncio
import hashlib
import logging
import re
from datetime import datetime, timezone
from typing import AsyncGenerator, Dict
from wordsegment import load, segment
from exorde_data import Item, Content, Title, Author, CreatedAt, Url, Domain
from aiohttp import ClientConnectorError

logging.basicConfig(level=logging.INFO)

MANAGER_IP = "http://192.227.159.3:8000"
USER_AGENT = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36'
MAX_CONCURRENT_TASKS = 200
MAX_RETRIES_PROXY = 3  # Maximum number of retries for 503 errors

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

async def fetch_with_proxy(session, url, collector, retries=5) -> AsyncGenerator[Dict, None]:
    headers = {'User-Agent': USER_AGENT}
    for attempt in range(retries):
        if collector.should_stop_fetching():
            logging.info("Stopping fetch_with_proxy retries as maximum items have been collected.")
            break
        try:
            async with session.get(f'{MANAGER_IP}/proxy?url={url}', headers=headers) as response:
                response.raise_for_status()
                yield await response.json()
                return
        except aiohttp.ClientConnectorError as e:
            logging.error(f"Connection error fetching URL {url}: {e}")
        except aiohttp.ClientResponseError as e:
            logging.error(f"Response error fetching URL {url}: {e}")
        except aiohttp.ClientConnectionError as e:
            logging.error(f"Connection error fetching URL {url}: {e}")
        except Exception as e:
            logging.error(f"Unexpected error fetching URL {url}: {e}")

        wait_time = 2 ** attempt
        logging.info(f"Retrying in {wait_time} seconds...")
        await asyncio.sleep(wait_time)

    logging.error(f"Maximum retries reached for URL {url}. Skipping.")

def format_timestamp(timestamp):
    return datetime.fromtimestamp(timestamp, timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')

def is_within_timeframe_seconds(created_utc, max_oldness_seconds, current_time):
    return (current_time - created_utc) <= max_oldness_seconds

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
        if item.content:
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

async def fetch_comments(session, post_permalink, collector, max_oldness_seconds, min_post_length, current_time) -> AsyncGenerator[Item, None]:
    try:
        comments_url = f"https://www.reddit.com{post_permalink}.json"
        async for comments_json in fetch_with_proxy(session, comments_url, collector):
            if not comments_json or len(comments_json) <= 1:
                logging.info("No comments found or invalid response in fetch_comments")
                return

            comments = comments_json[1]['data']['children']
            for comment in comments:
                if collector.should_stop_fetching():
                    logging.info("Stopping fetch_comments due to max items collected")
                    return

                if comment['kind'] != 't1':
                    logging.info(f"Skipping non-comment item: {comment['kind']}")
                    continue

                comment_data = comment['data']
                comment_created_at = comment_data['created_utc']

                # Skip comments by AutoModerator
                if comment_data.get('author') == 'AutoModerator':
                    logging.info(f"Skipping AutoModerator comment: {comment_data['id']}")
                    continue

                if not is_within_timeframe_seconds(comment_created_at, max_oldness_seconds, current_time):
                    continue

                comment_content = comment_data.get('body', '[deleted]')
                comment_author = comment_data.get('author', '[unknown]')
                comment_url = f"https://reddit.com{comment_data['permalink']}"
                comment_id = comment_data['name']  # Extracting the comment ID

                # Check if the content is valid (not empty and not just a URL)
                if len(comment_content.strip()) < min_post_length or comment_content.strip().startswith('http'):
                    logging.info(f"Skipping invalid comment: {comment_data['id']} with content '{comment_content}'")
                    continue

                item = Item(
                    content=Content(comment_content),
                    author=Author(hashlib.sha1(bytes(comment_author, encoding="utf-8")).hexdigest()),
                    created_at=CreatedAt(format_timestamp(comment_created_at)),
                    domain=Domain("reddit.com"),
                    url=Url(comment_url),
                )

                if await collector.add_item(item, comment_id):  # Using comment_id to avoid duplicates
                    logging.info(f"New valid comment found: {item}")
                    yield item
    except GeneratorExit:
        logging.info("GeneratorExit received in fetch_comments, exiting gracefully.")
        raise
    except Exception as e:
        logging.error(f"Error in fetch_comments: {e}")


async def fetch_posts(session, subreddit_url, collector, max_oldness_seconds, min_post_length, current_time, limit=100, after=None) -> AsyncGenerator[Item, None]:
    try:
        # Construct the URL with limit and after parameters
        params = {
            'limit': limit,
            'raw_json': 1
        }
        if after:
            params['after'] = after

        async for response_json in fetch_with_proxy(session, subreddit_url, collector, params):
            if not response_json or 'data' not in response_json or 'children' not in response_json['data']:
                logging.info("No posts found or invalid response in fetch_posts")
                return

            posts = response_json['data']['children']
            for post in posts:
                if collector.should_stop_fetching():
                    logging.info("Stopping fetch_posts due to max items collected")
                    return

                post_kind = post.get('kind')
                post_info = post.get('data', {})

                if post_kind != 't3':
                    logging.info(f"Skipping non-post item: {post_kind}")
                    continue

                post_permalink = post_info.get('permalink', None)
                post_created_at = post_info.get('created_utc', 0)

                if is_within_timeframe_seconds(post_created_at, max_oldness_seconds, current_time):
                    post_content = post_info.get('selftext', '[deleted]')
                    post_author = post_info.get('author', '[unknown]')
                    post_url = f"https://reddit.com{post_permalink}"
                    post_id = post_info['name']  # Extracting the post ID

                    if len(post_content.strip()) < min_post_length or post_content.strip().startswith('http'):
                        logging.info(f"Skipping invalid post: {post_id} with content '{post_content}'")
                        continue

                    item = Item(
                        title=Title(post_info.get('title', '[deleted]')),
                        content=Content(post_content),  # Ensure content is always added
                        author=Author(hashlib.sha1(bytes(post_author, encoding="utf-8")).hexdigest()),
                        created_at=CreatedAt(format_timestamp(post_created_at)),
                        domain=Domain("reddit.com"),
                        url=Url(post_url),
                    )

                    if await collector.add_item(item, post_id):
                        logging.info(f"New valid post found: {item}")
                        yield item

                if post_permalink:
                    async for comment in fetch_comments(session, post_permalink, collector, max_oldness_seconds, min_post_length, current_time):
                        try:
                            if len(comment.content) >= min_post_length:
                                yield comment
                            else:
                                logging.info(f"Skipping short comment in post comments: {comment.url}")
                        except GeneratorExit:
                            logging.info("GeneratorExit received in fetch_comments within fetch_posts, exiting gracefully.")
                            raise

            # Return the value of 'after' to be used in the next request
            return response_json['data']['after']
    except GeneratorExit:
        logging.info("GeneratorExit received in fetch_posts, exiting gracefully.")
        raise
    except Exception as e:
        logging.error(f"Error in fetch_posts: {e}")
        return None




def is_valid_item(item, min_post_length):
    if len(item.content) < min_post_length \
    or item.url.startswith("https://reddit.comhttps:")  \
    or not ("reddit.com" in item.url) \
    or item.content == "[deleted]":
        return False
    else:
        return True

async def limited_fetch(semaphore, session, subreddit_url, collector, max_oldness_seconds, min_post_length, current_time, nb_subreddit_attempts, post_limit) -> AsyncGenerator[Item, None]:
    failed_subreddits = []
    async with semaphore:
        for attempt in range(1):
            try:
                async for item in fetch_posts(session, subreddit_url, collector, max_oldness_seconds, min_post_length, current_time, post_limit):
                    try:
                        yield item
                    except GeneratorExit:
                        logging.info("GeneratorExit received in fetch_posts within limited_fetch, exiting gracefully.")
                        raise
            except GeneratorExit:
                logging.info("GeneratorExit received inside attempt loop in limited_fetch, exiting gracefully.")
                raise
            except Exception as e:
                logging.error(f"Error inside attempt loop in limited_fetch: {e}")
                failed_subreddits.append(subreddit_url)

        for attempt in range(nb_subreddit_attempts - 1):
            if not failed_subreddits:
                break
            logging.info(f"Retrying failed subreddits, attempt {attempt + 2}")
            current_failed = []
            for subreddit_url in failed_subreddits:
                try:
                    async for item in fetch_posts(session, subreddit_url, collector, max_oldness_seconds, min_post_length, current_time, post_limit):
                        try:
                            yield item
                        except GeneratorExit:
                            logging.info("GeneratorExit received in fetch_posts within limited_fetch retries, exiting gracefully.")
                            raise
                except GeneratorExit:
                    logging.info("GeneratorExit received inside retry loop in limited_fetch, exiting gracefully.")
                    raise
                except Exception as e:
                    logging.error(f"Error inside retry loop in limited_fetch: {e}")
                    current_failed.append(subreddit_url)
            failed_subreddits = current_failed



async def query(parameters: Dict) -> AsyncGenerator[Item, None]:
    max_oldness_seconds = parameters.get('max_oldness_seconds')
    maximum_items_to_collect = parameters.get('maximum_items_to_collect', 25)  # Default to 25 if not provided
    min_post_length = parameters.get('min_post_length')
    batch_size = parameters.get('batch_size', 100)
    nb_subreddit_attempts = parameters.get('nb_subreddit_attempts', 100)
    post_limit = parameters.get('post_limit', 100)  # Limit for the number of posts per subreddit

    # Enforce a maximum limit of 100
    maximum_items_to_collect = min(maximum_items_to_collect, 100)

    logging.info(f"[Reddit] Input parameters: max_oldness_seconds={max_oldness_seconds}, "
                 f"maximum_items_to_collect={maximum_items_to_collect}, min_post_length={min_post_length}, "
                 f"batch_size={batch_size}, nb_subreddit_attempts={nb_subreddit_attempts}, post_limit={post_limit}")

    collector = CommentCollector(maximum_items_to_collect)
    current_time = datetime.now(timezone.utc).timestamp()

    session = aiohttp.ClientSession(connector=aiohttp.TCPConnector(limit_per_host=MAX_CONCURRENT_TASKS))
    try:
        async for url_response in fetch_with_proxy(session, f'{MANAGER_IP}/get_urls?batch_size={batch_size}', collector):
            if not url_response or 'urls' not in url_response:
                logging.error("Failed to get subreddit URLs from proxy")
                return

            subreddit_urls = url_response['urls']

            semaphore = asyncio.Semaphore(MAX_CONCURRENT_TASKS)
            tasks = [limited_fetch(semaphore, session, subreddit_url, collector, max_oldness_seconds, min_post_length, current_time, nb_subreddit_attempts, post_limit) for subreddit_url in subreddit_urls]

            for task in tasks:
                async for item in task:
                    try:
                        yield item
                    except GeneratorExit:
                        logging.info("GeneratorExit received in limited_fetch within query, exiting gracefully.")
                        raise

            for index, item in enumerate(collector.items, start=1):
                item = post_process_item(item)
                logging.info(f"Found comment {index}: {item}")
                yield item
    except GeneratorExit:
        logging.info("GeneratorExit received in query, exiting gracefully.")
        raise
    except Exception as e:
        logging.error(f"Error in query: {e}")
    finally:
        logging.info("Cleaning up: closing session.")
        await session.close()
        logging.info("Session closed.")
        logging.info("End of iterator - StopAsyncIteration")
