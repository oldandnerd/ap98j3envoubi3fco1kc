import aiohttp
import asyncio
import hashlib
import logging
import random
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
REDDIT_URL_TEMPLATE = "https://www.reddit.com{subreddit_url}.json?limit=100"

async def fetch_with_proxy(session, url, retries=5, backoff_factor=0.3):
    headers = {'User-Agent': USER_AGENT}
    for attempt in range(retries):
        try:
            async with session.get(f'{MANAGER_IP}/proxy?url={url}', headers=headers) as response:
                response.raise_for_status()
                return await response.json()
        except aiohttp.ClientError as e:
            logging.error(f"[Retry {attempt + 1}/{retries}] Error fetching data: {e}, url={url}")
            if attempt < retries - 1:
                await asyncio.sleep(backoff_factor * (2 ** attempt))
    raise aiohttp.ClientError(f"Failed to fetch {url} after {retries} attempts")

def format_timestamp(timestamp):
    return datetime.fromtimestamp(timestamp, timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')

def is_within_timeframe_seconds(created_utc, max_oldness_seconds):
    current_time = datetime.now(timezone.utc).timestamp()
    return (current_time - created_utc) <= max_oldness_seconds

async def fetch_posts(session, subreddit_url, after=None):
    url = REDDIT_URL_TEMPLATE.format(subreddit_url=subreddit_url)
    if after:
        url += f"&after={after}"
    return await fetch_with_proxy(session, url)

async def query(parameters: Dict) -> AsyncGenerator[Item, None]:
    max_oldness_seconds = parameters.get('max_oldness_seconds')
    maximum_items_to_collect = parameters.get('maximum_items_to_collect')
    min_post_length = parameters.get('min_post_length')

    async with aiohttp.ClientSession() as session:
        url_response = await fetch_with_proxy(session, f'{MANAGER_IP}/get_url')
        subreddit_url = url_response['url']

        # Ensure the URL ends with /
        subreddit_url = subreddit_url.rstrip('/') + '/'

        logging.info(f"Fetched URL from proxy: {subreddit_url}")

        items_collected = 0
        after = None
        total_fetched = 0

        while items_collected < maximum_items_to_collect and total_fetched < 1000:
            try:
                response_json = await fetch_posts(session, subreddit_url, after)
            except aiohttp.ClientError as e:
                logging.error(f"Failed to fetch posts: {e}")
                break

            if not response_json:
                logging.error("Response JSON is empty or invalid")
                return

            if isinstance(response_json, dict):
                post_data = response_json.get('data', {}).get('children', [])
                after = response_json.get('data', {}).get('after', None)
            elif isinstance(response_json, list):
                post_data = response_json[0].get('data', {}).get('children', [])
                after = response_json[0].get('data', {}).get('after', None)

            if not post_data:
                logging.info("No more posts to fetch.")
                break

            for post in post_data:
                if items_collected >= maximum_items_to_collect:
                    break
                post_kind = post.get('kind')
                post_info = post.get('data', {})

                if post_kind == 't3':  # Ensuring it's a post
                    post_title = post_info.get('title', '[no title]')
                    post_created_at = post_info.get('created_utc', 0)
                    post_url = f"https://reddit.com{post_info.get('permalink', '')}"

                    if not is_within_timeframe_seconds(post_created_at, max_oldness_seconds):
                        continue

                    # Process the post itself
                    post_content = post_info.get('selftext', '[no content]')
                    if len(post_content) >= min_post_length:
                        item = Item(
                            content=Content(post_content),
                            author=Author(hashlib.sha1(bytes(post_info.get('author', '[unknown]'), encoding="utf-8")).hexdigest()),
                            created_at=CreatedAt(format_timestamp(post_created_at)),
                            title=Title(post_title),
                            domain=Domain("reddit.com"),
                            url=Url(post_url),
                        )

                        print(item)  # Print the item
                        yield item
                        items_collected += 1
                        if items_collected >= maximum_items_to_collect:
                            break

                    # Process the comments
                    if 'replies' in post_info and isinstance(post_info['replies'], dict):
                        comments = post_info['replies'].get('data', {}).get('children', [])
                        for comment in comments:
                            if items_collected >= maximum_items_to_collect:
                                break
                            if comment['kind'] == 't1':
                                comment_data = comment['data']
                                comment_content = comment_data.get('body', '[deleted]')
                                comment_author = comment_data.get('author', '[unknown]')
                                comment_created_at = comment_data.get('created_utc', 0)
                                comment_url = f"https://reddit.com{comment_data.get('permalink', '')}"

                                if (len(comment_content) >= min_post_length and
                                    is_within_timeframe_seconds(comment_created_at, max_oldness_seconds)):

                                    item = Item(
                                        content=Content(comment_content),
                                        author=Author(hashlib.sha1(bytes(comment_author, encoding="utf-8")).hexdigest()),
                                        created_at=CreatedAt(format_timestamp(comment_created_at)),
                                        title=Title(post_title),
                                        domain=Domain("reddit.com"),
                                        url=Url(comment_url),
                                    )

                                    print(item)  # Print the item
                                    yield item
                                    items_collected += 1

            total_fetched += len(post_data)
            if not after:
                logging.info("Reached the end of posts or no after cursor found.")
                break

# Example usage:
# parameters = {
#     'max_oldness_seconds': 86400,
#     'maximum_items_to_collect': 10,
#     'min_post_length': 10
# }
# asyncio.run(query(parameters))
