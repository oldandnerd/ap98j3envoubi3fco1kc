import aiohttp
import asyncio
import hashlib
import logging
from datetime import datetime, timezone
from typing import AsyncGenerator, Dict, List
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

async def fetch_comments(session, post_permalink):
    comments_url = f"https://www.reddit.com{post_permalink}.json"
    comments_json = await fetch_with_proxy(session, comments_url)
    return comments_json

async def fetch_posts(session, subreddit_url, max_oldness_seconds, min_post_length, maximum_items_to_collect):
    response_json = await fetch_with_proxy(session, subreddit_url)
    items_collected = 0
    comments_skipped = 0

    if not response_json:
        logging.error("Response JSON is empty or invalid")
        return items_collected, comments_skipped, []

    # Check the structure of the response JSON
    if 'data' not in response_json or 'children' not in response_json['data']:
        logging.error("Unexpected JSON structure")
        return items_collected, comments_skipped, []

    posts = response_json['data']['children']
    items = []

    for post in posts:
        if items_collected >= maximum_items_to_collect:
            break

        post_kind = post.get('kind')
        post_info = post.get('data', {})

        if post_kind == 't3':
            post_title = post_info.get('title', '[no title]')
            post_permalink = post_info.get('permalink')

            if post_permalink:
                comments_json = await fetch_comments(session, post_permalink)
                if comments_json and len(comments_json) > 1:
                    comments = comments_json[1]['data']['children']

                    for comment in comments:
                        if items_collected >= maximum_items_to_collect:
                            break
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
                                    title=Title(post_title),
                                    domain=Domain("reddit.com"),
                                    url=Url(comment_url),
                                )

                                logging.info("Comment found: %s", item)
                                items.append(item)
                                items_collected += 1
                            else:
                                comments_skipped += 1

    logging.info(f"Total comments skipped due to oldness: {comments_skipped}")
    return items_collected, comments_skipped, items

async def query(parameters: Dict) -> AsyncGenerator[Item, None]:
    max_oldness_seconds = parameters.get('max_oldness_seconds')
    maximum_items_to_collect = parameters.get('maximum_items_to_collect', 1000)
    min_post_length = parameters.get('min_post_length')
    batch_size = parameters.get('batch_size', 20)

    async with aiohttp.ClientSession() as session:
        url_response = await fetch_with_proxy(session, f'{MANAGER_IP}/get_urls?batch_size={batch_size}')
        if not url_response or 'urls' not in url_response:
            logging.error("Failed to get subreddit URLs from proxy")
            return

        subreddit_urls = url_response['urls']
        total_items_collected = 0

        tasks = []
        for subreddit_url in subreddit_urls:
            if not subreddit_url.endswith('.json'):
                subreddit_url = subreddit_url.rstrip('/') + '/.json'
            tasks.append(fetch_posts(session, subreddit_url, max_oldness_seconds, min_post_length, maximum_items_to_collect))

        results = await asyncio.gather(*tasks)

        for items_collected, comments_skipped, items in results:
            total_items_collected += items_collected
            for item in items:
                yield item

        logging.info(f"Total items collected across all subreddits: {total_items_collected}")

# Example usage:
# parameters = {
#     'max_oldness_seconds': 86400,
#     'maximum_items_to_collect': 10,
#     'min_post_length': 10,
#     'batch_size': 20
# }
# asyncio.run(query(parameters))
