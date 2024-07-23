import aiohttp
import asyncio
import hashlib
import logging
import random
from datetime import datetime, timezone
from typing import AsyncGenerator
from exorde_data import (
    Item,
    Content,
    Author,
    CreatedAt,
    Title,
    Url,
    Domain,
    ExternalId,
    ExternalParentId,
)

logging.basicConfig(level=logging.INFO)

MANAGER_IP = "http://192.227.159.3:8000"
USER_AGENT = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36'

async def fetch_with_proxy(session, url):
    headers = {'User-Agent': USER_AGENT}
    async with session.get(f'{MANAGER_IP}/proxy?url={url}', headers=headers) as response:
        response.raise_for_status()
        return await response.json()

def format_timestamp(timestamp):
    return datetime.fromtimestamp(timestamp, timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')

def is_within_timeframe_seconds(created_utc, max_oldness_seconds):
    current_time = datetime.now(timezone.utc).timestamp()
    return (current_time - created_utc) <= max_oldness_seconds

async def query(parameters: dict) -> AsyncGenerator[Item, None]:
    max_oldness_seconds = parameters.get('max_oldness_seconds')
    maximum_items_to_collect = parameters.get('maximum_items_to_collect')
    min_post_length = parameters.get('min_post_length')

    async with aiohttp.ClientSession() as session:
        url_response = await fetch_with_proxy(session, f'{MANAGER_IP}/get_url')
        subreddit_url = url_response['url']

        response_json = await fetch_with_proxy(session, subreddit_url)

        if not response_json:
            return

        post = response_json[0]['data']['children'][0]['data']
        post_title = post['title']
        comments = response_json[1]['data']['children']
        items_collected = 0

        for comment in comments:
            if items_collected >= maximum_items_to_collect:
                break
            if comment['kind'] == 't1':
                comment_data = comment['data']
                comment_content = comment_data.get('body', '[deleted]')
                comment_author = comment_data.get('author', '[unknown]')
                comment_created_at = comment_data['created_utc']
                comment_url = f"https://reddit.com{comment_data['permalink']}"
                comment_external_id = comment_data['id']
                comment_external_parent_id = comment_data.get('parent_id')

                if (len(comment_content) >= min_post_length and
                    is_within_timeframe_seconds(comment_created_at, max_oldness_seconds)):

                    item = Item(
                        content=Content(comment_content),
                        author=Author(hashlib.sha1(bytes(comment_author, encoding="utf-8")).hexdigest()),
                        created_at=CreatedAt(format_timestamp(comment_created_at)),
                        title=Title(post_title),
                        domain=Domain("reddit.com"),
                        url=Url(comment_url),
                        external_id=ExternalId(comment_external_id),
                        external_parent_id=ExternalParentId(comment_external_parent_id)
                    )

                    yield item
                    items_collected += 1