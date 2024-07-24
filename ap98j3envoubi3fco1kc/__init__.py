import aiohttp
import asyncio
import hashlib
import logging
from datetime import datetime, timezone
from typing import AsyncGenerator, Dict, Union
from exorde_data import Item, Content, Author, CreatedAt, Title, Url, Domain

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

def format_timestamp(timestamp):
    return datetime.fromtimestamp(timestamp, timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')

def is_within_timeframe_seconds(created_utc, max_oldness_seconds, current_time):
    buffer_seconds = max_oldness_seconds * 0.80  # Apply 20% buffer
    return (current_time - created_utc) <= buffer_seconds

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

async def fetch_comments_from_json(comments_json, post_permalink, collector, max_oldness_seconds, min_post_length, current_time):
    try:
        comments = comments_json['data']['children']
        for comment in comments:
            if comment['kind'] == 't1':
                comment_data = comment['data']
                comment_created_at = comment_data['created_utc']

                if not is_within_timeframe_seconds(comment_created_at, max_oldness_seconds, current_time):
                    continue  # Skip old comments

                comment_content = comment_data.get('body', '[deleted]')
                comment_author = comment_data.get('author', '[unknown]')
                comment_url = f"https://reddit.com{comment_data['permalink']}"

                if len(comment_content) >= min_post_length:
                    item = Item(
                        content=Content(comment_content),
                        author=Author(hashlib.sha1(bytes(comment_author, encoding="utf-8")).hexdigest()),
                        created_at=CreatedAt(format_timestamp(comment_created_at)),
                        title=Title(post_permalink),
                        domain=Domain("reddit.com"),
                        url=Url(comment_url),
                    )

                    try:
                        if not await collector.add_item(item):
                            return
                    except Exception as e:
                        logging.error(f"Error adding item: {e}")
    except Exception as e:
        logging.error(f"Error processing comments JSON: {e}")

async def process_json_data(json_data, collector, max_oldness_seconds, min_post_length, current_time):
    if isinstance(json_data, list):
        for json_object in json_data:
            await process_single_json_object(json_object, collector, max_oldness_seconds, min_post_length, current_time)
    else:
        await process_single_json_object(json_data, collector, max_oldness_seconds, min_post_length, current_time)

async def process_single_json_object(json_object, collector, max_oldness_seconds, min_post_length, current_time):
    try:
        posts = json_object['data']['children']

        for post in posts:
            if collector.should_stop_fetching():
                return

            post_kind = post.get('kind')
            post_info = post.get('data', {})

            if post_kind == 't3':
                post_permalink = post_info.get('permalink')
                post_created_at = post_info.get('created_utc', 0)

                if not is_within_timeframe_seconds(post_created_at, max_oldness_seconds, current_time):
                    logging.info(f"Skipping old post: {post_permalink}")
                    continue  # Log old post but continue to fetch comments

                # Assuming comments JSON is available within the post data
                if 'comments' in post_info:
                    await fetch_comments_from_json(post_info['comments'], post_permalink, collector, max_oldness_seconds, min_post_length, current_time)
                if collector.should_stop_fetching():
                    return
    except Exception as e:
        logging.error(f"Error processing JSON data: {e}")

async def query(json_data: Union[Dict, list], parameters: Dict) -> AsyncGenerator[Item, None]:
    max_oldness_seconds = parameters.get('max_oldness_seconds')
    maximum_items_to_collect = parameters.get('maximum_items_to_collect', 1000)
    min_post_length = parameters.get('min_post_length')

    collector = CommentCollector(maximum_items_to_collect)
    current_time = datetime.now(timezone.utc).timestamp()

    try:
        await process_json_data(json_data, collector, max_oldness_seconds, min_post_length, current_time)

        for index, item in enumerate(collector.items, start=1):
            created_at_timestamp = datetime.strptime(item.created_at, '%Y-%m-%dT%H:%M:%SZ').timestamp()
            age_string = get_age_string(created_at_timestamp, current_time)
            logging.info(f"Found comment {index} and it's {age_string}: {item}")
            yield item
    except GeneratorExit:
        logging.info("[Reddit] GeneratorExit caught, stopping the generator.")
        raise  # Re-raise the exception to properly close the generator
    except asyncio.CancelledError:
        logging.info("[Reddit] CancelledError caught, stopping the generator.")
        raise  # Re-raise the exception to properly close the generator
    except Exception as e:
        logging.error(f"An error occurred: {e}")

# Example usage:
# json_data = [
#     {
#         "kind": "Listing",
#         "data": {
#             "children": [
#                 {
#                     "kind": "t3",
#                     "data": {
#                         "permalink": "/r/NoStupidQuestions/comments/1ea4ai0/why_do_guys_like_sucking_on_womens_boobs/",
#                         "created_utc": 1721729900.0,
#                         "comments": {
#                             "data": {
#                                 "children": [
#                                     {
#                                         "kind": "t1",
#                                         "data": {
#                                             "created_utc": 1721730000.0,
#                                             "body": "Because it's enjoyable for both parties.",
#                                             "author": "user1",
#                                             "permalink": "/r/NoStupidQuestions/comments/1ea4ai0/why_do_guys_like_sucking_on_womens_boobs/comment1/"
#                                         }
#                                     }
#                                 ]
#                             }
#                         }
#                     }
#                 }
#             ]
#         }
#     }
# ]
# parameters = {
#     'max_oldness_seconds': 86400,
#     'maximum_items_to_collect': 10,
#     'min_post_length': 10,
# }
# asyncio.run(query(json_data, parameters))
