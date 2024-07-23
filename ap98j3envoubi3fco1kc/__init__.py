# WOkring in prodction
import random
import aiohttp
from aiohttp import ClientSession, TCPConnector
import asyncio
from typing import AsyncGenerator, List
import time
from datetime import datetime as datett
from datetime import timezone
import hashlib
import logging
import re
from lxml.html import fromstring
from exorde_data import (
    Item,
    Content,
    Author,
    CreatedAt,
    Title,
    Url,
    Domain,
)
from wordsegment import load, segment
from tokenizers import Tokenizer, models, pre_tokenizers

# Load word segmentation library
load()

# Load tokenizer
tokenizer = Tokenizer(models.BPE())
tokenizer.pre_tokenizer = pre_tokenizers.Whitespace()

logging.basicConfig(level=logging.INFO)

MANAGER_IP = "http://192.227.159.3:8000"
USER_AGENT_LIST = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36'
]

global MAX_EXPIRATION_SECONDS
global SKIP_POST_PROBABILITY
MAX_EXPIRATION_SECONDS = 80000
SKIP_POST_PROBABILITY = 0.1
BASE_TIMEOUT = 30

DEFAULT_OLDNESS_SECONDS = 36000
DEFAULT_MAXIMUM_ITEMS = 25
DEFAULT_MIN_POST_LENGTH = 5
DEFAULT_NUMBER_SUBREDDIT_ATTEMPTS = 3
DEFAULT_LAYOUT_SCRAPING_WEIGHT = 0.05
DEFAULT_SKIP_PROBA = 0.1

async def collect_from_generator(generator: AsyncGenerator) -> List:
    results = []
    async for item in generator:
        results.append(item)
    return results


async def get_subreddit_url():
    retries = 5
    for attempt in range(retries):
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(f'{MANAGER_IP}/get_url') as response:
                    response.raise_for_status()
                    return await response.json()
        except aiohttp.ClientError as e:
            logging.warning(f"[Retry {attempt + 1}/{retries}] Failed to fetch subreddit URL: {e}")
            await asyncio.sleep(2 ** attempt)
    raise aiohttp.ClientError(f"Failed to connect to {MANAGER_IP} after {retries} attempts")

def read_parameters(parameters):
    if parameters and isinstance(parameters, dict):
        max_oldness_seconds = parameters.get("max_oldness_seconds", DEFAULT_OLDNESS_SECONDS)
        maximum_items_to_collect = parameters.get("maximum_items_to_collect", DEFAULT_MAXIMUM_ITEMS)
        min_post_length = parameters.get("min_post_length", DEFAULT_MIN_POST_LENGTH)
        nb_subreddit_attempts = parameters.get("nb_subreddit_attempts", DEFAULT_NUMBER_SUBREDDIT_ATTEMPTS)
        new_layout_scraping_weight = parameters.get("new_layout_scraping_weight", DEFAULT_LAYOUT_SCRAPING_WEIGHT)
        skip_post_probability = parameters.get("skip_post_probability", DEFAULT_SKIP_PROBA)
    else:
        max_oldness_seconds = DEFAULT_OLDNESS_SECONDS
        maximum_items_to_collect = DEFAULT_MAXIMUM_ITEMS
        min_post_length = DEFAULT_MIN_POST_LENGTH
        nb_subreddit_attempts = DEFAULT_NUMBER_SUBREDDIT_ATTEMPTS
        new_layout_scraping_weight = DEFAULT_LAYOUT_SCRAPING_WEIGHT
        skip_post_probability = DEFAULT_SKIP_PROBA

    return max_oldness_seconds, maximum_items_to_collect, min_post_length, nb_subreddit_attempts, new_layout_scraping_weight, skip_post_probability

def is_within_timeframe_seconds(input_timestamp, timeframe_sec):
    current_timestamp = int(time.time())  # Get the current UNIX timestamp
    return (current_timestamp - int(input_timestamp)) <= timeframe_sec

def format_timestamp(timestamp):
    dt = datett.fromtimestamp(timestamp, timezone.utc)
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")

def extract_subreddit_name(input_string):
    match = re.search(r'r/([^/]+)', input_string)
    return match.group(1) if match else None

def split_strings_subreddit_name(input_string):
    words = []
    start = 0

    for i in range(1, len(input_string)):
        if input_string[i].isupper():
            words.append(input_string[start:i])
            start = i

    words.append(input_string[start:])
    return ' '.join(words)

async def fetch_with_retry(session, url, headers, retries=5, backoff_factor=0.3):
    for attempt in range(retries):
        try:
            async with session.get(f'{MANAGER_IP}/proxy?url={url}', headers=headers, timeout=BASE_TIMEOUT) as response:
                response.raise_for_status()
                return await response.json()
        except aiohttp.ClientError as e:
            logging.warning(f"[Retry {attempt + 1}/{retries}] Request failed: {e}. Retrying...")
        await asyncio.sleep(backoff_factor * (2 ** attempt))
    logging.error(f"[Reddit] Failed to fetch {url} after {retries} attempts")
    return None


async def scrap_post(session: ClientSession, url: str, count: int, limit: int) -> AsyncGenerator[Item, None]:
    if count >= limit:
        return

    def process_post(data):
        nonlocal count
        content = data["data"]
        created_utc = content["created_utc"]

        if not is_within_timeframe_seconds(created_utc, MAX_EXPIRATION_SECONDS):
            logging.info(f"[Reddit] Skipping old post: {url}")
            return None

        item_ = Item(
            content=Content(content.get("selftext", "")),
            author=Author(hashlib.sha1(bytes(content["author"], encoding="utf-8")).hexdigest()),
            created_at=CreatedAt(str(format_timestamp(created_utc))),
            title=Title(content["title"]),
            domain=Domain("reddit.com"),
            url=Url(url),
        )
        if len(tokenizer.encode(item_.content).tokens) > 512:
            logging.info(f"[Reddit] Skipping post with more than 512 tokens")
            return None
        if count < limit:
            count += 1
            return item_
        return None

    def process_comment(data):
        nonlocal count
        content = data["data"]
        created_utc = content["created_utc"]

        if not is_within_timeframe_seconds(created_utc, MAX_EXPIRATION_SECONDS):
            logging.info(f"[Reddit] Skipping old comment")
            return None

        item_ = Item(
            content=Content(content["body"]),
            author=Author(hashlib.sha1(bytes(content["author"], encoding="utf-8")).hexdigest()),
            created_at=CreatedAt(str(format_timestamp(created_utc))),
            domain=Domain("reddit.com"),
            url=Url(url + content["permalink"]),
        )
        if len(tokenizer.encode(item_.content).tokens) > 512:
            logging.info(f"[Reddit] Skipping comment with more than 512 tokens")
            return None
        if count < limit:
            count += 1
            return item_
        return None

    _url = url + ".json"
    logging.info(f"[Reddit] Scraping - getting {_url}")

    try:
        response_json = await fetch_with_retry(session, _url, headers={"User-Agent": random.choice(USER_AGENT_LIST)})
        if not response_json:
            return

        post_data, comments_data = response_json

        # Process the post
        post_item = process_post(post_data["data"]["children"][0])
        if post_item:
            yield post_item

        # Process comments
        for comment in comments_data["data"]["children"]:
            comment_item = process_comment(comment)
            if comment_item:
                yield comment_item

    except aiohttp.ClientError as e:
        logging.error(f"[Reddit] Failed to fetch {_url}: {e}")
    except Exception as e:
        logging.error(f"[Reddit] An error occurred: {e}")






async def fetch_multiple_posts(session: ClientSession, urls: list, limit: int) -> AsyncGenerator[Item, None]:
    tasks = []
    count = 0

    for url in urls:
        if count >= limit:
            break
        tasks.append(collect_from_generator(scrap_post(session, url, count, limit)))

    results = await asyncio.gather(*tasks, return_exceptions=True)
    
    for result in results:
        if isinstance(result, Exception):
            logging.exception(f"[Reddit] An error occurred: {result}")
        else:
            for item in result:
                if count < limit:
                    yield item
                    count += 1



async def scrap_subreddit_parallel(session: ClientSession, subreddit_url: str, count: int, limit: int, batch_size: int) -> AsyncGenerator[Item, None]:
    if count >= limit:
        return

    try:
        async with session.get(f'{MANAGER_IP}/proxy?url={subreddit_url}', headers={"User-Agent": random.choice(USER_AGENT_LIST)}, timeout=BASE_TIMEOUT) as response:
            html_content = await response.text()
            html_tree = fromstring(html_content)
            post_urls = []

            for post in html_tree.xpath("//shreddit-post/@permalink"):
                if count >= limit:
                    break
                url = post
                if url.startswith("/r/"):
                    url = "https://www.reddit.com" + post
                if "https" not in url:
                    url = f"https://reddit.com{url}"
                post_urls.append(url)

                if len(post_urls) >= batch_size:
                    async for item in fetch_multiple_posts(session, post_urls, limit):
                        if count < limit:
                            yield item
                            count += 1
                    post_urls = []

            if post_urls:
                async for item in fetch_multiple_posts(session, post_urls, limit):
                    if count < limit:
                        yield item
                        count += 1

    except GeneratorExit:
        logging.info(f"[Reddit] GeneratorExit caught in scrap_subreddit_parallel()")
        return
    except aiohttp.ClientError as e:
        logging.error(f"[Reddit] Failed to fetch {subreddit_url}: {e}")





async def scrap_subreddit_json(session: ClientSession, subreddit_url: str, count: int, limit: int) -> AsyncGenerator[Item, None]:
    if count >= limit:
        return

    url_to_fetch = subreddit_url.rstrip('/') + "/.json"
    if random.random() < 0.75:
        url_to_fetch = subreddit_url.rstrip('/') + "/new/.json"

    if url_to_fetch.endswith("/new/new/.json"):
        url_to_fetch = url_to_fetch.replace("/new/new/.json", "/new.json")

    logging.info(f"[Reddit] [JSON MODE] opening: {url_to_fetch}")
    await asyncio.sleep(1)

    try:
        response_json = await fetch_with_retry(session, url_to_fetch, headers={"User-Agent": random.choice(USER_AGENT_LIST)})
        if not response_json:
            return

        permalinks = list(find_permalinks(response_json))

        for i in range(0, len(permalinks), limit):
            batch_permalinks = permalinks[i:i + limit]
            full_urls = [f"https://reddit.com{permalink}" for permalink in batch_permalinks]
            tasks = [collect_from_generator(scrap_post(session, url, count, limit)) for url in full_urls]

            results = await asyncio.gather(*tasks, return_exceptions=True)
            for result in results:
                if isinstance(result, Exception):
                    logging.exception(f"[Reddit] An error occurred: {result}")
                else:
                    for item in result:
                        if count < limit:
                            yield item
                            count += 1

    except GeneratorExit:
        logging.info(f"[Reddit] GeneratorExit caught in scrap_subreddit_json()")
        return
    except aiohttp.ClientError as e:
        logging.error(f"[Reddit] Failed to fetch {url_to_fetch}: {e}")






async def scrap_subreddit_new_layout(session: ClientSession, subreddit_url: str, count: int, limit: int) -> AsyncGenerator[Item, None]:
    if count >= limit:
        return
    try:
        async with session.get(f'{MANAGER_IP}/proxy?url={subreddit_url}', headers={"User-Agent": random.choice(USER_AGENT_LIST)}, timeout=BASE_TIMEOUT) as response:
            html_content = await response.text()
            html_tree = fromstring(html_content)
            for post in html_tree.xpath("//shreddit-post/@permalink"):
                if count >= limit:
                    break
                url = post
                if url.startswith("/r/"):
                    url = "https://www.reddit.com" + post
                await asyncio.sleep(1)
                try:
                    if "https" not in url:
                        url = f"https://reddit.com{url}"
                    async for item in scrap_post(session, url, count, limit):
                        if count < limit:
                            yield item
                            count += 1
                except GeneratorExit:
                    logging.info(f"[Reddit] GeneratorExit caught in scrap_subreddit_new_layout() - post: {url}")
                    return
                except Exception as e:
                    logging.exception(f"[Reddit] Error scraping post {url}: {e}")
    except GeneratorExit:
        logging.info(f"[Reddit] GeneratorExit caught in scrap_subreddit_new_layout()")
        return
    except aiohttp.ClientError as e:
        logging.error(f"[Reddit] Failed to fetch {subreddit_url}: {e}")


def find_permalinks(data):
    if isinstance(data, dict):
        if 'permalink' in data:
            yield data['permalink']
        for key, value in data.items():
            yield from find_permalinks(value)
    elif isinstance(data, list):
        for item in data:
            yield from find_permalinks(item)
            
def post_process_item(item):
    try:
        if len(item['content']) > 10:
            subreddit_name = extract_subreddit_name(item["url"])
            if subreddit_name is None:
                return item
            segmented_subreddit_strs = segment(subreddit_name)
            segmented_subreddit_name = " ".join(segmented_subreddit_strs)
            item["content"] = item["content"] + ". - " + segmented_subreddit_name + " ," + subreddit_name
    except Exception as e:
        logging.exception(f"[Reddit post_process_item] Word segmentation failed: {e}, ignoring...")
    try:
        item["url"] = correct_reddit_url(item["url"])
    except:
        logging.warning(f"[Reddit] failed to correct the URL of item {item['url']}")
    return item

def is_valid_item(item, min_post_length):
    return (
        len(item["content"]) >= min_post_length and
        not item["url"].startswith("https://reddit.comhttps:") and
        "reddit.com" in item["url"] and
        item["content"] != "[deleted]"
    )

def correct_reddit_url(url):
    parts = url.split("https://reddit.comhttps://", 1)
    if len(parts) == 2:
        corrected_url = "https://" + parts[1]
        return corrected_url
    # Remove extra "r/" from URLs if present
    corrected_url = re.sub(r'(/r/){2,}', '/r/', url)
    return corrected_url

async def query(parameters: dict) -> AsyncGenerator[Item, None]:
    global MAX_EXPIRATION_SECONDS, SKIP_POST_PROBABILITY
    (
        max_oldness_seconds,
        MAXIMUM_ITEMS_TO_COLLECT,
        min_post_length,
        nb_subreddit_attempts,
        new_layout_scraping_weight,
        SKIP_POST_PROBABILITY
    ) = read_parameters(parameters)
    logging.info(f"[Reddit] Input parameters: {parameters}")
    MAX_EXPIRATION_SECONDS = max_oldness_seconds
    yielded_items = 0
    batch_size = 20  # You can adjust this batch size as needed

    await asyncio.sleep(random.uniform(3, 15))

    connector = TCPConnector(limit_per_host=10)
    async with aiohttp.ClientSession(connector=connector) as session:
        for i in range(nb_subreddit_attempts):
            await asyncio.sleep(random.uniform(1, i))
            url_response = await get_subreddit_url()
            subreddit_url = url_response['url']
            logging.info(f"[Reddit] Attempt {(i+1)}/{nb_subreddit_attempts} Scraping {subreddit_url} with max oldness of {max_oldness_seconds}")
            if "reddit.com" not in subreddit_url:
                raise ValueError(f"Not a Reddit URL {subreddit_url}")
            url_parameters = subreddit_url.split("reddit.com")[1].split("/")[1:]
            if "comments" in url_parameters:
                async for result in scrap_post(session, subreddit_url, yielded_items, MAXIMUM_ITEMS_TO_COLLECT):
                    result = post_process_item(result)
                    if is_valid_item(result, min_post_length):
                        logging.info(f"[Reddit] Found Reddit post: {result}")
                        yield result
                        yielded_items += 1
                    if yielded_items >= MAXIMUM_ITEMS_TO_COLLECT:
                        break
            else:
                selected_function = scrap_subreddit_json
                if random.random() < new_layout_scraping_weight:
                    selected_function = scrap_subreddit_new_layout
                if random.random() < new_layout_scraping_weight:
                    async for result in scrap_subreddit_parallel(session, subreddit_url, yielded_items, MAXIMUM_ITEMS_TO_COLLECT, batch_size):
                        result = post_process_item(result)
                        if is_valid_item(result, min_post_length):
                            logging.info(f"[Reddit] Found Reddit comment: {result}")
                            yield result
                            yielded_items += 1
                        if yielded_items >= MAXIMUM_ITEMS_TO_COLLECT:
                            break
                else:
                    async for result in selected_function(session, subreddit_url, yielded_items, MAXIMUM_ITEMS_TO_COLLECT):
                        result = post_process_item(result)
                        if is_valid_item(result, min_post_length):
                            logging.info(f"[Reddit] Found Reddit comment: {result}")
                            yield result
                            yielded_items += 1
                        if yielded_items >= MAXIMUM_ITEMS_TO_COLLECT:
                            break
