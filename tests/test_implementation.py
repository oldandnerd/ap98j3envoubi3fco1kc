from ap98j3envoubi3fco1kc.v_1 import query
from exorde_data import Item
import pytest


@pytest.mark.asyncio
async def test_query():
    item_count = 0
    async for result in query(
        {
            "keyword": "Macron",
            "max_oldness_seconds":1000000,
            "nb_subreddit_attempts": 3,
            "maximum_items_to_collect": 25,
            "skip_post_probability" : 0.1,
            "new_layout_scraping_weight": 0.1,
            "url_parameters": {
                "keyword": "crypto",
                "autonomous_subreddit_choice": 0,
            }
        }
    ):
        assert isinstance(result, Item)
        item_count += 1
    print("FOUND ",item_count," items")
