import os
from azure.cosmos import CosmosClient, exceptions
from dotenv import load_dotenv
import logging
from collections import defaultdict

logging.basicConfig(level=logging.INFO)

# Load environment variables
load_dotenv()

# Cosmos DB configuration
endpoint = os.getenv("COSMOS_DB_ENDPOINT")
key = os.getenv("COSMOS_DB_KEY")
database_name = os.getenv("COSMOS_DB_DATABASE_NAME")
container_name = os.getenv("COSMOS_DB_CONTAINER_NAME")

# Initialize the Cosmos client
client = CosmosClient(endpoint, key)
database = client.get_database_client(database_name)
container = database.get_container_client(container_name)


def remove_duplicate_tweets():
    # Query to get all tweets
    query = "SELECT c.id, c._ts FROM c"

    all_tweets = list(container.query_items(
        query=query,
        enable_cross_partition_query=True
    ))

    logging.info(f"Total tweets fetched: {len(all_tweets)}")

    # Group tweets by id
    tweet_groups = defaultdict(list)
    for tweet in all_tweets:
        tweet_groups[tweet['id']].append(tweet)

    logging.info(f"Number of unique tweet IDs: {len(tweet_groups)}")

    total_removed = 0
    duplicates_found = 0

    for tweet_id, tweets in tweet_groups.items():
        if len(tweets) > 1:
            duplicates_found += 1
            logging.info(
                f"Found {len(tweets)} duplicates for tweet ID: {tweet_id}")

            # Sort items by _ts (timestamp) in descending order to keep the most recent
            tweets.sort(key=lambda x: x['_ts'], reverse=True)

            # Keep the first (most recent) item and delete the rest
            for tweet in tweets[1:]:
                try:
                    container.delete_item(
                        item=tweet['id'], partition_key=tweet['id'])
                    total_removed += 1
                    logging.info(
                        f"Deleted duplicate item with ID: {tweet['id']}")
                except exceptions.CosmosHttpResponseError as e:
                    logging.error(
                        f"Failed to delete item with ID {tweet['id']}: {str(e)}")

    logging.info(f"Total duplicate groups found: {duplicates_found}")
    logging.info(f"Total duplicate items removed: {total_removed}")


if __name__ == "__main__":
    remove_duplicate_tweets()
