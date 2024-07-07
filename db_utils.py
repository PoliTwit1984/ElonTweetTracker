import os
import json
import logging
from azure.cosmos import CosmosClient, exceptions
from dotenv import load_dotenv

logging.basicConfig(level=logging.DEBUG)

# Load environment variables
load_dotenv()

# Cosmos DB configuration from environment variables
endpoint = os.getenv("COSMOS_DB_ENDPOINT")
key = os.getenv("COSMOS_DB_KEY")
database_name = os.getenv("COSMOS_DB_DATABASE_NAME")
container_name = os.getenv("COSMOS_DB_CONTAINER_NAME")

# Check if all necessary environment variables are available
if not all([endpoint, key, database_name, container_name]):
    raise ValueError("One or more environment variables are missing")

# Initialize the Cosmos client
client = CosmosClient(endpoint, key)
database = client.get_database_client(database_name)
container = database.get_container_client(container_name)


def get_latest_tweet():
    query = "SELECT TOP 1 c.id, c.created_at, c.text FROM c ORDER BY c.created_at DESC"

    items = list(container.query_items(
        query=query,
        enable_cross_partition_query=True
    ))

    if items:
        latest_tweet = items[0]
        return latest_tweet['id'], latest_tweet['created_at'], latest_tweet['text']
    else:
        return None, None, None


logging.basicConfig(level=logging.DEBUG)

# Load environment variables
load_dotenv()

# Cosmos DB configuration from environment variables
endpoint = os.getenv("COSMOS_DB_ENDPOINT")
key = os.getenv("COSMOS_DB_KEY")
database_name = os.getenv("COSMOS_DB_DATABASE_NAME")
container_name = os.getenv("COSMOS_DB_CONTAINER_NAME")

# Check if all necessary environment variables are available
if not all([endpoint, key, database_name, container_name]):
    raise ValueError("One or more environment variables are missing")

# Initialize the Cosmos client
client = CosmosClient(endpoint, key)
database = client.get_database_client(database_name)
container = database.get_container_client(container_name)


def get_latest_tweet():
    query = "SELECT TOP 1 c.id, c.created_at, c.text FROM c ORDER BY c.created_at DESC"

    items = list(container.query_items(
        query=query,
        enable_cross_partition_query=True
    ))

    if items:
        latest_tweet = items[0]
        return latest_tweet['id'], latest_tweet['created_at'], latest_tweet['text']
    else:
        return None, None, None


def insert_tweets_into_db(filename='tweets_data.json'):
    logging.debug(f"Starting tweet insertion from file: {filename}")

    # Load JSON data
    if not os.path.exists(filename):
        logging.error(f"No file named {filename} found.")
        return

    with open(filename, 'r') as file:
        tweets_data = json.load(file)
    logging.debug(f"Loaded {len(tweets_data)} tweets from file")

    # Insert data into Cosmos DB
    inserted_count = 0
    skipped_count = 0
    error_count = 0

    for tweet in tweets_data:
        try:
            # Ensure we're using the original tweet ID, not any referenced tweet ID
            original_tweet_id = tweet['id']
            logging.debug(f"Processing original tweet {original_tweet_id}")

            # Query the database for the original tweet
            query = f"SELECT VALUE COUNT(1) FROM c WHERE c.id = '{original_tweet_id}'"
            results = list(container.query_items(
                query, enable_cross_partition_query=True))

            if results[0] == 0:
                # Original tweet doesn't exist, insert it
                container.create_item(body=tweet)
                inserted_count += 1
                logging.debug(
                    f"Inserted new original tweet {original_tweet_id}")
            else:
                # Original tweet already exists, skip it
                skipped_count += 1
                logging.debug(
                    f"Original tweet {original_tweet_id} already exists, skipping")

        except Exception as e:
            logging.error(
                f"Error processing original tweet {tweet.get('id', 'unknown')}: {str(e)}")
            error_count += 1

    logging.info(f"Insertion complete. "
                 f"Inserted: {inserted_count}, "
                 f"Skipped (already exist): {skipped_count}, "
                 f"Errors: {error_count}")

    return inserted_count, skipped_count, error_count
