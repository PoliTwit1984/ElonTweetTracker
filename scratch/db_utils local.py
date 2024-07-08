import os
import json
import logging
import time
from azure.cosmos import CosmosClient, exceptions
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv

load_dotenv()

# Cosmos DB configuration
endpoint = os.getenv("COSMOS_DB_ENDPOINT")
key = os.getenv("COSMOS_DB_KEY")
database_name = os.getenv("COSMOS_DB_DATABASE_NAME")
container_name = os.getenv("COSMOS_DB_CONTAINER_NAME")

# Azure Blob Storage configuration
AZURE_STORAGE_CONNECTION_STRING = os.getenv("AZURE_STORAGE_CONNECTION_STRING")

# Initialize the Cosmos client
client = CosmosClient(endpoint, key)
database = client.get_database_client(database_name)
container = database.get_container_client(container_name)


def get_blob_service_client():
    return BlobServiceClient.from_connection_string(AZURE_STORAGE_CONNECTION_STRING)


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


def insert_tweets_into_db(blob_name='tweets_data.json', blob_container='tweetdata'):
    logging.info(f"Starting tweet insertion from blob: {blob_name}")

    # Get blob data
    blob_service_client = get_blob_service_client()
    blob_client = blob_service_client.get_blob_client(
        container=blob_container, blob=blob_name)

    try:
        blob_data = blob_client.download_blob()
        tweets_data = json.loads(blob_data.readall())
        logging.info(f"Loaded {len(tweets_data)} tweets from blob")
    except Exception as e:
        logging.error(f"Error loading data from blob: {str(e)}")
        return

    inserted_count = 0
    skipped_count = 0
    error_count = 0

    # Process tweets in batches
    batch_size = 100
    for i in range(0, len(tweets_data), batch_size):
        batch = tweets_data[i:i+batch_size]
        tweet_ids = [tweet['id'] for tweet in batch]

        try:
            # Check for existing tweets in batch
            query = "SELECT c.id FROM c WHERE c.id IN (" + ",".join(
                f"'{id}'" for id in tweet_ids) + ")"
            existing_ids = set([item['id'] for item in container.query_items(
                query, enable_cross_partition_query=True)])

            for tweet in batch:
                tweet_id = tweet['id']
                if tweet_id in existing_ids:
                    skipped_count += 1
                    logging.debug(f"Tweet {tweet_id} already exists, skipping")
                else:
                    try:
                        container.create_item(body=tweet)
                        inserted_count += 1
                        logging.debug(f"Inserted new tweet {tweet_id}")
                    except exceptions.CosmosHttpResponseError as e:
                        if e.status_code == 409:  # Conflict, tweet was inserted by another process
                            skipped_count += 1
                            logging.debug(
                                f"Tweet {tweet_id} was inserted by another process, skipping")
                        else:
                            raise

        except exceptions.CosmosHttpResponseError as e:
            if e.status_code == 429:  # Too Many Requests
                retry_after = int(e.headers.get(
                    'x-ms-retry-after-ms', 1000)) / 1000.0
                logging.warning(
                    f"Rate limited. Waiting for {retry_after} seconds before retrying.")
                time.sleep(retry_after)
                # Decrement i to retry this batch
                i -= batch_size
            else:
                logging.error(f"Error processing batch: {str(e)}")
                error_count += len(batch)

        except Exception as e:
            logging.error(f"Unexpected error processing batch: {str(e)}")
            error_count += len(batch)

        # Log progress
        if (i + batch_size) % 1000 == 0 or (i + batch_size) >= len(tweets_data):
            logging.info(f"Processed {i + batch_size}/{len(tweets_data)} tweets. "
                         f"Inserted: {inserted_count}, Skipped: {skipped_count}, Errors: {error_count}")

    logging.info(f"Insertion complete. "
                 f"Inserted: {inserted_count}, "
                 f"Skipped (already exist): {skipped_count}, "
                 f"Errors: {error_count}")

    return inserted_count, skipped_count, error_count
