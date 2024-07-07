import os
import json
from azure.cosmos import CosmosClient
from azure.cosmos.exceptions import CosmosHttpResponseError
from dotenv import load_dotenv
import logging

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
    if os.path.exists(filename):
        with open(filename, 'r') as file:
            tweets_data = json.load(file)
        logging.debug(f"Loaded {len(tweets_data)} tweets from file")
    else:
        logging.error(f"No file named {filename} found.")
        return

    # Insert data into Cosmos DB
    inserted_count = 0
    updated_count = 0
    error_count = 0

    for tweet in tweets_data:
        try:
            logging.debug(f"Processing tweet {tweet['id']}")

            # Check if the tweet already exists
            existing_items = list(container.query_items(
                query="SELECT * FROM c WHERE c.id = @id",
                parameters=[{"name": "@id", "value": tweet['id']}],
                enable_cross_partition_query=True
            ))

            if existing_items:
                # Update existing item
                container.upsert_item(body=tweet)
                updated_count += 1
                logging.debug(f"Updated tweet {tweet['id']}")
            else:
                # Insert new item
                container.create_item(body=tweet)
                inserted_count += 1
                logging.debug(f"Inserted tweet {tweet['id']}")

        except Exception as e:
            logging.error(f"Error processing tweet {tweet['id']}: {str(e)}")
            error_count += 1

    logging.info(
        f"Insertion complete. Inserted: {inserted_count}, Updated: {updated_count}, Errors: {error_count}")


def fetch_tweets():
    query = "SELECT * FROM c ORDER BY c.created_at DESC"

    items = list(container.query_items(
        query=query,
        enable_cross_partition_query=True
    ))

    logging.debug(f"Fetched items: {json.dumps(items, indent=2)}")

    tweets = []
    for item in items:
        referenced_tweets_data = []
        if item.get('referenced_tweets'):
            for ref in item['referenced_tweets']:
                referenced_tweet = next(
                    (rt for rt in items if rt['id'] == ref['id']), None)
                if referenced_tweet:
                    referenced_tweets_data.append({
                        'author': referenced_tweet.get('author_id', 'Unknown'),
                        'text': referenced_tweet.get('text', 'No text available'),
                        'image_description': referenced_tweet.get('image_descriptions', ['No description'])[0]
                    })

        tweet = {
            'id': item['id'],
            'text': item['text'],
            'created_at': item['created_at'],
            'sentiment': {
                'label': item.get('sentiment', {}).get('label', 'Unknown'),
                'score': item.get('sentiment', {}).get('score', 'N/A')
            },
            'social_responsibility': {
                'message': item.get('social_responsibility', {}).get('message', 'N/A'),
                'score': item.get('social_responsibility', {}).get('score', 'N/A')
            },
            'referenced_tweet': referenced_tweets_data if referenced_tweets_data else None
        }
        logging.debug(f"Processed tweet: {json.dumps(tweet, indent=2)}")
        tweets.append(tweet)

    return tweets
