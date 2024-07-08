import logging
import azure.functions as func
import requests
import os
import json
from datetime import datetime, timedelta, timezone
from azure.storage.blob import BlobServiceClient
from utils import analyze_image_with_gpt4o, evaluate_social_responsibility, analyze_tweet_sentiment, advanced_analyze_tweet_content
from db_utils import get_latest_tweet, insert_tweets_into_db

app = func.FunctionApp()

BEARER_TOKEN = os.environ["BEARER_TOKEN"]
AZURE_STORAGE_CONNECTION_STRING = os.environ["AZURE_STORAGE_CONNECTION_STRING"]


@app.schedule(schedule="0 */1 * * * *", arg_name="myTimer", run_on_startup=True, use_monitor=False)
def timer_trigger(myTimer: func.TimerRequest) -> None:
    logging.info('Timer trigger function "timer_trigger" started execution.')
    if myTimer.past_due:
        logging.info('The timer is past due!')

    logging.info(f"BEARER_TOKEN configured: {'BEARER_TOKEN' in os.environ}")
    logging.info(
        f"AZURE_STORAGE_CONNECTION_STRING configured: {'AZURE_STORAGE_CONNECTION_STRING' in os.environ}")

    try:
        main()
    except Exception as e:
        logging.error(f"An error occurred in main execution: {str(e)}")

    logging.info('Timer trigger function "timer_trigger" completed execution.')


def get_blob_service_client():
    return BlobServiceClient.from_connection_string(AZURE_STORAGE_CONNECTION_STRING)


def load_from_blob(container_name='tweetdata', blob_name='tweets_data.json'):
    blob_service_client = get_blob_service_client()
    blob_client = blob_service_client.get_blob_client(
        container=container_name, blob=blob_name)
    try:
        download_stream = blob_client.download_blob()
        return json.loads(download_stream.readall())
    except Exception as e:
        logging.warning(f"Error loading data from blob: {str(e)}")
        return []


def save_to_blob(data, container_name='tweetdata', blob_name='tweets_data.json'):
    logging.info(f"Attempting to save {len(data)} tweets to blob storage")
    blob_service_client = get_blob_service_client()

    container_client = blob_service_client.get_container_client(container_name)
    if not container_client.exists():
        container_client.create_container()

    blob_client = blob_service_client.get_blob_client(
        container=container_name, blob=blob_name)
    try:
        blob_client.upload_blob(json.dumps(data, indent=4), overwrite=True)
        logging.info(f"Data saved to blob storage")
    except Exception as e:
        logging.error(f"Error saving data to blob storage: {str(e)}")


def parse_tweets(tweets_response):
    tweets_data = []

    if 'data' not in tweets_response:
        logging.warning("No new tweets found.")
        return tweets_data

    def get_tweet_url(tweet_id):
        return f"https://x.com/i/web/status/{tweet_id}"

    def process_media(media_keys, includes_media):
        media_info = []
        image_descriptions = []
        image_urls = []
        for media_key in media_keys:
            media = next(
                (m for m in includes_media if m['media_key'] == media_key), None)
            if media:
                if media['type'] == 'photo':
                    image_url = media['url']
                    media_info.append(f"Image URL: {image_url}")
                    image_urls.append(image_url)
                    try:
                        image_description = analyze_image_with_gpt4o(
                            image_url, verbose=True)
                        media_info.append(
                            f"Image Description: {image_description}")
                        image_descriptions.append(image_description)
                    except Exception as e:
                        media_info.append(f"Error analyzing image: {str(e)}")
                elif media['type'] == 'video':
                    preview_url = media.get(
                        'preview_image_url', 'URL not available')
                    media_info.append(f"Video Preview URL: {preview_url}")
                    if preview_url != 'URL not available':
                        image_urls.append(preview_url)
                        try:
                            image_description = analyze_image_with_gpt4o(
                                preview_url, verbose=True)
                            media_info.append(
                                f"Video Preview Description: {image_description}")
                            image_descriptions.append(image_description)
                        except Exception as e:
                            media_info.append(
                                f"Error analyzing video preview: {str(e)}")
        return media_info, image_descriptions, image_urls

    includes_media = tweets_response.get('includes', {}).get('media', [])

    for tweet in tweets_response['data']:
        logging.info(f"Processing tweet {tweet['id']}")
        tweet_data = {
            "id": tweet['id'],
            "text": tweet['text'],
            "created_at": tweet['created_at'],
            "author_id": tweet['author_id'],
            "url": get_tweet_url(tweet['id']),
            "image_descriptions": [],
            "image_urls": [],
            "referenced_tweets": []
        }

        if 'attachments' in tweet and 'media_keys' in tweet['attachments']:
            media_info, image_descriptions, image_urls = process_media(
                tweet['attachments']['media_keys'], includes_media)
            tweet_data["image_descriptions"] = image_descriptions
            tweet_data["image_urls"] = image_urls

        referenced_text = ""
        if 'referenced_tweets' in tweet:
            for ref in tweet['referenced_tweets']:
                ref_data = referenced_tweet_id_lookup(ref['id'])
                tweet_data["referenced_tweets"].append({
                    "type": ref['type'],
                    "id": ref['id'],
                    "text": ref_data["text"],
                    "image_description": ref_data["image_description"],
                    "image_url": ref_data["image_url"]
                })
                referenced_text += ref_data["text"]

        keywords, hashtags, named_entities = advanced_analyze_tweet_content(
            tweet['text'], referenced_text, verbose=True)
        tweet_data["keywords"] = keywords
        tweet_data["hashtags"] = hashtags
        tweet_data["named_entities"] = named_entities

        sentiment_result = analyze_tweet_sentiment(tweet_data, verbose=True)
        if 'error' not in sentiment_result:
            tweet_data["sentiment"] = sentiment_result
        else:
            logging.error(
                f"Error in sentiment analysis: {sentiment_result['error']}")

        response, rating = evaluate_social_responsibility(
            tweet_data, verbose=True)
        if rating:
            tweet_data["social_responsibility"] = {
                "response": response, "rating": rating}

        tweets_data.append(tweet_data)
        logging.info(f"Processed and added tweet {tweet['id']}")

    logging.info(f"Total tweets processed: {len(tweets_data)}")
    return tweets_data


def referenced_tweet_id_lookup(tweet_id):
    url = f"https://api.twitter.com/2/tweets/{tweet_id}"
    params = {
        "tweet.fields": "attachments,text,author_id,entities,created_at",
        "expansions": "attachments.media_keys,author_id",
        "media.fields": "type,url,preview_image_url",
        "user.fields": "name,username"
    }
    headers = {
        "Authorization": f"Bearer {BEARER_TOKEN}"
    }

    response = requests.get(url, params=params, headers=headers)

    if response.status_code != 200:
        logging.error(
            f"Failed to fetch referenced tweet {tweet_id}: {response.status_code}")
        return {"text": "", "image_description": "", "image_url": ""}

    ref_tweet_data = response.json()
    tweet = ref_tweet_data['data']
    includes = ref_tweet_data.get('includes', {})

    media_description = ""
    image_url = ""

    if 'attachments' in tweet and 'media_keys' in tweet['attachments']:
        media_keys = tweet['attachments']['media_keys']
        for media_key in media_keys:
            media = next((m for m in includes.get('media', [])
                         if m['media_key'] == media_key), None)
            if media:
                if media['type'] == 'photo':
                    image_url = media['url']
                    try:
                        media_description = analyze_image_with_gpt4o(
                            image_url, verbose=True)
                    except Exception as e:
                        logging.error(f"Error analyzing image: {str(e)}")
                elif media['type'] == 'video':
                    preview_url = media.get(
                        'preview_image_url', 'Not available')
                    if preview_url != 'Not available':
                        image_url = preview_url
                        try:
                            media_description = analyze_image_with_gpt4o(
                                preview_url, verbose=True)
                        except Exception as e:
                            logging.error(
                                f"Error analyzing video preview: {str(e)}")
            else:
                logging.warning(f"No media found for media_key: {media_key}")

    return {"text": tweet['text'], "image_description": media_description, "image_url": image_url}


def main():
    existing_tweets = load_from_blob()
    tweet_id, created_at, text = get_latest_tweet()

    if created_at:
        latest_datetime = datetime.strptime(
            created_at, "%Y-%m-%dT%H:%M:%S.%fZ")
        start_time = (latest_datetime + timedelta(seconds=1)
                      ).replace(tzinfo=timezone.utc)
    else:
        start_time = (datetime.now(timezone.utc) - timedelta(hours=24))

    user_id = "44196397"  # Elon Musk's Twitter user ID
    url = f"https://api.twitter.com/2/users/{user_id}/tweets"
    params = {
        "tweet.fields": "attachments,created_at,text,author_id,referenced_tweets",
        "expansions": "attachments.media_keys,referenced_tweets.id",
        "media.fields": "type,url,preview_image_url",
        "user.fields": "username,name,profile_image_url",
        "max_results": 100,
        "start_time": start_time.isoformat()
    }
    headers = {
        "Authorization": f"Bearer {BEARER_TOKEN}"
    }

    logging.info(f"Fetching tweets since: {start_time.isoformat()}")
    response = requests.get(url, params=params, headers=headers)

    if response.status_code == 200:
        response_json = response.json()
        new_tweets = parse_tweets(response_json)

        if new_tweets:
            all_tweets = existing_tweets + new_tweets
            save_to_blob(all_tweets)
            insert_tweets_into_db(new_tweets)
        else:
            logging.info("No new tweets to save or insert.")
    else:
        logging.error(f"Failed to fetch tweets: {response.status_code}")
        logging.error(f"Response: {response.json()}")
