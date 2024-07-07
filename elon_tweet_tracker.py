import requests
import dotenv
import os
import logging
import json
from datetime import datetime, timedelta, timezone

from utils import analyze_image_with_gpt4o, evaluate_social_responsibility, analyze_tweet_sentiment, advanced_analyze_tweet_content
from db_utils import get_latest_tweet, insert_tweets_into_db

dotenv.load_dotenv()
BEARER_TOKEN = os.getenv("BEARER_TOKEN")

logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s - %(levelname)s - %(message)s')

tweets_data = []  # This will store all processed tweet data


def load_from_json(filename='tweets_data.json'):
    if os.path.exists(filename):
        with open(filename, 'r') as file:
            return json.load(file)
    return []


def save_to_json(data, filename='tweets_data.json'):
    logging.info(f"Attempting to save {len(data)} tweets to {filename}")
    with open(filename, 'w') as file:
        json.dump(data, file, indent=4)
    logging.info(f"Data saved to {filename}")


def parse_tweets(tweets_response):
    global tweets_data

    if 'data' not in tweets_response:
        logging.warning("No new tweets found.")
        return

    def get_tweet_url(tweet_id):
        return f"https://x.com/i/web/status/{tweet_id}"

    def process_media(media_keys, includes_media):
        media_info = []
        image_descriptions = []
        for media_key in media_keys:
            media = next(
                (m for m in includes_media if m['media_key'] == media_key), None)
            if media:
                if media['type'] == 'photo':
                    image_url = media['url']
                    media_info.append(f"Image URL: {image_url}")
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
                        try:
                            image_description = analyze_image_with_gpt4o(
                                preview_url, verbose=True)
                            media_info.append(
                                f"Video Preview Description: {image_description}")
                            image_descriptions.append(image_description)
                        except Exception as e:
                            media_info.append(
                                f"Error analyzing video preview: {str(e)}")
        return media_info, image_descriptions

    includes_media = tweets_response.get('includes', {}).get('media', [])

    for tweet in tweets_response['data']:
        logging.info(f"Processing tweet {tweet['id']}")
        print("-" * 80)
        print("Tweet:")
        print(
            f"({tweet['created_at']}) Author ID: {tweet['author_id']} tweeted: {tweet['text']}")
        print(f"Tweet URL: {get_tweet_url(tweet['id'])}")

        tweet_data = {
            "id": tweet['id'],
            "text": tweet['text'],
            "created_at": tweet['created_at'],
            "author_id": tweet['author_id'],
            "url": get_tweet_url(tweet['id']),
            "image_descriptions": [],
            "referenced_tweets": []
        }

        if 'attachments' in tweet and 'media_keys' in tweet['attachments']:
            print("Media in this tweet:")
            media_info, image_descriptions = process_media(
                tweet['attachments']['media_keys'], includes_media)
            for info in media_info:
                print(info)
            tweet_data["image_descriptions"] = image_descriptions

        referenced_text = ""
        if 'referenced_tweets' in tweet:
            for ref in tweet['referenced_tweets']:
                print(f"\nReferenced tweet (type: {ref['type']}):")
                ref_data = referenced_tweet_id_lookup(ref['id'])
                tweet_data["referenced_tweets"].append({
                    "type": ref['type'],
                    "id": ref['id'],
                    "text": ref_data["text"],
                    "image_description": ref_data["image_description"]
                })
                referenced_text += ref_data["text"]

        # Perform advanced content analysis
        keywords, hashtags, named_entities = advanced_analyze_tweet_content(
            tweet['text'], referenced_text, verbose=True)
        print("\nContent Analysis:")
        print(f"Keywords: {keywords}")
        print(f"Hashtags: {hashtags}")
        print(f"Named Entities: {named_entities}")

        tweet_data["keywords"] = keywords
        tweet_data["hashtags"] = hashtags
        tweet_data["named_entities"] = named_entities

        # Perform sentiment analysis
        sentiment_result = analyze_tweet_sentiment(tweet_data, verbose=True)
        print("\nSentiment Analysis:")
        if 'error' not in sentiment_result:
            print(f"Sentiment Score: {sentiment_result['sentiment_score']}")
            print(f"Explanation: {sentiment_result['explanation']}")
            print(f"Key Factors: {', '.join(sentiment_result['key_factors'])}")
            tweet_data["sentiment"] = sentiment_result
        else:
            print(f"Error in sentiment analysis: {sentiment_result['error']}")

        # Evaluate social responsibility
        response, rating = evaluate_social_responsibility(
            tweet_data, verbose=True)
        print(f"\nSocial Responsibility Evaluation: {response}")
        if rating:
            print(f"Rating: {rating}/100")
            tweet_data["social_responsibility"] = {
                "response": response, "rating": rating}

        print()  # Empty line for separation between tweets

        # Append the processed tweet data to the list
        tweets_data.append(tweet_data)
        logging.info(f"Processed and added tweet {tweet['id']}")

    logging.info(f"Total tweets processed: {len(tweets_data)}")


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
        print(
            f"Failed to fetch referenced tweet {tweet_id}: {response.status_code}")
        return {"text": "", "image_description": ""}

    ref_tweet_data = response.json()
    tweet = ref_tweet_data['data']
    includes = ref_tweet_data.get('includes', {})

    print(f"\nReferenced Tweet:")
    print(f"ID: {tweet_id}")
    print(f"Created at: {tweet.get('created_at')}")
    print(
        f"Author: {includes.get('users', [{}])[0].get('username', 'Unknown')}")
    print(f"Text: {tweet['text']}")
    print(f"Tweet URL: https://x.com/i/web/status/{tweet_id}")

    media_description = ""

    if 'attachments' in tweet and 'media_keys' in tweet['attachments']:
        media_keys = tweet['attachments']['media_keys']
        for media_key in media_keys:
            media = next((m for m in includes.get('media', [])
                         if m['media_key'] == media_key), None)
            if media:
                print(f"Media Type: {media['type']}")
                if media['type'] == 'photo':
                    image_url = media['url']
                    print(f"Image URL: {image_url}")
                    try:
                        media_description = analyze_image_with_gpt4o(
                            image_url, verbose=True)
                        print(f"Image Description: {media_description}")
                    except Exception as e:
                        print(f"Error analyzing image: {str(e)}")
                elif media['type'] == 'video':
                    preview_url = media.get(
                        'preview_image_url', 'Not available')
                    print(f"Video Preview URL: {preview_url}")
                    if preview_url != 'Not available':
                        try:
                            media_description = analyze_image_with_gpt4o(
                                preview_url, verbose=True)
                            print(
                                f"Video Preview Description: {media_description}")
                        except Exception as e:
                            print(f"Error analyzing video preview: {str(e)}")
            else:
                print(f"No media found for media_key: {media_key}")
    else:
        print("No attachments or media keys in this referenced tweet.")

    return {"text": tweet['text'], "image_description": media_description}


def main():
    global tweets_data
    tweets_data = load_from_json()  # Load existing tweets from JSON file

    tweet_id, created_at, text = get_latest_tweet()

    if created_at:
        latest_datetime = datetime.strptime(
            created_at, "%Y-%m-%dT%H:%M:%S.%fZ")
        start_time = (latest_datetime + timedelta(seconds=1)
                      ).replace(tzinfo=timezone.utc)
    else:
        start_time = (datetime.now(timezone.utc) - timedelta(hours=24))

    user_id = "44196397"
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
    print(f"Request URL: {url}")
    print(f"Request Headers: {headers}")
    print(f"Request Params: {params}")

    response = requests.get(url, params=params, headers=headers)

    if response.status_code == 200:
        response_json = response.json()
        # Print the entire response JSON
        print(json.dumps(response_json, indent=2))
        parse_tweets(response_json)

        logging.info(f"Number of tweets processed: {len(tweets_data)}")

        if tweets_data:
            # Save to JSON file after parsing all tweets
            save_to_json(tweets_data)

            # Insert new tweets into the database
            insert_tweets_into_db()
        else:
            logging.warning("No new tweets to save or insert.")
    else:
        print(f"Failed to fetch tweets: {response.status_code}")
        print(f"Response: {response.json()}")


if __name__ == "__main__":
    main()
