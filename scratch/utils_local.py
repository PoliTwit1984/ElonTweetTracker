from collections import Counter
import logging
from dotenv import load_dotenv
import os
import openai
import re
import json
import nltk
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
from nltk import pos_tag, ne_chunk
from nltk.chunk import tree2conlltags

# Load environment variables from .env file
load_dotenv()

# Retrieve OpenAI API key from environment variables
openai_api_key = os.getenv('OPENAI_API_KEY')

# Set OpenAI API key
client = openai.Client(api_key=openai_api_key)

# Ensure the stopwords corpus is downloaded
nltk.download('stopwords', quiet=True)
nltk.download('punkt', quiet=True)
nltk.download('averaged_perceptron_tagger', quiet=True)
nltk.download('maxent_ne_chunker', quiet=True)
nltk.download('words', quiet=True)

stop_words = set(stopwords.words('english'))


def advanced_analyze_tweet_content(tweet_text, referenced_text="", verbose=False):
    if verbose:
        logging.debug(
            f"Analyzing tweet content: {tweet_text} {referenced_text}")

    try:
        combined_text = f"{tweet_text} {referenced_text}"
        words = word_tokenize(combined_text)
        filtered_words = [word for word in words if word.lower(
        ) not in stop_words and len(word) > 2]
        pos_tags = pos_tag(filtered_words)
        named_entities = ne_chunk(pos_tags)
        iob_tagged = tree2conlltags(named_entities)

        keywords = [word for word, pos, ne in iob_tagged if ne ==
                    'O' and pos.startswith(('NN', 'VB', 'JJ'))]
        named_entities = [word for word, pos, ne in iob_tagged if ne != 'O']
        word_freq = Counter(keywords)
        final_keywords = [(word, count) for word, count in word_freq.most_common(
        ) if re.match("^[a-zA-Z0-9_-]*$", word)]
        hashtags = re.findall(r"#(\w+)", combined_text)

        if verbose:
            logging.debug(f"Keywords: {final_keywords}")
            logging.debug(f"Hashtags: {hashtags}")
            logging.debug(f"Named Entities: {named_entities}")

        return final_keywords, hashtags, named_entities

    except Exception as e:
        if verbose:
            logging.error(
                f"An error occurred while analyzing tweet content: {e}")
        return [], [], []


def analyze_tweet_sentiment(tweet_data, verbose=False):
    if verbose:
        logging.debug(f"Analyzing sentiment for tweet: {tweet_data['id']}")

    try:
        tweet_text = tweet_data['text']
        context = ""

        if 'referenced_tweets' in tweet_data:
            for ref_tweet in tweet_data['referenced_tweets']:
                if ref_tweet['type'] == 'replied_to':
                    context += f"This tweet is a reply to: '{ref_tweet['text']}'\n"

        if 'image_descriptions' in tweet_data:
            context += "The tweet includes the following images:\n"
            for i, desc in enumerate(tweet_data['image_descriptions'], 1):
                context += f"Image {i}: {desc}\n"

        prompt = f"""Analyze the sentiment of the following tweet by Elon Musk. Consider the context if provided. 
        Rate the sentiment on a scale from -1 (very negative) to 1 (very positive).
        Also provide a brief explanation for the rating and list key factors influencing the sentiment.

        Context: {context}

        Tweet: "{tweet_text}"

        Format your response as follows:
        Sentiment rating: [Your rating]
        Explanation: [Your explanation]
        Key factors: [List of key factors]
        """

        completion = client.chat.completions.create(
            model="gpt-4o",
            messages=[
                {"role": "system", "content": "You are a sentiment analysis expert specializing in analyzing Elon Musk's tweets."},
                {"role": "user", "content": prompt}
            ]
        )

        response = completion.choices[0].message.content

        # More robust parsing of the response
        sentiment_score = 0
        explanation = "No explanation provided"
        key_factors = []

        for line in response.split('\n'):
            if line.startswith("Sentiment rating:"):
                try:
                    sentiment_score = float(line.split(":")[1].strip())
                except ValueError:
                    sentiment_score = 0
            elif line.startswith("Explanation:"):
                explanation = line.split(":", 1)[1].strip()
            elif line.startswith("Key factors:"):
                key_factors = [factor.strip() for factor in line.split(":", 1)[
                    1].strip().split(',')]

        result = {
            'tweet_id': tweet_data['id'],
            'tweet_text': tweet_text,
            'context': context,
            'sentiment_score': sentiment_score,
            'explanation': explanation,
            'key_factors': key_factors
        }

        if verbose:
            logging.debug("Sentiment analysis complete")
            logging.debug(f"Sentiment analysis result: {result}")

        return result
    except Exception as e:
        if verbose:
            logging.error(
                f"An error occurred while analyzing tweet sentiment: {e}")
        return {
            'tweet_id': tweet_data['id'],
            'error': f"Error analyzing tweet sentiment: {str(e)}"
        }


def analyze_image_with_gpt4o(image_url, verbose=False):
    if verbose:
        logging.debug(f"Analyzing image: {image_url}")
    try:
        response = client.chat.completions.create(
            model="gpt-4-vision-preview",
            messages=[
                {
                    "role": "user",
                    "content": [
                        {"type": "text", "text": "Describe this image briefly."},
                        {"type": "image_url", "image_url": {"url": image_url}}
                    ]
                }
            ],
            max_tokens=300,
        )

        image_description = response.choices[0].message.content

        if verbose:
            logging.debug(f"GPT-4 Vision response: {response}")
            logging.debug("Image analysis complete")

        return image_description
    except Exception as e:
        if verbose:
            logging.error(f"An error occurred while analyzing the image: {e}")
        return f"Error analyzing image: {str(e)}"


def evaluate_social_responsibility(tweet_data, verbose=False):
    if verbose:
        logging.debug(
            f"Evaluating social responsibility for tweet: {tweet_data['text']}")

    try:
        content = f"Tweet by Elon Musk: {tweet_data['text']}\n"
        if 'image_descriptions' in tweet_data:
            for i, desc in enumerate(tweet_data['image_descriptions'], 1):
                content += f"Image {i} in tweet: {desc}\n"
        if 'referenced_tweets' in tweet_data:
            for ref_tweet in tweet_data['referenced_tweets']:
                content += f"Referenced tweet: {ref_tweet['text']}\n"

        completion = client.chat.completions.create(
            model="gpt-4o",
            messages=[
                {
                    "role": "system",
                    "content": "You are an expert in social responsibility. Your task is to evaluate tweets by Elon Musk for social responsibility. Consider that Elon Musk is the owner of Twitter (now X), CEO of Tesla and SpaceX, and has a massive following of over 100 million on the platform. His tweets can significantly influence public opinion, stock markets, and global conversations. Consider the tweet text, any images described, and the context of retweets or replies if present. Assess whether the content is socially responsible given his position of influence. Provide a nuanced analysis and a numerical rating from 1 to 100, where 1 is least socially responsible and 100 is most socially responsible."
                },
                {
                    "role": "user",
                    "content": f"Evaluate the following tweet by Elon Musk for social responsibility, considering his influence and position. Explain your reasoning. At the end of the response provide a numerical rating from 1 to 100 in the format of Rating: X where X is the actual numerical value:\n\n{content}"
                },
            ],
        )
        if verbose:
            logging.debug("Social responsibility evaluation complete")

        response = completion.choices[0].message.content

        rating_match = re.search(r'Rating: (\d+)', response)
        if rating_match:
            rating = int(rating_match.group(1))
        else:
            rating = None
            if verbose:
                logging.warning(
                    "Could not extract numerical rating from the response")

        return response, rating
    except Exception as e:
        if verbose:
            logging.error(
                f"An error occurred while evaluating social responsibility: {e}")
        return f"Error evaluating social responsibility: {str(e)}", None
