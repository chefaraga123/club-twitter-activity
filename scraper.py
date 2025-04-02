import os
import requests
import json
import pandas as pd
import time
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables from .env (for local testing)
load_dotenv()

# Check if running in Render (Render sets this automatically)
RUNNING_IN_RENDER = os.getenv("RENDER") == "true"

# Determine base directory
if RUNNING_IN_RENDER:
    BASE_DIR = "/usr/src/app"  # Default Render working directory
else:
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))  # Local script directory

# Define file paths
INPUT_CSV = os.path.join(BASE_DIR, "Footium_Community_Tweets_Latest.csv")
OUTPUT_CSV = os.path.join(BASE_DIR, "output_tweets_15.csv")
FINAL_CSV = os.path.join(BASE_DIR, "output_tweets_15_With_Timestamp.csv")
LOG_FILE = os.path.join(BASE_DIR, "cron_job.log")

# Use environment variables for sensitive data
BEARER_TOKEN = os.getenv("BEARER_TOKEN")
HEADERS = {"Authorization": f"Bearer {BEARER_TOKEN}"}

def log_message(message):
    """Log messages to a file for debugging"""
    with open(LOG_FILE, "a") as log:
        log.write(f"{datetime.now()} - {message}\n")

def fetch_tweets():
    """Fetch tweets and save them to a CSV file"""
    url = "https://api.x.com/2/lists/1651199577985261569/tweets"
    querystring = {"max_results": "10"}

    response = requests.get(url, headers=HEADERS, params=querystring)

    if response.status_code == 200:
        data = response.json()
        df = pd.DataFrame(data.get("data", []))
        df.to_csv(INPUT_CSV, index=False)
        log_message("Fetched and saved tweets successfully.")
        return df
    else:
        log_message(f"Error fetching tweets: {response.status_code}")


# I want to update this to get the author_id, tweet_timestamp & public_metrics
#querystring = {"tweet.fields":["author_id","public_metrics"],"user.fields":"username"}

# If I do lookup by Ids can I do this in one go?
# Can I get the context from all the tweets in the list in one?
def get_context_from_post(tweets):
    """Fetch author ID and public metrics from tweet ID"""

    tweet_ids = list(tweets['id'])
    ids_string = ",".join(tweet_ids)

    querystring = {
        "ids": ids_string,
        "tweet.fields": "author_id,public_metrics,created_at"  # Include additional fields as needed
    }

    response = requests.get(
        "https://api.x.com/2/tweets", 
        headers=HEADERS, 
        params=querystring
    )

    context_data = response.json()
    context_df = pd.DataFrame(context_data['data'])
    print("context_df: ", context_df, "\n\n")

    author_ids = []
    for tweet in response.json().get("data", []):
        author_ids.append(tweet.get("author_id"))

    author_ids_string = ",".join(author_ids)

    author_querystring = {
        "ids": author_ids_string,
        "user.fields": "username"
    }
    author_response = requests.get(
        "https://api.x.com/2/users", 
        headers=HEADERS, 
        params=author_querystring
    )

    author_data = author_response.json()
    author_data = author_data['data']

# Create a DataFrame from the extracted data
    author_df = pd.DataFrame(author_data)

    print("author_df: ", author_df, "\n\n")
    print("author_response: ", author_response.json(), "\n\n")


    if response.status_code == 200:
        tweet_data = response.json().get("data", {})
        
        # Initialize a list to hold processed tweet information
        processed_tweets = []

        for tweet in tweet_data:
            public_metrics = tweet.get("public_metrics")

            tweet_data = {
                "tweet_id": tweet.get("id"),
                "author_id": tweet.get("author_id"),
                "tweet_text": tweet.get("text"),
                "created_at": tweet.get("created_at"),
                "like_count": public_metrics.get("like_count"),
                "retweet_count": public_metrics.get("retweet_count"),
                "quote_count": public_metrics.get("quote_count"),
                "reply_count": public_metrics.get("reply_count"),
                "impression_count": public_metrics.get("impression_count"),
            }

            processed_tweets.append(tweet_data)

        print("processed_tweets: ", processed_tweets, "\n\n")
        return processed_tweets
        
    log_message(f"Error fetching data for: {response.status_code}")
    return None


# can do look up by ids (plural) rather than needing to iterate through each id singularly
def get_author_from_authorid(author_id):
    """Fetch username from author ID"""
    if not author_id:
        return None

    url = f"https://api.x.com/2/users/{author_id}"
    response = requests.get(url, headers=HEADERS)

    if response.status_code == 200:
        return response.json().get("data", {}).get("username")
    
    log_message(f"Error fetching username for author ID {author_id}: {response.status_code}")
    return None


# Returns a dataframe with three columns: id, edit_history_tweet_ids, text
tweets = fetch_tweets()
processed_tweets = get_context_from_post(tweets)
#tweet_context = get_context_from_post(tweets)
#print(tweet_context)