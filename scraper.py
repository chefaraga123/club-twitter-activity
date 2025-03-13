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
    querystring = {"max_results": "100"}

    response = requests.get(url, headers=HEADERS, params=querystring)

    if response.status_code == 200:
        data = response.json()
        df = pd.DataFrame(data.get("data", []))
        df.to_csv(INPUT_CSV, index=False)
        log_message("Fetched and saved tweets successfully.")
    else:
        log_message(f"Error fetching tweets: {response.status_code}")


def get_authorid_from_post(tweet_id):
    """Fetch author ID from tweet ID"""
    url = f"https://api.x.com/2/tweets/{tweet_id}"
    querystring = {"tweet.fields": "author_id"}

    response = requests.get(url, headers=HEADERS, params=querystring)

    if response.status_code == 200:
        return response.json().get("data", {}).get("author_id")
    log_message(f"Error fetching author ID for {tweet_id}: {response.status_code}")
    return None


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


def process_csv():
    """Process tweets and fetch author information"""
    if not os.path.exists(INPUT_CSV):
        log_message("Error: Input CSV does not exist.")
        return

    df = pd.read_csv(INPUT_CSV)

    if 'id' not in df.columns:
        log_message("Error: CSV file must contain an 'id' column.")
        return

    df['author_id'] = df.get('author_id', None)
    df['author_username'] = df.get('author_username', None)

    for index, row in df.iterrows():
        if pd.isna(row['author_id']) or row['author_id'] == "":
            tweet_id = str(row['id'])
            author_id = get_authorid_from_post(tweet_id)

            if author_id:
                username = get_author_from_authorid(author_id)
                df.at[index, 'author_id'] = author_id
                df.at[index, 'author_username'] = username
                log_message(f"Processed Tweet ID: {tweet_id}, Author ID: {author_id}, Username: {username}")

            df.to_csv(OUTPUT_CSV, index=False)
            log_message(f"Progress saved after processing tweet ID {tweet_id}")

            time.sleep(60)  # Prevent hitting API rate limits

    log_message("Author processing complete.")

if __name__ == "__main__":
    log_message("Starting script")

    fetch_tweets()
    process_csv()

    log_message("Script completed")
