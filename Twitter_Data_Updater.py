import os
import requests
import json
import pandas as pd
import time
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()
BEARER_TOKEN = os.getenv("BEARER_TOKEN")
HEADERS = {"Authorization": f"Bearer {BEARER_TOKEN}"}


def fetch_tweets():
    """Fetch tweets and save them to a CSV file"""
    url = "https://api.x.com/2/lists/1651199577985261569/tweets"
    querystring = {"max_results": "20"}

    response = requests.get(url, headers=HEADERS, params=querystring)

    if response.status_code == 200:
        data = response.json()
        df = pd.DataFrame(data.get("data", []))
        return df


# I want to update this to get the author_id, tweet_timestamp & public_metrics
# querystring = {"tweet.fields":["author_id","public_metrics"],"user.fields":"username"}
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

    df_metrics = context_df['public_metrics'].apply(pd.Series)
    
    context_df.drop(columns=['public_metrics'], inplace=True)

    context_df = context_df.join(df_metrics, how="inner")

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

    # Rename the column 'old_name' to 'new_name'
    author_df.rename(columns={'id': 'author_id'}, inplace=True)


    #print("author_df: ", author_df, "\n\n")

    merged_df = pd.merge(author_df, context_df, left_on='author_id', right_on='author_id', how='inner')
    merged_df.drop(['name', 'bookmark_count'], axis=1, inplace=True)
    merged_df.rename(columns={'username': 'author_username'}, inplace=True)

    merged_df['edit_history_tweet_ids'] = merged_df['edit_history_tweet_ids'].apply(tuple)
    merged_df.drop_duplicates(inplace=True)
    
    merged_df.to_csv("test.csv", index=False)
    
    return merged_df



# Returns a dataframe with three columns: id, edit_history_tweet_ids, text
tweets = fetch_tweets()
processed_tweets = get_context_from_post(tweets)

# df_bottom
df = pd.read_csv('merged_tweets_metrics_1.csv')

# df_top
processed_tweets = processed_tweets[df.columns]
# Concatenate dataframes with the top one first
df_combined = pd.concat([processed_tweets, df], ignore_index=True)

df_combined_no_duplicates = df_combined.drop_duplicates()

# Convert the timestamp column to datetime
df_combined_no_duplicates['created_at'] = pd.to_datetime(df_combined_no_duplicates['created_at'], errors='coerce')
df_combined_no_duplicates.sort_values("created_at", ascending=False, inplace=True)

# Save the combined dataframe to a new CSV file
df_combined_no_duplicates.to_csv("latest.csv", index=False)
