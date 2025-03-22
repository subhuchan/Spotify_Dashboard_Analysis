import requests
import pandas as pd
import time
from requests.exceptions import RequestException

# Spotify API Credentials
client_id = '3e5d6d1940df4a45aea194573a7d8b98'
client_secret = '0d702e2fb84f48cc84fc5ad796496427'

# Function to get Spotify access token
def get_spotify_token(client_id, client_secret):
    auth_url = 'https://accounts.spotify.com/api/token'
    auth_response = requests.post(auth_url, {
        'grant_type': 'client_credentials',
        'client_id': client_id,
        'client_secret': client_secret,
    })
    auth_response.raise_for_status()
    auth_data = auth_response.json()
    return auth_data['access_token']

# Function to search for a track and get its ID
def search_track(track_name, artist_name, token):
    query = f"{track_name} artist:{artist_name}"
    url = f"https://api.spotify.com/v1/search?q={query}&type=track"
    retries = 3
    for attempt in range(retries):
        try:
            response = requests.get(url, headers={
                'Authorization': f'Bearer {token}'
            }, timeout=10)
            response.raise_for_status()
            json_data = response.json()
            first_result = json_data['tracks']['items'][0]
            return first_result['id']
        except (KeyError, IndexError):
            return None
        except RequestException as e:
            if attempt < retries - 1:
                time.sleep(2 ** attempt)  # Exponential backoff
            else:
                print(f"Error searching track: {e}")
                return None

# Function to get track details
def get_track_details(track_id, token):
    url = f"https://api.spotify.com/v1/tracks/{track_id}"
    retries = 3
    for attempt in range(retries):
        try:
            response = requests.get(url, headers={
                'Authorization': f'Bearer {token}'
            }, timeout=10)
            response.raise_for_status()
            json_data = response.json()
            return json_data['album']['images'][0]['url']
        except (KeyError, IndexError):
            return None
        except RequestException as e:
            if attempt < retries - 1:
                time.sleep(2 ** attempt)  # Exponential backoff
            else:
                print(f"Error fetching track details: {e}")
                return None

# Get Access Token
access_token = get_spotify_token(client_id, client_secret)

# Read your DataFrame
df_spotify = pd.read_csv('spotify-2023.csv', encoding='ISO-8859-1')

# Batch Processing
batch_size = 50  # Process 50 rows per batch
total_rows = len(df_spotify)

for start_index in range(0, total_rows, batch_size):
    end_index = min(start_index + batch_size, total_rows)
    print(f"Processing rows {start_index + 1} to {end_index}...")

    for i in range(start_index, end_index):
        row = df_spotify.iloc[i]
        track_id = search_track(row['track_name'], row['artist_name'], access_token)
        if track_id:
            image_url = get_track_details(track_id, access_token)
            df_spotify.at[i, 'image_url'] = image_url
        time.sleep(0.5)  # Add delay to avoid hitting rate limits

    # Save after each batch
    df_spotify.to_csv('updated_spotify-2023.csv', index=False)
    print(f"Batch {start_index + 1}-{end_index} saved.")

print("✅ All batches processed and saved successfully!")
