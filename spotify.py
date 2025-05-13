import spotipy
import pandas as pd
from spotipy.oauth2 import SpotifyClientCredentials
import time

client_id =""
client_secret =""
sp = spotipy.Spotify(auth_manager=SpotifyClientCredentials(client_id, client_secret))

# Fetch 500 tracks using a keyword (you can change this)
keyword = "pop"
limit = 50
offsets = range(0, 1000, limit)

all_data = []

for offset in offsets:
    results = sp.search(q=keyword, type='track', limit=limit, offset=offset)
    tracks = results['tracks']['items']

    for track in tracks:
        artists = track['artists']
        artist_names = ', '.join([a['name'] for a in artists])
        artist_ids = ', '.join([a['id'] for a in artists])

        album = track['album']
        external_ids = track.get('external_ids', {}).get('isrc', '')

        all_data.append({
            "Track Name": track['name'],
            "Artist Name(s)": artist_names,
            "Album Name": album['name'],
            "Album Type": album['album_type'],
            "Release Date": album['release_date'],
            "Total Tracks in Album": album['total_tracks'],
            "Track Number": track['track_number'],
            "Duration (ms)": track['duration_ms'],
            "Explicit": track['explicit'],
            "Popularity": track['popularity'],
            "Spotify Track URL": track['external_urls']['spotify'],
            "Spotify Preview URL": track['preview_url'],
            "Available Markets": len(track['available_markets']),
            "Disc Number": track['disc_number'],
            "Track ID": track['id'],
            "Artist ID(s)": artist_ids,
            "Album ID": album['id'],
            "Track URI": track['uri'],
            "Is Local": track['is_local'],
            "External IDs": external_ids
        })

    time.sleep(0.5)  # Avoid hitting rate limits

# Save to CSV
df = pd.DataFrame(all_data)
df.to_json("/international_artists_tracks.json", index=False)

