import spotipy
from spotipy.oauth2 import SpotifyOAuth
import lyricsgenius

def spotify_authenticate(scope):
    return spotipy.Spotify(auth_manager=SpotifyOAuth(scope=scope))

def genius_authenticate(token):
    return lyricsgenius.Genius(token)
