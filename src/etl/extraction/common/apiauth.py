import spotipy
from spotipy.oauth2 import SpotifyOAuth


def spotify_authenticate(scope):
    return spotipy.Spotify(auth_manager=SpotifyOAuth(scope=scope))