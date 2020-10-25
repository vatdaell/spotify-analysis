from os import getenv
from dotenv import load_dotenv, find_dotenv
from common.apiauth import genius_authenticate
from common.store import Store


if __name__ == "__main__":
    # Load env vars
    load_dotenv(find_dotenv())

    genius = genius_authenticate(getenv("GENIUS_ACCESS_TOKEN"))
    song = genius.search_song("daw", "aijrwoa")
    if song is None:
        print("Song not found")
    else:
        print("Song found")