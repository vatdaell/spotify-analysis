from tasks import recently_played_extract as rpe
from tasks import recently_played_transform as rpt
from tasks import lyrics_extract as le
from tasks import audio_features_extract as afe
from tasks import song_transform as st
from tasks import song_load as sl

if __name__ == "__main__":
    tasks = [rpe, rpt, le, afe, st, sl]
    for task in tasks:
        task.main()
