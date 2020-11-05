from tasks import recently_played_transform as rpt
from tasks import recently_played_load as rpl

if __name__ == "__main__":
    tasks = [rpt, rpl]
    for task in tasks:
        task.main()
