# Spotify Analysis
An exploration of my personal Spotify listening habits

## Description
Exploring my personal Spotify listening habits. The project has a data pipeline that extracts recent listens from the Spotify api and loads it to a MySQL database for further analysis. The project stores raw and intermediary data in a s3 data lake to be further processed and loaded.

## Installation

1) Clone this repository. 

```bash
git clone https://github.com/vatdaell/spotify-analysis.git
```
2) Install all the python packages
```bash
pip install -r requirements.txt
```
3) Set up an [AWS account](https://aws.amazon.com/) with an s3 bucket.
4) Create a [Spotify developer](https://developer.spotify.com/dashboard/login) account and create an application
5) Setup a MySQL database for use 

6) Create a .env file in the project directory and fill in the details.

```
S3_BUCKET=bucket_name
SPOTIPY_CLIENT_ID=clientid
SPOTIPY_CLIENT_SECRET=secret
SPOTIPY_REDIRECT_URI=redirect_uri
MYSQL_URL=mysql+pymysql://mysqlurl
TABLE_NAME=recent_plays_table_name
```

## Usage

To load the recently played data on s3 bucket and to the MySQL database
```bash
python src/etl/extraction/recently_played.py
python src/etl/transformation/transform_recently_played.py
python src/etl/loading/load_songs.py
```

## Roadmap
Some interesting features I want to implement/analyze in the future

* Use a task scheduler to automate etl tasks
* Extract lyrics of recently listened songs for sentiment analysis
* Recommend similar songs based on listening history 
* Link to merch store for top bands
* Analysis of genre of music listened to

## License
[MIT](https://choosealicense.com/licenses/mit/)