import mysql.connector


class DB(object):
    """
    Database abstraction to be used by ETL process
    """
    def __init__(self, host, port, username, password, db):
        self.db = mysql.connector.Connect(
            host=host,
            port=port,
            username=username,
            password=password,
            database=db
        )
        self._initializeTables()

    def insertRecentPlays(self, songdata):
        self.deleteRecentPlays()
        sql = """INSERT INTO recently_played
        (artist, album, track, duration, popularity, played_at, explicit)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
        try:
            cursor = self.db.cursor()
            cursor.executemany(sql, songdata)
            self.db.commit()
            print(cursor.rowcount, "rows were inserted")
        except mysql.connector.Error as error:
            print("Failed to update record to database rollback: {}".format(error))
            # reverting changes because of exception
            self.db.rollback()

    def deleteRecentPlays(self):
        sql = "DELETE FROM recently_played"
        cursor = self.db.cursor()
        try:
            cursor.execute(sql)
            self._resetAutoIncrement("recently_played")
            self.db.commit()
        except mysql.connector.Error as error:
            print(
                "Failed to update record to database rollback: {}".format(
                    error
                    )
                )
            # reverting changes because of exception
            self.db.rollback()

    def getRecentPlays(self):
        sql = "SELECT * FROM recently_played"
        cursor = self.db.cursor()
        cursor.execute(sql)
        result = cursor.fetchall()
        return result

    def insertSongs(self, songdata):
        self.deleteSongs()
        sql = """INSERT INTO songs
        (artist,album,track,spotify_track_id,danceability,energy,majority_key,loudness,
        mode,speechiness,acousticness,instrumentalness,liveness,valence,tempo,
        duration,lyrics,popularity,explicit)
        VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s
        )
        """
        try:
            cursor = self.db.cursor()
            cursor.executemany(sql, songdata)
            self.db.commit()
            print(cursor.rowcount, "rows were inserted")
        except mysql.connector.Error as error:
            print("Failed to update record to database rollback: {}".format(error))
            # reverting changes because of exception
            self.db.rollback()

    def deleteSongs(self):
        sql = "DELETE FROM songs"
        cursor = self.db.cursor()
        try:
            cursor.execute(sql)
            self._resetAutoIncrement("songs")
            self.db.commit()
        except mysql.connector.Error as error:
            print(
                "Failed to update record to database rollback: {}".format(
                    error
                    )
                )
            # reverting changes because of exception
            self.db.rollback()

    def _initializeTables(self):
        sql_recent = """
                CREATE TABLE IF NOT EXISTS recently_played (
                    id INT NOT NULL AUTO_INCREMENT,
                    artist VARCHAR(128) NOT NULL,
                    album VARCHAR(128) NOT NULL,
                    track VARCHAR(128) NOT NULL,
                    duration INT NOT NULL,
                    popularity INT NOT NULL,
                    played_at TIMESTAMP NOT NULL,
                    explicit BOOLEAN NOT NULL,
                    PRIMARY KEY(id)
                );
                """
        sql_song = """
                CREATE TABLE IF NOT EXISTS songs (
                    id INT NOT NULL AUTO_INCREMENT,
                    spotify_track_id VARCHAR(128) NOT NULL,
                    track VARCHAR(128) NOT NULL,
                    artist VARCHAR(128) NOT NULL,
                    album VARCHAR(128) NOT NULL,
                    duration INT NOT NULL,
                    popularity INT NOT NULL,
                    explicit BOOLEAN NOT NULL,
                    lyrics TEXT,
                    danceability FLOAT NOT NULL,
                    energy FLOAT NOT NULL,
                    majority_key FLOAT NOT NULL,
                    loudness FLOAT NOT NULL,
                    mode FLOAT NOT NULL,
                    speechiness FLOAT NOT NULL,
                    acousticness FLOAT NOT NULL,
                    instrumentalness FLOAT NOT NULL,
                    liveness FLOAT NOT NULL,
                    valence FLOAT NOT NULL,
                    tempo FLOAT NOT NULL,
                    PRIMARY KEY(id)
                );
            """
        try:
            cursor = self.db.cursor()
            cursor.execute(sql_recent)
            cursor.execute(sql_song)
            self.db.commit()

        except mysql.connector.Error as error:
            print("Failed to update record to database rollback: {}".format(error))
            # reverting changes because of exception
            self.db.rollback()

    def _resetAutoIncrement(self, table):
        sql = "ALTER TABLE {} AUTO_INCREMENT = 1".format(table)
        cursor = self.db.cursor()
        cursor.execute(sql)
        self.db.commit()
