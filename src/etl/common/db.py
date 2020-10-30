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
        sql = "INSERT INTO recently_played(artist, album, track, duration, popularity, played_at, explicit)VALUES (%s, %s, %s, %s, %s, %s, %s)"
        cursor = self.db.cursor()
        cursor.executemany(sql, songdata)
        self.db.commit()
        print(cursor.rowcount, "rows were inserted")

    def deleteRecentPlays(self):
        sql = "DELETE FROM recently_played"
        cursor = self.db.cursor()
        cursor.execute(sql)
        self._resetAutoIncrement()
        self.db.commit()

    def getRecentPlays(self):
        sql = "SELECT * FROM recently_played"
        cursor = self.db.cursor()
        cursor.execute(sql)
        result = cursor.fetchall()
        return result

    def _initializeTables(self):
        sql = """
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
                )
            """
        cursor = self.db.cursor()
        cursor.execute(sql)

    def _resetAutoIncrement(self):
        sql = "ALTER TABLE recently_played AUTO_INCREMENT = 1"
        cursor = self.db.cursor()
        cursor.execute(sql)
        self.db.commit()
