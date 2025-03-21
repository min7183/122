import sys
import os
import csv
import mysql.connector

def connect_db():
    return mysql.connector.connect(user='test', password='password', database='cs122a')

def import_data(folder_name):
    conn = connect_db()
    cursor = conn.cursor()
    try:
        cursor.execute("SET FOREIGN_KEY_CHECKS = 0;")
        
        drop_tables = ["sessions", "reviews", "videos", "movies", "series", "releases", "viewers", "producers", "users"]
        for table in drop_tables:
            cursor.execute(f"DROP TABLE IF EXISTS {table};")
        
        cursor.execute("""
            CREATE TABLE users (
                uid INT PRIMARY KEY,
                email VARCHAR(255) UNIQUE NOT NULL,
                joined_date DATE,
                nickname VARCHAR(255),
                street VARCHAR(255),
                city VARCHAR(255),
                state VARCHAR(255),
                `zip` VARCHAR(255),
                genres VARCHAR(255)
            );
        """)
        
        cursor.execute("""
            CREATE TABLE viewers (
                uid INT PRIMARY KEY,
                subscription VARCHAR(255),
                first_name VARCHAR(255),
                last_name VARCHAR(255),
                FOREIGN KEY (uid) REFERENCES users(uid) ON DELETE CASCADE
            );
        """)
        
        cursor.execute("""
            CREATE TABLE producers (
                uid INT PRIMARY KEY,
                bio TEXT,
                company VARCHAR(255),
                FOREIGN KEY (uid) REFERENCES users(uid) ON DELETE CASCADE
            );
        """)
        
        cursor.execute("""
            CREATE TABLE releases (
                rid INT PRIMARY KEY,
                producer_uid INT,
                title VARCHAR(255) NOT NULL,
                genre VARCHAR(255),
                release_date DATE,
                FOREIGN KEY (producer_uid) REFERENCES producers(uid) ON DELETE CASCADE
            );
        """)
        
        cursor.execute("""
            CREATE TABLE series (
                rid INT PRIMARY KEY,
                introduction TEXT,
                FOREIGN KEY (rid) REFERENCES releases(rid) ON DELETE CASCADE
            );
        """)
        
        cursor.execute("""
            CREATE TABLE movies (
                rid INT PRIMARY KEY,
                website_url VARCHAR(255),
                FOREIGN KEY (rid) REFERENCES releases(rid) ON DELETE CASCADE
            );
        """)
        
        cursor.execute("""
            CREATE TABLE videos (
                rid INT,
                ep_num INT,
                title VARCHAR(255),
                length INT,
                PRIMARY KEY (rid, ep_num),
                FOREIGN KEY (rid) REFERENCES releases(rid) ON DELETE CASCADE
            );
        """)
        
        cursor.execute("""
            CREATE TABLE reviews (
                rvid INT PRIMARY KEY,
                uid INT,
                rid INT,
                rating DECIMAL(3,1),
                comment TEXT,
                posted_at DATETIME,
                FOREIGN KEY (uid) REFERENCES viewers(uid) ON DELETE CASCADE,
                FOREIGN KEY (rid) REFERENCES releases(rid) ON DELETE CASCADE
            );
        """)
        
        cursor.execute("""
            CREATE TABLE sessions (
                sid INT PRIMARY KEY,
                uid INT,
                rid INT,
                ep_num INT,
                initiate_at DATETIME,
                leave_at DATETIME,
                quality VARCHAR(255),
                device VARCHAR(255),
                FOREIGN KEY (uid) REFERENCES viewers(uid) ON DELETE CASCADE,
                FOREIGN KEY (rid, ep_num) REFERENCES videos(rid, ep_num) ON DELETE CASCADE
            );
        """)
        
        cursor.execute("SET FOREIGN_KEY_CHECKS = 1;")
        tables_for_import = ["users", "viewers", "producers", "releases", "series", "movies", "videos", "reviews", "sessions"]
        for table in tables_for_import:
            csv_path = os.path.join(folder_name, f"{table}.csv")
            if not os.path.exists(csv_path):
                continue
            with open(csv_path, 'r', newline='') as csvfile:
                csv_reader = csv.reader(csvfile)
                next(csv_reader, None)
                for row in csv_reader:
                    row = [None if field == '' else field for field in row]
                    placeholders = ", ".join(["%s"] * len(row))
                    query = f"INSERT INTO {table} VALUES ({placeholders})"
                    cursor.execute(query, row)
        
        conn.commit()
        print("Success")
    except mysql.connector.Error as err:
        conn.rollback()
        print("Fail", err)
    finally:
        cursor.close()
        conn.close()

def insert_viewer(uid, email, nickname, street, city, state, zip_code, genres, joined_date, first_name, last_name, subscription):
    """
    Inserts a new viewer.
    Command order: uid, email, nickname, street, city, state, zip, genres, joined_date, first_name, last_name, subscription
    Note: Users table requires order: uid, email, joined_date, nickname, street, city, state, `zip`, genres
    """
    conn = connect_db()
    cursor = conn.cursor()
    try:
        cursor.execute("""
            INSERT INTO users (uid, email, joined_date, nickname, street, city, state, `zip`, genres)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (uid, email, joined_date, nickname, street, city, state, zip_code, genres))
        
        cursor.execute("""
            INSERT INTO viewers (uid, subscription, first_name, last_name)
            VALUES (%s, %s, %s, %s)
        """, (uid, subscription, first_name, last_name))
        
        conn.commit()
        print("Success")
    except mysql.connector.Error as err:
        conn.rollback()
        print("Fail")
    finally:
        cursor.close()
        conn.close()

def add_genre(uid, genre):
    conn = connect_db()
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT genres FROM users WHERE uid = %s", (uid,))
        result = cursor.fetchone()
        if result is not None:
            current_genres = result[0]
            if current_genres:
                genres_list = [g.strip() for g in current_genres.split(';')]
                if genre.lower() in [g.lower() for g in genres_list]:
                    print("Fail")
                    return
                updated_genres = current_genres + ";" + genre
            else:
                updated_genres = genre
            cursor.execute("UPDATE users SET genres = %s WHERE uid = %s", (updated_genres, uid))
            conn.commit()
            print("Success")
        else:
            print("Fail")
    except mysql.connector.Error as err:
        conn.rollback()
        print("Fail", err)
    finally:
        cursor.close()
        conn.close()

def delete_viewer(uid):
    conn = connect_db()
    cursor = conn.cursor()
    try:
        cursor.execute("DELETE FROM viewers WHERE uid = %s", (uid,))
        cursor.execute("DELETE FROM users WHERE uid = %s", (uid,))
        conn.commit()
        print("Success")
    except mysql.connector.Error as err:
        conn.rollback()
        print("Fail", err)
    finally:
        cursor.close()
        conn.close()

def insert_movie(rid, website_url):
    conn = connect_db()
    cursor = conn.cursor()
    try:
        cursor.execute("INSERT INTO movies (rid, website_url) VALUES (%s, %s)", (rid, website_url))
        conn.commit()
        print("Success")
    except mysql.connector.Error as err:
        conn.rollback()
        print("Fail", err)
    finally:
        cursor.close()
        conn.close()

def insert_session(sid, uid, rid, ep_num, initiate_at, leave_at, quality, device):
    conn = connect_db()
    cursor = conn.cursor()
    try:
        cursor.execute("""
            INSERT INTO sessions (sid, uid, rid, ep_num, initiate_at, leave_at, quality, device)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """, (sid, uid, rid, ep_num, initiate_at, leave_at, quality, device))
        conn.commit()
        print("Success")
    except mysql.connector.Error as err:
        conn.rollback()
        print("Fail")
    finally:
        cursor.close()
        conn.close()

def update_release(rid, title):
    conn = connect_db()
    cursor = conn.cursor()
    try:
        cursor.execute("UPDATE releases SET title = %s WHERE rid = %s", (title, rid))
        conn.commit()
        print("Success")
    except mysql.connector.Error as err:
        conn.rollback()
        print("Fail", err)
    finally:
        cursor.close()
        conn.close()

def list_releases(uid):
    conn = connect_db()
    cursor = conn.cursor()
    try:
        cursor.execute("""
            SELECT DISTINCT r.rid, r.genre, r.title
            FROM releases r
            JOIN reviews rev ON r.rid = rev.rid
            WHERE rev.uid = %s
            ORDER BY r.title ASC
        """, (uid,))
        results = cursor.fetchall()
        for row in results:
            print(",".join(str(item) if item is not None else "NULL" for item in row))
    except mysql.connector.Error as err:
        pass
    finally:
        cursor.close()
        conn.close()

def popular_release(n):
    conn = connect_db()
    cursor = conn.cursor()
    try:
        cursor.execute("""
            SELECT r.rid, r.title, COUNT(rev.rid) as reviewCount
            FROM releases r
            LEFT JOIN reviews rev ON r.rid = rev.rid
            GROUP BY r.rid, r.title
            ORDER BY reviewCount DESC, r.rid DESC
            LIMIT %s
        """, (n,))
        results = cursor.fetchall()
        for row in results:
            print(",".join(str(item) if item is not None else "NULL" for item in row))
    except mysql.connector.Error as err:
        pass
    finally:
        cursor.close()
        conn.close()

def release_title(sid):
    conn = connect_db()
    cursor = conn.cursor()
    try:
        cursor.execute("""
            SELECT r.rid, r.title AS release_title, r.genre, v.title AS video_title, v.ep_num, v.length
            FROM sessions s
            JOIN releases r ON s.rid = r.rid
            JOIN videos v ON s.rid = v.rid AND s.ep_num = v.ep_num
            WHERE s.sid = %s
            ORDER BY r.title ASC
        """, (sid,))
        results = cursor.fetchall()
        if results:
            for row in results:
                print(",".join(str(item) if item is not None else "NULL" for item in row))
        else:
            print("Fail")
    except mysql.connector.Error as err:
        print("Fail", err)
    finally:
        cursor.close()
        conn.close()

def active_viewer(minimum_sessions, start_date, end_date):
    database_connection = connect_db()
    database_cursor = database_connection.cursor()
    active_users_list = []

    try:
        database_cursor.execute("""
            SELECT viewer.uid, viewer.first_name, viewer.last_name
            FROM viewers viewer
            JOIN sessions session ON viewer.uid = session.uid
            WHERE session.initiate_at BETWEEN %s AND %s
            GROUP BY viewer.uid, viewer.first_name, viewer.last_name
            HAVING COUNT(session.sid) >= %s
            ORDER BY viewer.uid ASC
        """, (start_date, end_date, minimum_sessions))

        matching_viewers = database_cursor.fetchall()

        for viewer in matching_viewers:
            formatted_viewer = ",".join(str(detail) if detail is not None else "NULL" for detail in viewer)
            print(formatted_viewer)

    except mysql.connector.Error:
        print("Fail")
    finally:
        database_cursor.close()
        database_connection.close()


def videos_viewed(rid):
    conn = connect_db()
    cursor = conn.cursor()
    try:
        cursor.execute("""
            SELECT COUNT(DISTINCT s.uid) 
            FROM sessions s 
            WHERE s.rid = %s
        """, (rid,))
        viewer_count = cursor.fetchone()[0]

        cursor.execute("""
            SELECT rid, ep_num, title, length
            FROM videos
            WHERE rid = %s
            ORDER BY ep_num ASC
        """, (rid,))
        
        results = cursor.fetchall()
        
        if results:
            for row in results:
                full_row = list(row) + [viewer_count]
                print(",".join(str(item) if item is not None else "NULL" for item in full_row))
        else:
            print("Fail")
    except mysql.connector.Error as err:
        print("Fail")
    finally:
        cursor.close()
        conn.close()


def handle_command():
    if len(sys.argv) < 2:
        print("Invalid command.")
        return
    command = sys.argv[1]
    args = sys.argv[2:]
    
    if command == "import":
        import_data(args[0])
    elif command == "insertViewer":
        insert_viewer(int(args[0]), args[1], args[2], args[3], args[4], args[5],
                      args[6], args[7], args[8], args[9], args[10], args[11])
    elif command == "addGenre":
        add_genre(int(args[0]), args[1])
    elif command == "deleteViewer":
        delete_viewer(int(args[0]))
    elif command == "insertMovie":
        insert_movie(int(args[0]), args[1])
    elif command == "insertSession":
        insert_session(int(args[0]), int(args[1]), int(args[2]), int(args[3]),
                       args[4], args[5], args[6], args[7])
    elif command == "updateRelease":
        update_release(int(args[0]), args[1])
    elif command == "listReleases":
        list_releases(int(args[0]))
    elif command == "popularRelease":
        popular_release(int(args[0]))
    elif command == "releaseTitle":
        release_title(int(args[0]))
    elif command == "activeViewer":
        active_viewer(int(args[0]), args[1], args[2])
    elif command == "videosViewed":
        videos_viewed(int(args[0]))
    else:
        print(f"Unknown command: {command}")

if __name__ == "__main__":
    handle_command()