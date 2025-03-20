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
        # Disable FK checks while dropping/creating tables.
        cursor.execute("SET FOREIGN_KEY_CHECKS = 0;")
        
        # Drop tables in reverse dependency order.
        drop_tables = ["sessions", "reviews", "videos", "movies", "series", "releases", "viewers", "producers", "users"]
        for table in drop_tables:
            cursor.execute(f"DROP TABLE IF EXISTS {table};")
        
        # Create tables in dependency order.
        # 1. users table.
        cursor.execute("""
            CREATE TABLE users (
                uid INT PRIMARY KEY,
                email VARCHAR(255) UNIQUE NOT NULL,
                nickname VARCHAR(255),
                street VARCHAR(255),
                city VARCHAR(255),
                state VARCHAR(255),
                zip VARCHAR(255),
                genres VARCHAR(255),
                joined_date DATE
            );
        """)
        
        # 2. viewers table.
        cursor.execute("""
            CREATE TABLE viewers (
                uid INT PRIMARY KEY,
                subscription VARCHAR(255),
                first_name VARCHAR(255),
                last_name VARCHAR(255),
                FOREIGN KEY (uid) REFERENCES users(uid) ON DELETE CASCADE
            );
        """)
        
        # 3. producers table.
        cursor.execute("""
            CREATE TABLE producers (
                uid INT PRIMARY KEY,
                company VARCHAR(255),
                bio TEXT,
                FOREIGN KEY (uid) REFERENCES users(uid) ON DELETE CASCADE
            );
        """)
        
        # 4. releases table.
        cursor.execute("""
            CREATE TABLE releases (
                rid INT PRIMARY KEY,
                title VARCHAR(255) NOT NULL,
                genre VARCHAR(255),
                released_date DATE
            );
        """)
        
        # 5. series table.
        cursor.execute("""
            CREATE TABLE series (
                rid INT PRIMARY KEY,
                seasons INT,
                FOREIGN KEY (rid) REFERENCES releases(rid) ON DELETE CASCADE
            );
        """)
        
        # 6. movies table.
        cursor.execute("""
            CREATE TABLE movies (
                rid INT PRIMARY KEY,
                website_url VARCHAR(255),
                FOREIGN KEY (rid) REFERENCES releases(rid) ON DELETE CASCADE
            );
        """)
        
        # 7. videos table.
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
        
        # 8. reviews table.
        cursor.execute("""
            CREATE TABLE reviews (
                uid INT,
                rid INT,
                rating DECIMAL(3,1),
                comment TEXT,
                created_at DATETIME,
                PRIMARY KEY (uid, rid),
                FOREIGN KEY (uid) REFERENCES viewers(uid) ON DELETE CASCADE,
                FOREIGN KEY (rid) REFERENCES releases(rid) ON DELETE CASCADE
            );
        """)
        
        # 9. sessions table.
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
        
        # Import CSV data.
        tables_for_import = ["users", "viewers", "producers", "releases", "series", "movies", "videos", "reviews", "sessions"]
        for table in tables_for_import:
            csv_path = os.path.join(folder_name, f"{table}.csv")
            if not os.path.exists(csv_path):
                continue
            with open(csv_path, 'r', newline='') as csvfile:
                csv_reader = csv.reader(csvfile)
                for row in csv_reader:
                    # Replace empty strings with None.
                    row = [None if field == '' else field for field in row]
                    placeholders = ", ".join(["%s"] * len(row))
                    query = f"INSERT INTO {table} VALUES ({placeholders})"
                    cursor.execute(query, row)
        
        conn.commit()
        print("Success")
    except mysql.connector.Error as err:
        conn.rollback()
        print("Fail")
    finally:
        cursor.close()
        conn.close()

def insert_viewer(uid, email, nickname, street, city, state, zip_code, genres, joined_date, first, last, subscription):
    """
    Inserts a new viewer by adding a record to both the users and viewers tables.
    """
    conn = connect_db()
    cursor = conn.cursor()
    try:
        # Insert into users.
        cursor.execute("""
            INSERT INTO users (uid, email, nickname, street, city, state, zip, genres, joined_date)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (uid, email, nickname, street, city, state, zip_code, genres, joined_date))
        
        # Then insert into viewers.
        cursor.execute("""
            INSERT INTO viewers (uid, subscription, first_name, last_name)
            VALUES (%s, %s, %s, %s)
        """, (uid, subscription, first, last))
        
        conn.commit()
        print("Success")
    except mysql.connector.Error as err:
        conn.rollback()
        print("Fail")
    finally:
        cursor.close()
        conn.close()

def add_genre(uid, genre):
    """
    Adds a new genre to a user's genres field (semicolon separated).
    """
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
                    print("Success")
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
        print("Fail")
    finally:
        cursor.close()
        conn.close()

def delete_viewer(uid):
    """
    Deletes a viewer. This removes the record from viewers and (if needed) from users.
    """
    conn = connect_db()
    cursor = conn.cursor()
    try:
        # Delete from viewers (which cascades from users if defined accordingly).
        cursor.execute("DELETE FROM viewers WHERE uid = %s", (uid,))
        # Optionally, delete from users if the viewer record is separate.
        cursor.execute("DELETE FROM users WHERE uid = %s", (uid,))
        conn.commit()
        print("Success")
    except mysql.connector.Error as err:
        conn.rollback()
        print("Fail")
    finally:
        cursor.close()
        conn.close()

def insert_movie(rid, website_url):
    """
    Inserts a new movie record.
    """
    conn = connect_db()
    cursor = conn.cursor()
    try:
        cursor.execute("INSERT INTO movies (rid, website_url) VALUES (%s, %s)", (rid, website_url))
        conn.commit()
        print("Success")
    except mysql.connector.Error as err:
        conn.rollback()
        print("Fail")
    finally:
        cursor.close()
        conn.close()

def insert_session(sid, uid, rid, ep_num, initiate_at, leave_at, quality, device):
    """
    Inserts a new session record.
    """
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
    """
    Updates the title of a release.
    """
    conn = connect_db()
    cursor = conn.cursor()
    try:
        cursor.execute("UPDATE releases SET title = %s WHERE rid = %s", (title, rid))
        conn.commit()
        print("Success")
    except mysql.connector.Error as err:
        conn.rollback()
        print("Fail")
    finally:
        cursor.close()
        conn.close()

def list_releases(uid):
    """
    Lists all unique releases reviewed by a given viewer (uid) in ascending order by release title.
    Output format: rid, genre, title
    """
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
    """
    Lists the top N releases that have the most reviews.
    Output format: rid, title, reviewCount
    """
    conn = connect_db()
    cursor = conn.cursor()
    try:
        cursor.execute("""
            SELECT r.rid, r.title, COUNT(rev.rid) as reviewCount
            FROM releases r
            LEFT JOIN reviews rev ON r.rid = rev.rid
            GROUP BY r.rid, r.title
            ORDER BY reviewCount DESC, r.rid ASC
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
    """
    Given a session id, finds the release (and corresponding video) associated with that session.
    Output format: rid, release_title, genre, video_title, ep_num, length
    """
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
        print("Fail")
    finally:
        cursor.close()
        conn.close()

def active_viewer(n, start_date, end_date):
    """
    Lists all active viewers who have started at least n sessions between start_date and end_date.
    Output format: uid, first_name, last_name (from viewers)
    """
    conn = connect_db()
    cursor = conn.cursor()
    output_lines = []
    print(n, start_date, end_date)
    try:
        cursor.execute("""
            SELECT v.uid, v.first_name, v.last_name
            FROM viewers v
            JOIN sessions s ON v.uid = s.uid
            WHERE s.initiate_at BETWEEN %s AND %s
            GROUP BY v.uid, v.first_name, v.last_name
            HAVING COUNT(s.sid) >= %s
            ORDER BY v.uid ASC
        """, (start_date, end_date, n))
        results = cursor.fetchall()
        for row in results:
            output_lines.append(",".join(str(item) if item is not None else "NULL" for item in row))
    except mysql.connector.Error as err:
        print("Fail")
        return
    finally:
        cursor.close()
        conn.close()
    
    # Print exactly the result (no extra newline if empty).
    # sys.stdout.write("\n".join(output_lines))

def videos_viewed(rid):
    """
    For a given video (by its release id), counts the number of unique viewers that have started a session on it.
    Output format: rid, ep_num, title, length, COUNT
    If no sessions exist for the video, output "Fail".
    """
    conn = connect_db()
    cursor = conn.cursor()
    try:
        cursor.execute("""
            SELECT v.rid, v.ep_num, v.title, v.length, 
                   COUNT(DISTINCT s.uid) as viewer_count
            FROM videos v
            LEFT JOIN sessions s ON v.rid = s.rid AND v.ep_num = s.ep_num
            WHERE v.rid = %s
            GROUP BY v.rid, v.ep_num, v.title, v.length
            ORDER BY v.rid DESC, v.ep_num ASC
        """, (rid,))
        results = cursor.fetchall()
        if results:
            for row in results:
                print(",".join(str(item) if item is not None else "NULL" for item in row))
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
        # Expected: uid, email, nickname, street, city, state, zip, genres, joined_date, first_name, last_name, subscription
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
