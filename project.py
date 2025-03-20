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
        # Drop tables in order that respects dependencies
        tables = ["Session", "Review", "Video", "Movie", "TVShow", "`Release`", "Viewer"]
        for table in tables:
            cursor.execute(f"DROP TABLE IF EXISTS {table};")
        
        # Create tables as specified in the project write-up
        cursor.execute("""
            CREATE TABLE Viewer (
                uid INT PRIMARY KEY,
                email VARCHAR(255) UNIQUE NOT NULL,
                nickname VARCHAR(255),
                street VARCHAR(255),
                city VARCHAR(255),
                state VARCHAR(255),
                zip VARCHAR(255),
                genres VARCHAR(255),
                joined_date DATE,
                first VARCHAR(255),
                last VARCHAR(255),
                subscription VARCHAR(255)
            );
        """)
        
        cursor.execute("""
            CREATE TABLE `Release` (
                rid INT PRIMARY KEY,
                title VARCHAR(255) NOT NULL,
                genre VARCHAR(255),
                released_date DATE
            );
        """)
        
        cursor.execute("""
            CREATE TABLE TVShow (
                rid INT PRIMARY KEY,
                seasons INT,
                FOREIGN KEY (rid) REFERENCES `Release`(rid) ON DELETE CASCADE
            );
        """)
        
        cursor.execute("""
            CREATE TABLE Movie (
                rid INT PRIMARY KEY,
                website_url VARCHAR(255),
                FOREIGN KEY (rid) REFERENCES `Release`(rid) ON DELETE CASCADE
            );
        """)
        
        cursor.execute("""
            CREATE TABLE Video (
                rid INT,
                ep_num INT,
                title VARCHAR(255),
                length INT,
                PRIMARY KEY (rid, ep_num),
                FOREIGN KEY (rid) REFERENCES `Release`(rid) ON DELETE CASCADE
            );
        """)
        
        cursor.execute("""
            CREATE TABLE Review (
                uid INT,
                rid INT,
                rating DECIMAL(3,1),
                comment TEXT,
                created_at DATETIME,
                PRIMARY KEY (uid, rid),
                FOREIGN KEY (uid) REFERENCES Viewer(uid) ON DELETE CASCADE,
                FOREIGN KEY (rid) REFERENCES `Release`(rid) ON DELETE CASCADE
            );
        """)
        
        cursor.execute("""
            CREATE TABLE Session (
                sid INT PRIMARY KEY,
                uid INT,
                rid INT,
                ep_num INT,
                initiate_at DATETIME,
                leave_at DATETIME,
                quality VARCHAR(255),
                device VARCHAR(255),
                FOREIGN KEY (uid) REFERENCES Viewer(uid) ON DELETE CASCADE,
                FOREIGN KEY (rid, ep_num) REFERENCES Video(rid, ep_num) ON DELETE CASCADE
            );
        """)
        
        cursor.execute("SET FOREIGN_KEY_CHECKS = 1;")
        
        # Import CSV files (assumes file names exactly match table names)
        # Note: For the table Release, the CSV file should be named Release.csv (without backticks)
        tables_for_import = ["Viewer", "Release", "TVShow", "Movie", "Video", "Review", "Session"]
        for table in tables_for_import:
            csv_path = os.path.join(folder_name, f"{table}.csv")
            if not os.path.exists(csv_path):
                continue
            with open(csv_path, 'r', newline='') as csvfile:
                csv_reader = csv.reader(csvfile)
                for row in csv_reader:
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
    conn = connect_db()
    cursor = conn.cursor()
    try:
        cursor.execute("""
            INSERT INTO Viewer (uid, email, nickname, street, city, state, zip, genres, joined_date, first, last, subscription)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (uid, email, nickname, street, city, state, zip_code, genres, joined_date, first, last, subscription))
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
        cursor.execute("SELECT genres FROM Viewer WHERE uid = %s", (uid,))
        result = cursor.fetchone()
        if result is not None:
            current_genres = result[0]
            if current_genres:
                genres_list = [g.strip() for g in current_genres.split(';')]
                if genre.lower() in [g.lower() for g in genres_list]:
                    print("Success")
                    return
                else:
                    updated_genres = current_genres + ";" + genre
            else:
                updated_genres = genre
            cursor.execute("UPDATE Viewer SET genres = %s WHERE uid = %s", (updated_genres, uid))
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
    conn = connect_db()
    cursor = conn.cursor()
    try:
        cursor.execute("DELETE FROM Viewer WHERE uid = %s", (uid,))
        conn.commit()
        print("Success")
    except mysql.connector.Error as err:
        conn.rollback()
        print("Fail")
    finally:
        cursor.close()
        conn.close()

def insert_movie(rid, website_url):
    conn = connect_db()
    cursor = conn.cursor()
    try:
        cursor.execute("INSERT INTO Movie (rid, website_url) VALUES (%s, %s)", (rid, website_url))
        conn.commit()
        print("Success")
    except mysql.connector.Error as err:
        conn.rollback()
        print("Fail")
    finally:
        cursor.close()
        conn.close()

def insert_session(sid, uid, rid, ep_num, initiate_at, leave_at, quality, device):
    conn = connect_db()
    cursor = conn.cursor()
    try:
        cursor.execute("""
            INSERT INTO Session (sid, uid, rid, ep_num, initiate_at, leave_at, quality, device)
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
        cursor.execute("UPDATE `Release` SET title = %s WHERE rid = %s", (title, rid))
        conn.commit()
        print("Success")
    except mysql.connector.Error as err:
        conn.rollback()
        print("Fail")
    finally:
        cursor.close()
        conn.close()

def list_releases(uid):
    conn = connect_db()
    cursor = conn.cursor()
    try:
        cursor.execute("""
            SELECT DISTINCT r.rid, r.genre, r.title
            FROM `Release` r
            JOIN Review rev ON r.rid = rev.rid
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
            FROM `Release` r
            LEFT JOIN Review rev ON r.rid = rev.rid
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
    conn = connect_db()
    cursor = conn.cursor()
    try:
        cursor.execute("""
            SELECT r.rid, r.title as release_title, r.genre, v.title as video_title, v.ep_num, v.length
            FROM Session s
            JOIN `Release` r ON s.rid = r.rid
            JOIN Video v ON s.rid = v.rid AND s.ep_num = v.ep_num
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
    conn = connect_db()
    cursor = conn.cursor()
    try:
        cursor.execute("""
            SELECT v.uid, v.first, v.last
            FROM Viewer v
            JOIN Session s ON v.uid = s.uid
            WHERE s.initiate_at BETWEEN %s AND %s
            GROUP BY v.uid, v.first, v.last
            HAVING COUNT(s.sid) >= %s
            ORDER BY v.uid ASC
        """, (start_date, end_date, n))
        results = cursor.fetchall()
        for row in results:
            print(",".join(str(item) if item is not None else "NULL" for item in row))
    except mysql.connector.Error as err:
        pass
    finally:
        cursor.close()
        conn.close()

def videos_viewed(rid):
    conn = connect_db()
    cursor = conn.cursor()
    try:
        cursor.execute("""
            SELECT v.rid, v.ep_num, v.title, v.length, 
                   COUNT(DISTINCT s.uid) as viewer_count
            FROM Video v
            LEFT JOIN Session s ON v.rid = s.rid AND v.ep_num = s.ep_num
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
