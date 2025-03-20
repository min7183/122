import sys
import os
import csv
import mysql.connector

def connect_db():
    return mysql.connector.connect(user='test', password='password', database='cs122a')

def import_data(folder_name):
    # Connect to database
    conn = connect_db()
    cursor = conn.cursor()
    
    try:
        # Clear existing tables and create new ones
        cursor.execute("SET FOREIGN_KEY_CHECKS = 0;")
        
        # Drop existing tables in the correct order to handle dependencies
        tables = ["Viewer", "Release", "TVShow", "Movie", "Video", "Review", "Session"]

        for table in tables:
            cursor.execute(f"DROP TABLE IF EXISTS {table};")
        
        # Create tables in order of dependencies
        cursor.execute("""
            CREATE TABLE Users (
                uid INT PRIMARY KEY,
                email VARCHAR(255) NOT NULL,
                nickname VARCHAR(255),
                street VARCHAR(255),
                city VARCHAR(255),
                state VARCHAR(255),
                zip VARCHAR(255),
                genres VARCHAR(255),
                joined_date DATE
            );
        """)
        
        cursor.execute("""
            CREATE TABLE Viewers (
                uid INT PRIMARY KEY,
                first VARCHAR(255),
                last VARCHAR(255),
                subscription VARCHAR(255),
                FOREIGN KEY (uid) REFERENCES Users(uid) ON DELETE CASCADE
            );
        """)
        
        cursor.execute("""
            CREATE TABLE Producers (
                uid INT PRIMARY KEY,
                company VARCHAR(255),
                bio TEXT,
                FOREIGN KEY (uid) REFERENCES Users(uid) ON DELETE CASCADE
            );
        """)
        
        cursor.execute("""
            CREATE TABLE Releases (
                rid INT PRIMARY KEY,
                title VARCHAR(255) NOT NULL,
                genre VARCHAR(255),
                release_date DATE
            );
        """)
        
        cursor.execute("""
            CREATE TABLE Movies (
                rid INT PRIMARY KEY,
                website_url VARCHAR(255),
                FOREIGN KEY (rid) REFERENCES Releases(rid) ON DELETE CASCADE
            );
        """)
        
        cursor.execute("""
            CREATE TABLE Series (
                rid INT PRIMARY KEY,
                introduction TEXT,
                FOREIGN KEY (rid) REFERENCES Releases(rid) ON DELETE CASCADE
            );
        """)
        
        cursor.execute("""
            CREATE TABLE Videos (
                rid INT,
                ep_num INT,
                title VARCHAR(255),
                length INT,
                PRIMARY KEY (rid, ep_num),
                FOREIGN KEY (rid) REFERENCES Releases(rid) ON DELETE CASCADE
            );
        """)
        
        cursor.execute("""
            CREATE TABLE Reviews (
                rvid INT PRIMARY KEY,
                uid INT,
                rid INT,
                rating INT,
                body TEXT,
                posted_at DATETIME,
                FOREIGN KEY (uid) REFERENCES Users(uid) ON DELETE CASCADE,
                FOREIGN KEY (rid) REFERENCES Releases(rid) ON DELETE CASCADE
            );
        """)
        
        cursor.execute("""
            CREATE TABLE Sessions (
                sid INT PRIMARY KEY,
                uid INT,
                quality VARCHAR(50),
                device VARCHAR(50),
                initiate_at DATETIME,
                leave_at DATETIME,
                FOREIGN KEY (uid) REFERENCES Viewers(uid) ON DELETE CASCADE
            );
        """)
        
        cursor.execute("""
            CREATE TABLE SessionOn (
                sid INT,
                rid INT,
                ep_num INT,
                PRIMARY KEY (sid, rid, ep_num),
                FOREIGN KEY (sid) REFERENCES Sessions(sid) ON DELETE CASCADE,
                FOREIGN KEY (rid, ep_num) REFERENCES Videos(rid, ep_num) ON DELETE CASCADE
            );
        """)
        
        cursor.execute("SET FOREIGN_KEY_CHECKS = 1;")
        
        # Import data from CSV files
        for table in tables:
            csv_path = os.path.join(folder_name, f"{table}.csv")
            
            # Check if file exists
            if not os.path.exists(csv_path):
                continue
            
            with open(csv_path, 'r') as csvfile:
                csv_reader = csv.reader(csvfile)
                for row in csv_reader:
                    # Replace empty strings with None
                    row = [None if field == '' else field for field in row]
                    
                    # Generate placeholders for SQL query
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
        # Insert into Users table first
        cursor.execute("""
            INSERT INTO Users (uid, email, nickname, street, city, state, zip, genres, joined_date)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (uid, email, nickname, street, city, state, zip_code, genres, joined_date))
        
        # Then insert into Viewers table
        cursor.execute("""
            INSERT INTO Viewers (uid, first, last, subscription)
            VALUES (%s, %s, %s, %s)
        """, (uid, first, last, subscription))
        
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
        cursor.execute("SELECT genres FROM Users WHERE uid = %s", (uid,))
        result = cursor.fetchone()
        if result:
            current_genres = result[0]
            if current_genres:
                # Check if genre already exists
                genres_list = current_genres.split(';')
                if genre.lower() not in [g.lower() for g in genres_list]:
                    updated_genres = current_genres + ";" + genre
                else:
                    # Genre already exists, no update needed
                    print("Success")
                    return
            else:
                updated_genres = genre
                
            cursor.execute("UPDATE Users SET genres = %s WHERE uid = %s", (updated_genres, uid))
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
        # First delete from Viewers table (child)
        cursor.execute("DELETE FROM Viewers WHERE uid = %s", (uid,))
        
        # Then delete from Users table (parent)
        cursor.execute("DELETE FROM Users WHERE uid = %s", (uid,))
        
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
        cursor.execute("INSERT INTO Movies (rid, website_url) VALUES (%s, %s)", (rid, website_url))
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
        # Insert into Sessions table
        cursor.execute("""
            INSERT INTO Sessions (sid, uid, quality, device, initiate_at, leave_at)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (sid, uid, quality, device, initiate_at, leave_at))
        
        # Insert into SessionOn relation
        cursor.execute("""
            INSERT INTO SessionOn (sid, rid, ep_num)
            VALUES (%s, %s, %s)
        """, (sid, rid, ep_num))
        
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
        cursor.execute("UPDATE Releases SET title = %s WHERE rid = %s", (title, rid))
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
            FROM Releases r
            JOIN Reviews rev ON r.rid = rev.rid
            WHERE rev.uid = %s
            ORDER BY r.title ASC
        """, (uid,))
        
        results = cursor.fetchall()
        if results:
            for row in results:
                print(",".join(str(item) if item is not None else "NULL" for item in row))
    except mysql.connector.Error as err:
        print("Fail")
    finally:
        cursor.close()
        conn.close()

def popular_release(n):
    conn = connect_db()
    cursor = conn.cursor()
    try:
        cursor.execute("""
            SELECT r.rid, r.title, COUNT(rev.rid) as reviewCount
            FROM Releases r
            LEFT JOIN Reviews rev ON r.rid = rev.rid
            GROUP BY r.rid, r.title
            ORDER BY reviewCount DESC, r.rid DESC
            LIMIT %s
        """, (n,))
        
        results = cursor.fetchall()
        if results:
            for row in results:
                print(",".join(str(item) if item is not None else "NULL" for item in row))
    except mysql.connector.Error as err:
        print("Fail")
    finally:
        cursor.close()
        conn.close()

def release_title(sid):
    conn = connect_db()
    cursor = conn.cursor()
    try:
        cursor.execute("""
            SELECT r.rid, r.title as release_title, r.genre, v.title as video_title, v.ep_num, v.length
            FROM Sessions s
            JOIN SessionOn so ON s.sid = so.sid
            JOIN Releases r ON so.rid = r.rid
            JOIN Videos v ON so.rid = v.rid AND so.ep_num = v.ep_num
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
            FROM Viewers v
            JOIN Sessions s ON v.uid = s.uid
            WHERE s.initiate_at BETWEEN %s AND %s
            GROUP BY v.uid, v.first, v.last
            HAVING COUNT(s.sid) >= %s
            ORDER BY v.uid ASC
        """, (start_date, end_date, n))
        
        results = cursor.fetchall()
        if results:
            for row in results:
                print(",".join(str(item) if item is not None else "NULL" for item in row))
    except mysql.connector.Error as err:
        print("Fail")
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
            FROM Videos v
            LEFT JOIN SessionOn so ON v.rid = so.rid AND v.ep_num = so.ep_num
            LEFT JOIN Sessions s ON so.sid = s.sid
            WHERE v.rid = %s
            GROUP BY v.rid, v.ep_num, v.title, v.length
            ORDER BY v.rid DESC
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