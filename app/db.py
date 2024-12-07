# db.py
import pymysql
from pymongo import MongoClient

# MySQL Connection
def connect_mysql():
    connection = pymysql.connect(
        host='localhost',
        user='root',          # Your MySQL username
        password='chowdhury5039',  # Your new MySQL password
        db='chatdb',          # Name of your database
        charset='utf8mb4',
        cursorclass=pymysql.cursors.DictCursor
    )
    return connection


def connect_mongo():
    # MongoDB Atlas connection setup
    mongo_username = "ziaurchowdhury"  # Your MongoDB username
    mongo_password = "chowdhury5039"    # Your MongoDB password
    mongo_cluster = "chatdb-cluster.maj3x.mongodb.net"  # Your cluster address
    mongo_db_name = "chatdb"             # The name of your MongoDB database

    # Construct the connection URI
    connection_string = f"mongodb+srv://{mongo_username}:{mongo_password}@{mongo_cluster}/{mongo_db_name}?retryWrites=true&w=majority"

    client = MongoClient(connection_string)
    return client[mongo_db_name]


def test_mysql_connection():
    try:
        conn = connect_mysql()
        with conn.cursor() as cursor:
            # Example SQL operation: Creating a table if it doesn't exist
            cursor.execute(
                "CREATE TABLE IF NOT EXISTS messages (id INT AUTO_INCREMENT PRIMARY KEY, content VARCHAR(255))")
            conn.commit()
            print("Connected to MySQL Database")
    except Exception as e:
        print("MySQL Connection Error:", e)
    finally:
        conn.close()


def test_mongo_connection():
    try:
        db = connect_mongo()
        print("Connected to MongoDB Database:", db.name)

        # Example CRUD operation in MongoDB
        collection = db['messages']  # Change this to your collection name
        # Create
        collection.insert_one({"content": "Hello, MongoDB!"})
        print("Inserted a message into MongoDB")

        # Read
        mongo_records = list(collection.find())
        print("MongoDB Records:", mongo_records)

    except Exception as e:
        print("MongoDB Connection Error:", e)

# Example usage
if __name__ == "__main__":
    # Connect to MongoDB
    db = connect_mongo()
    print("Connected to MongoDB Database:", db.name)
