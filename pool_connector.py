import mysql.connector
from mysql.connector import pooling
from concurrent.futures import ThreadPoolExecutor
import json

batch_size = 1000  # Number of records to insert in each batch
num_threads = 4 


def execute_query(batch):
    print(batch)

class MySQLConnectionManager:
    def __init__(self, host, port,  user, password,  database, pool_size):
        self.dbconfig = {
            "host": host,
            "port": port,
            "user": user,
            "password": password,
            "database": database,
        }
        self.pool = pooling.MySQLConnectionPool(pool_name="mypool", pool_size=pool_size, **self.dbconfig)

    def get_connection(self):
        try:
            conn = self.pool.get_connection()

            if conn.is_connected():
                print("Connection was gotten from pool")
                return conn

        except mysql.connector.Error as e:
            print(f"Error: {e}")

        return None

    def execute_query(self, connection, query):
        if connection is not None:
            try:
                cursor = connection.cursor()
                cursor.execute(query)
                return cursor.fetchall()
            except mysql.connector.Error as e:
                print(f"Error: {e}")
            finally:
                cursor.close()

        return None

    def close_connection(self, connection):
        if connection is not None and connection.is_connected():
            print("connection was closed")
            connection.close()

# # Example usage:
# manager = MySQLConnectionManager("localhost", "3307",  "root", "morijin", "catalog", pool_size=32)

# connections = [manager.get_connection() for _ in range(30)]


# Read and process the JSON data
with open("product_catalog.json", "r") as json_file:
    data = json.load(json_file)

batches = [data[i:i+batch_size] for i in range(0, len(data), batch_size)]


with ThreadPoolExecutor(max_workers=num_threads) as executor:
    print('*************We started a batch work ***********************')
    executor.map(execute_query, batches)


# for i in range(32):
#     result = manager.execute_query(connections[i], query)

#     if result:
#         for row in result:
#             print(row)

# for conn in connections:
#     manager.close_connection(conn)

# for conn in connections:
#     manager.pool.add_connection(conn)
