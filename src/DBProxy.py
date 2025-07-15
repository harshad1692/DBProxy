from abc import ABC, abstractmethod
import sqlite3
import psycopg2
import pymysql
import pymongo
import redis
from typing import Any, Optional, List, Dict

# Abstract base class for database connections
class Database(ABC):
    @abstractmethod
    def connect(self):
        pass
    
    @abstractmethod
    def execute_query(self, query: str, params: Optional[Any] = None) -> List[Any]:
        pass
    
    @abstractmethod
    def execute_special(self, operation: str, params: Optional[Dict] = None) -> Any:
        pass
    
    @abstractmethod
    def close(self):
        pass

# SQLite implementation
class SQLiteDatabase(Database):
    def __init__(self, db_name: str):
        self.db_name = db_name
        self.conn = None
    
    def connect(self):
        self.conn = sqlite3.connect(self.db_name)
        return self.conn
    
    def execute_query(self, query: str, params: Optional[tuple] = None) -> List[Any]:
        cursor = self.conn.cursor()
        try:
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            self.conn.commit()
            return cursor.fetchall() if cursor.description else []
        finally:
            cursor.close()
    
    def execute_special(self, operation: str, params: Optional[Dict] = None) -> Any:
        cursor = self.conn.cursor()
        try:
            if operation == "transaction":
                queries = params.get("queries", [])
                for query, q_params in queries:
                    cursor.execute(query, q_params or ())
                self.conn.commit()
                return ["Transaction committed"]
            elif operation == "batch_insert":
                table = params.get("table")
                values = params.get("values", [])
                query = f"INSERT INTO {table} VALUES ({','.join(['?' for _ in values[0]])})"
                cursor.executemany(query, values)
                self.conn.commit()
                return [cursor.rowcount]
            else:
                raise ValueError("Unsupported SQLite operation")
        finally:
            cursor.close()
    
    def close(self):
        if self.conn:
            self.conn.close()

# PostgreSQL implementation
class PostgreSQLDatabase(Database):
    def __init__(self, dbname: str, user: str, password: str, host: str, port: str):
        self.db_config = {
            "dbname": dbname,
            "user": user,
            "password": password,
            "host": host,
            "port": port
        }
        self.conn = None
    
    def connect(self):
        self.conn = psycopg2.connect(**self.db_config)
        return self.conn
    
    def execute_query(self, query: str, params: Optional[tuple] = None) -> List[Any]:
        cursor = self.conn.cursor()
        try:
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            self.conn.commit()
            return cursor.fetchall() if cursor.description else []
        finally:
            cursor.close()
    
    def execute_special(self, operation: str, params: Optional[Dict] = None) -> Any:
        cursor = self.conn.cursor()
        try:
            if operation == "transaction":
                queries = params.get("queries", [])
                for query, q_params in queries:
                    cursor.execute(query, q_params or ())
                self.conn.commit()
                return ["Transaction committed"]
            elif operation == "batch_insert":
                table = params.get("table")
                values = params.get("values", [])
                query = f"INSERT INTO {table} VALUES ({','.join(['%s' for _ in values[0]])})"
                cursor.executemany(query, values)
                self.conn.commit()
                return [cursor.rowcount]
            else:
                raise ValueError("Unsupported PostgreSQL operation")
        finally:
            cursor.close()
    
    def close(self):
        if self.conn:
            self.conn.close()

# MySQL implementation
class MySQLDatabase(Database):
    def __init__(self, host: str, user: str, password: str, database: str, port: int = 3306):
        self.db_config = {
            "host": host,
            "user": user,
            "password": password,
            "database": database,
            "port": port
        }
        self.conn = None
    
    def connect(self):
        self.conn = pymysql.connect(**self.db_config)
        return self.conn
    
    def execute_query(self, query: str, params: Optional[tuple] = None) -> List[Any]:
        cursor = self.conn.cursor()
        try:
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            self.conn.commit()
            return cursor.fetchall() if cursor.description else []
        finally:
            cursor.close()
    
    def execute_special(self, operation: str, params: Optional[Dict] = None) -> Any:
        cursor = self.conn.cursor()
        try:
            if operation == "transaction":
                queries = params.get("queries", [])
                for query, q_params in queries:
                    cursor.execute(query, q_params or ())
                self.conn.commit()
                return ["Transaction committed"]
            elif operation == "batch_insert":
                table = params.get("table")
                values = params.get("values", [])
                query = f"INSERT INTO {table} VALUES ({','.join(['%s' for _ in values[0]])})"
                cursor.executemany(query, values)
                self.conn.commit()
                return [cursor.rowcount]
            else:
                raise ValueError("Unsupported MySQL operation")
        finally:
            cursor.close()
    
    def close(self):
        if self.conn:
            self.conn.close()

# MongoDB implementation
class MongoDBDatabase(Database):
    def __init__(self, host: str, port: int, db_name: str):
        self.client = None
        self.db_name = db_name
        self.host = host
        self.port = port
    
    def connect(self):
        self.client = pymongo.MongoClient(self.host, self.port)
        return self.client[self.db_name]
    
    def execute_query(self, query: str, params: Optional[Dict] = None) -> List[Any]:
        db = self.client[self.db_name]
        try:
            if query.startswith("insert"):
                collection = params.get("collection")
                data = params.get("data", {})
                result = db[collection].insert_one(data)
                return [result.inserted_id]
            elif query.startswith("find"):
                collection = params.get("collection")
                filter = params.get("filter", {})
                return list(db[collection].find(filter))
            elif query.startswith("update"):
                collection = params.get("collection")
                filter = params.get("filter", {})
                update = params.get("update", {})
                result = db[collection].update_many(filter, {"$set": update})
                return [result.modified_count]
            elif query.startswith("delete"):
                collection = params.get("collection")
                filter = params.get("filter", {})
                result = db[collection].delete_many(filter)
                return [result.deleted_count]
            else:
                raise ValueError("Unsupported MongoDB operation")
        finally:
            pass
    
    def execute_special(self, operation: str, params: Optional[Dict] = None) -> Any:
        db = self.client[self.db_name]
        try:
            if operation == "aggregate":
                collection = params.get("collection")
                pipeline = params.get("pipeline", [])
                return list(db[collection].aggregate(pipeline))
            elif operation == "batch_insert":
                collection = params.get("collection")
                documents = params.get("documents", [])
                result = db[collection].insert_many(documents)
                return list(result.inserted_ids)
            else:
                raise ValueError("Unsupported MongoDB operation")
        finally:
            pass
    
    def close(self):
        if self.client:
            self.client.close()

# Redis implementation
class RedisDatabase(Database):
    def __init__(self, host: str, port: int, db: int = 0):
        self.db_config = {
            "host": host,
            "port": port,
            "db": db
        }
        self.conn = None
        self.pubsub = None
    
    def connect(self):
        self.conn = redis.Redis(**self.db_config)
        self.pubsub = self.conn.pubsub()
        return self.conn
    
    def execute_query(self, query: str, params: Optional[Dict] = None) -> List[Any]:
        try:
            if query.startswith("set"):
                key = params.get("key")
                value = params.get("value")
                self.conn.set(key, value)
                return ["OK"]
            elif query.startswith("get"):
                key = params.get("key")
                return [self.conn.get(key)]
            elif query.startswith("delete"):
                key = params.get("key")
                result = self.conn.delete(key)
                return [result]
            else:
                raise ValueError("Unsupported Redis operation")
        finally:
            pass
    
    def execute_special(self, operation: str, params: Optional[Dict] = None) -> Any:
        try:
            if operation == "subscribe":
                channel = params.get("channel")
                self.pubsub.subscribe(channel)
                return ["Subscribed to " + channel]
            elif operation == "publish":
                channel = params.get("channel")
                message = params.get("message")
                result = self.conn.publish(channel, message)
                return [result]
            else:
                raise ValueError("Unsupported Redis operation")
        finally:
            pass
    
    def close(self):
        if self.conn:
            self.conn.close()

# DB Proxy class
class DBProxy:
    def __init__(self):
        self.db = None
    
    def connect_to_db(self, db_type: str, **kwargs):
        db_type = db_type.lower()
        if db_type == "sqlite":
            self.db = SQLiteDatabase(**kwargs)
        elif db_type == "postgresql":
            self.db = PostgreSQLDatabase(**kwargs)
        elif db_type == "mysql":
            self.db = MySQLDatabase(**kwargs)
        elif db_type == "mongodb":
            self.db = MongoDBDatabase(**kwargs)
        elif db_type == "redis":
            self.db = RedisDatabase(**kwargs)
        else:
            raise ValueError(f"Unsupported database type: {db_type}")
        return self.db.connect()
    
    def execute(self, query: str, params: Optional[Dict] = None) -> List[Any]:
        if not self.db:
            raise Exception("No database connected")
        return self.db.execute_query(query, params)
    
    def execute_special(self, operation: str, params: Optional[Dict] = None) -> Any:
        if not self.db:
            raise Exception("No database connected")
        return self.db.execute_special(operation, params)
    
    def close(self):
        if self.db:
            self.db.close()

# Example usage
if __name__ == "__main__":
    proxy = DBProxy()
    
    # SQLite: Transaction and Batch Insert
    proxy.connect_to_db(db_type="sqlite", db_name="example.db")
    proxy.execute("CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY, name TEXT)")
    proxy.execute_special("transaction", {
        "queries": [
            ("INSERT INTO users (name) VALUES (?)", ("Alice",)),
            ("INSERT INTO users (name) VALUES (?)", ("Bob",))
        ]
    })
    proxy.execute_special("batch_insert", {
        "table": "users",
        "values": [(None, "Charlie"), (None, "Dave")]
    })
    results = proxy.execute("SELECT * FROM users")
    print("SQLite Results:", results)
    proxy.close()
    
    # PostgreSQL: Transaction
    proxy.connect_to_db(
        db_type="postgresql",
        dbname="mydb",
        user="myuser",
        password="mypassword",
        host="localhost",
        port="5432"
    )
    proxy.execute("CREATE TABLE IF NOT EXISTS logs (id SERIAL PRIMARY KEY, message TEXT)")
    proxy.execute_special("transaction", {
        "queries": [
            ("INSERT INTO logs (message) VALUES (%s)", ("Log 1",)),
            ("INSERT INTO logs (message) VALUES (%s)", ("Log 2",))
        ]
    })
    results = proxy.execute("SELECT * FROM logs")
    print("PostgreSQL Results:", results)
    proxy.close()
    
    # MySQL: Batch Insert
    proxy.connect_to_db(
        db_type="mysql",
        host="localhost",
        user="myuser",
        password="mypassword",
        database="mydb"
    )
    proxy.execute("CREATE TABLE IF NOT EXISTS products (id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(255))")
    proxy.execute_special("batch_insert", {
        "table": "products",
        "values": [(None, "Laptop"), (None, "Phone")]
    })
    results = proxy.execute("SELECT * FROM products")
    print("MySQL Results:", results)
    proxy.close()
    
    # MongoDB: Aggregation
    proxy.connect_to_db(db_type="mongodb", host="localhost", port=27017, db_name="mydb")
    proxy.execute("insert", {"collection": "users", "data": {"name": "Bob", "age": 30}})
    proxy.execute("insert", {"collection": "users", "data": {"name": "Alice", "age": 25}})
    results = proxy.execute_special("aggregate", {
        "collection": "users",
        "pipeline": [
            {"$match": {"age": {"$gte": 25}}},
            {"$group": {"_id": None, "avg_age": {"$avg": "$age"}}}
        ]
    })
    print("MongoDB Aggregation Result:", results)
    proxy.close()
    
    # Redis: Pub/Sub
    proxy.connect_to_db(db_type="redis", host="localhost", port=6379, db=0)
    proxy.execute_special("subscribe", {"channel": "mychannel"})
    proxy.execute_special("publish", {"channel": "mychannel", "message": "Hello, Redis!"})
    print("Redis Pub/Sub Setup")
    proxy.close()