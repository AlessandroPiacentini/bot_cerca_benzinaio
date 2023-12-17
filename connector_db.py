import mysql.connector

class DatabaseSingleton:
    _instance = None

    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            cls._instance = super(DatabaseSingleton, cls).__new__(cls, *args, **kwargs)
            cls._instance.connection = None
        return cls._instance

    def connect(self, host, user, password, database):
        if not self.connection:
            try:
                self.connection = mysql.connector.connect(
                    host=host,
                    user=user,
                    password=password,
                    database=database
                )
                print("Connected to the database")
            except mysql.connector.Error as err:
                print(f"Error: {err}")
        else:
            print("Already connected to the database")

    def disconnect(self):
        if self.connection:
            self.connection.close()
            print("Disconnected from the database")
            self.connection = None
        else:
            print("Not connected to any database")

    def execute_query(self, query, data=None):
        cursor = self.connection.cursor()
        try:
            if data:
                cursor.execute(query, data)
            else:
                cursor.execute(query)
            result = cursor.fetchall()
            self.connection.commit()
            return result
        except mysql.connector.Error as err:
            print(f"Error: {err}")
            self.connection.rollback()
        finally:
            cursor.close()
    
    def create_table(self, table_name, columns):
        query = f"CREATE TABLE IF NOT EXISTS {table_name} ({', '.join(columns)})"
        self.execute_query(query)
        print(f"Table {table_name} created successfully")