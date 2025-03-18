import mysql.connector
from mysql.connector import Error
import pandas as pd
import numpy as np

class DatabaseHandler:
    """Class to handle database operations for cricket data"""
    
    def __init__(self, host="localhost", user="root", password="2003", database="cricketdata"):
        """Initialize with database connection parameters"""
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.connection = None
        self.connect()
    
    def connect(self):
        """Create a connection to MySQL database"""
        try:
            connection = mysql.connector.connect(
                host=self.host,
                user=self.user,
                password=self.password
            )
            
            # Create database if it doesn't exist
            if connection.is_connected():
                cursor = connection.cursor()
                cursor.execute(f"CREATE DATABASE IF NOT EXISTS {self.database}")
                cursor.close()
                connection.close()
                
                # Reconnect with the database specified
                connection = mysql.connector.connect(
                    host=self.host,
                    user=self.user,
                    password=self.password,
                    database=self.database
                )
                
                print(f"Connected to MySQL database '{self.database}'")
                self.connection = connection
                return True
        except Error as e:
            print(f"Error connecting to MySQL: {e}")
            return False
    
    def close_connection(self):
        """Close the database connection"""
        if self.connection and self.connection.is_connected():
            self.connection.close()
            print("MySQL connection closed")
    
    def create_table(self, table_name, df):
        """Create a table based on DataFrame columns"""
        if not self.connection or not self.connection.is_connected():
            print("Database connection is not established")
            return False
            
        try:
            cursor = self.connection.cursor()
            
            # Determine MySQL column types based on DataFrame dtypes
            column_defs = []
            for col_name, dtype in df.dtypes.items():
                # Map pandas dtypes to MySQL types
                if np.issubdtype(dtype, np.integer):
                    col_type = "INT"
                elif np.issubdtype(dtype, np.floating):
                    col_type = "FLOAT"
                elif np.issubdtype(dtype, np.datetime64):
                    col_type = "DATETIME"
                elif np.issubdtype(dtype, np.bool_):
                    col_type = "BOOLEAN"
                else:
                    col_type = "TEXT"  # Default to TEXT for strings and other types
                    
                # Clean column name (remove special characters)
                clean_col_name = ''.join(e if e.isalnum() else '_' for e in col_name)
                column_defs.append(f"`{clean_col_name}` {col_type}")
            
            # Create table query
            create_table_query = f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                {', '.join(column_defs)}
            )
            """
            
            cursor.execute(create_table_query)
            print(f"Table {table_name} created successfully")
            
            # Truncate table if it exists (clear existing data)
            cursor.execute(f"TRUNCATE TABLE {table_name}")
            self.connection.commit()
            
            cursor.close()
            return True
        except Error as e:
            print(f"Error creating table {table_name}: {e}")
            return False
    
    def insert_dataframe(self, table_name, df):
        """Insert DataFrame data into MySQL table"""
        if not self.connection or not self.connection.is_connected():
            print("Database connection is not established")
            return False
            
        try:
            cursor = self.connection.cursor()
            
            # Clean column names
            df.columns = [''.join(e if e.isalnum() else '_' for e in col) for col in df.columns]
            
            # Prepare column names and placeholders for SQL query
            columns = ', '.join([f'`{col}`' for col in df.columns])
            placeholders = ', '.join(['%s'] * len(df.columns))
            
            # Prepare insert query
            insert_query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
            
            # Convert DataFrame to list of tuples for insertion
            # Replace NaN values with None for SQL compatibility
            records = []
            for _, row in df.iterrows():
                record = []
                for val in row:
                    if pd.isna(val):
                        record.append(None)
                    else:
                        record.append(val)
                records.append(tuple(record))
            
            # Use executemany for better performance with large datasets
            batch_size = 1000  # Insert in batches to avoid memory issues
            for i in range(0, len(records), batch_size):
                batch = records[i:i+batch_size]
                cursor.executemany(insert_query, batch)
                self.connection.commit()
                print(f"Inserted batch {i//batch_size + 1}/{(len(records)//batch_size) + 1} into {table_name}")
            
            print(f"Data inserted successfully into {table_name}: {len(records)} rows")
            cursor.close()
            return True
        except Error as e:
            print(f"Error inserting data into {table_name}: {e}")
            return False
    
    def process_dataframes(self, dataframes_dict):
        """Process all DataFrames and store in MySQL tables"""
        if not self.connection or not self.connection.is_connected():
            if not self.connect():
                return False
        
        success = True
        for table_name, df in dataframes_dict.items():
            if df.empty:
                print(f"Skipping empty DataFrame {table_name}")
                continue
            
            # Create table
            if not self.create_table(table_name, df):
                success = False
                continue
            
            # Insert data
            if not self.insert_dataframe(table_name, df):
                success = False
        
        return success