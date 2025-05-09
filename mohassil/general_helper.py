from __future__ import annotations

import csv
from collections import defaultdict

import mysql.connector
import pyodbc
from sshtunnel import SSHTunnelForwarder

try:
    import pymssql
    PYMSSQL_AVAILABLE = True
except ImportError:
    PYMSSQL_AVAILABLE = False

# Modify the test_connection function to use pymssql
def test_connection(conn_details):
    """Test database connection with the provided details."""
    try:
        tunnel = None
        db_type = conn_details.get('db_type')
        host = conn_details.get('host')
        user = conn_details.get('user')
        password = conn_details.get('password')
        database = conn_details.get('database')
        ssh_host = conn_details.get('ssh_host')
        ssh_user = conn_details.get('ssh_user')
        ssh_password = conn_details.get('ssh_password')
        
        # Use the same port settings as in 1_branches.py
        sql_server_port = 1433
        mysql_port = 3306
        port = sql_server_port if db_type == "sqlserver" else mysql_port
        
        # Set up SSH tunnel if needed
        if ssh_host and ssh_user:
            tunnel = SSHTunnelForwarder(
                (ssh_host, 22),
                ssh_username=ssh_user,
                ssh_password=ssh_password if ssh_password else None,
                ssh_pkey=False,
                host_pkey_directories=[],
                remote_bind_address=(host, port)
            )
            tunnel.start()
            host = "127.0.0.1"
            port = tunnel.local_bind_port
        
        # Connect to database using the appropriate driver
        if db_type == "mysql":
            connection = mysql.connector.connect(
                host=host,
                user=user,
                password=password,
                database=database,
                port=port
            )
        elif db_type == "sqlserver":
            # Try pymssql first if available
            if PYMSSQL_AVAILABLE:
                connection = pymssql.connect(
                    server=host,
                    user=user,
                    password=password,
                    database=database,
                    port=port
                )
            else:
                # Fall back to pyodbc if pymssql is not available
                connection_string = (
                    f"DRIVER={{ODBC Driver 17 for SQL Server}};"
                    f"SERVER={host},{port};"
                    f"DATABASE={database};"
                    f"UID={user};"
                    f"PWD={password}"
                )
                connection = pyodbc.connect(connection_string)
        else:
            raise ValueError("Unsupported database type. Use 'mysql' or 'sqlserver'.")
        
        print(f"Connected to {database} ({db_type})")
        return connection, tunnel
        
    except Exception as e:
        print(f"Connection Error: {e}")
        if tunnel:
            tunnel.stop()
        return None

def get_db_connection_details(prompt_message):
    """
    Prompt the user for database connection details.
    
    Args:
        prompt_message (str): Message to display when prompting for details
        
    Returns:
        dict: Dictionary containing connection details
    """
    import os
    from dotenv import load_dotenv
    
    # Load environment variables from .env file
    load_dotenv()
    
    print(prompt_message)
    
    db_type = input("Database type (mysql/sqlserver) [sqlserver]: ").strip() or "sqlserver"
    
    # Get values from environment variables with empty defaults
    if db_type == "sqlserver":
        default_host = os.getenv("SQLSERVER_HOST", "")
        default_user = os.getenv("SQLSERVER_USER", "")
        default_password = os.getenv("SQLSERVER_PASSWORD", "")
        default_database = os.getenv("SQLSERVER_DATABASE", "")
    else:
        default_host = os.getenv("MYSQL_HOST", "")
        default_user = os.getenv("MYSQL_USER", "")
        default_password = os.getenv("MYSQL_PASSWORD", "")
        default_database = os.getenv("MYSQL_DATABASE", "")
    
    # SSH defaults
    default_ssh_host = os.getenv("SSH_HOST", "")
    default_ssh_user = os.getenv("SSH_USER", "")
    default_ssh_password = os.getenv("SSH_PASSWORD", "")
    
    # Prompt for connection details
    host = input(f"Host [{default_host}]: ").strip() or default_host
    user = input(f"Username [{default_user}]: ").strip() or default_user
    password = input(f"Password: ").strip() or default_password
    database = input(f"Database [{default_database}]: ").strip() or default_database
    
    # SSH tunnel details
    use_ssh = input("Use SSH tunnel? (y/n) [y]: ").strip().lower() or "y"
    if use_ssh == "y":
        ssh_host = input(f"SSH host [{default_ssh_host}]: ").strip() or default_ssh_host
        ssh_user = input(f"SSH username [{default_ssh_user}]: ").strip() or default_ssh_user
        ssh_password = input(f"SSH password: ").strip() or default_ssh_password
    else:
        ssh_host = ssh_user = ssh_password = None
    
    return {
        "db_type": db_type,
        "host": host,
        "user": user,
        "password": password,
        "database": database,
        "ssh_host": ssh_host,
        "ssh_user": ssh_user,
        "ssh_password": ssh_password
    }

def load_migration_config(file_path):
    """
    Reads the migrations configuration CSV and returns mappings grouped by `migration_name`.

    Args:
        file_path (str): Path to the CSV file.

    Returns:
        dict: A dictionary where keys are migration names and values are lists of column mappings.
        Example:
            {
                "clients": [
                    {"source_table": "source_clients_table", "source_column": "client_id",
                     "target_table": "target_clients_table", "target_column": "id"},
                    {"source_table": "source_clients_table", "source_column": "client_name",
                     "target_table": "target_clients_table", "target_column": "name"}
                ],
                "officers": [
                    {"source_table": "source_officers_table", "source_column": "officer_id",
                     "target_table": "target_officers_table", "target_column": "id"},
                    {"source_table": "source_officers_table", "source_column": "officer_name",
                     "target_table": "target_officers_table", "target_column": "name"}
                ]
            }
    """
    migrations = defaultdict(list)

    with open(file_path, mode="r") as file:
        reader = csv.DictReader(file)

        # Validate the presence of required headers
        required_headers = {"migration_name", "source_table", "source_column", "target_table", "target_column"}
        if not required_headers.issubset(reader.fieldnames):
            raise ValueError(f"CSV file is missing one or more required headers: {required_headers}")

        # Process each row and group by migration_name
        for row in reader:
            migrations[row["migration_name"]].append(
                {"source_table": row["source_table"], "source_column": row["source_column"],
                    "target_table": row["target_table"], "target_column": row["target_column"]})

    return migrations

def insert_record(cursor, table, data, logger=None):
    """
    Insert a record into the specified table and return the inserted ID.
    Args:
        cursor: Database cursor
        table (str): Table name
        data (dict): Dictionary of column-value pairs
        logger (logging.Logger, optional): Logger instance
    Returns:
        int: ID of the inserted record
    """
    columns = list(data.keys())
    placeholders = ", ".join(["%s"] * len(columns))
    query = f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({placeholders})"
    
    if logger:
        logger.debug(f"Executing insert query on {table}: {query}")
    
    cursor.execute(query, list(data.values()))
    last_id = cursor.lastrowid
    
    if logger:
        logger.debug(f"Inserted record in {table} with ID: {last_id}")
    
    return last_id

def get_record_details_by_id(cursor, table_name, record_id, columns, conn=None, logger=None, id_name="id"):
    """
    Get details for a specific record by its ID from any table.
    
    Args:
        cursor: Database cursor
        table_name (str): The name of the table to query.
        record_id: The ID of the record to fetch.
        columns (list): A list of column names to select.
        conn: Database connection to use if cursor is None
        logger (logging.Logger, optional): Logger instance
        id_name (str, optional): The name of the ID column. Defaults to "id".
        
    Returns:
        dict: Dictionary containing the requested details or None if not found.
    """
    if not columns:
        error_msg = f"Warning: No columns specified for table {table_name}"
        if logger:
            logger.warning(error_msg)
        print(error_msg)
        return None
        
    try:
        # Use the provided cursor or create a new one from the connection
        use_cursor = cursor
        if use_cursor is None and conn:
            use_cursor = conn.cursor()
            
        if use_cursor is None:
            error_msg = f"Warning: No cursor available for {table_name} lookup"
            if logger:
                logger.warning(error_msg)
            print(error_msg)
            return None
            
        column_list = ", ".join(columns)
        query = f"SELECT {column_list} FROM {table_name} WHERE {id_name} = %s"
        
        if logger:
            logger.debug(f"Executing query: {query} with ID: {record_id}")
            
        use_cursor.execute(query, (record_id,))
        result = use_cursor.fetchone()
        
        if result:
            # Create a dictionary mapping column names to fetched values
            return dict(zip(columns, result))
        
        if logger:
            logger.debug(f"No record found in {table_name} with {id_name}={record_id}")
        return None
        
    except Exception as e:
        error_msg = f"Error getting details for record {record_id} from {table_name}: {str(e)}"
        if logger:
            logger.error(error_msg)
        print(error_msg)
        return None

def get_record_value(table, condition, column, cursor=None, conn=None, logger=None):
    """
    Get any record value from any table based on a condition.
    
    Args:
        table (str): The table to query
        condition (str): The WHERE condition (without the 'WHERE' keyword)
        column (str): The column name to return
        cursor: Database cursor to use (uses the connection's cursor if None)
        conn: Database connection to use if cursor is None
        logger (logging.Logger, optional): Logger instance
        
    Returns:
        Any: The value of the specified column if found, None otherwise
    """
    try:
        # Use the provided cursor or create a new one from the connection
        use_cursor = cursor
        if use_cursor is None and conn:
            use_cursor = conn.cursor()
            
        if use_cursor is None:
            error_msg = f"Warning: No cursor available for {table} lookup"
            if logger:
                logger.warning(error_msg)
            print(error_msg)
            return None
        
        # For clients, we need to check with composite external_id
        if table == "clients" and "external_id" in condition and "branch_id" in condition:
            parts = condition.split(" and ")
            external_id_part = next((p for p in parts if "external_id" in p), None)
            branch_id_part = next((p for p in parts if "branch_id" in p), None)
            
            if external_id_part and branch_id_part:
                # Extract values
                client_code = external_id_part.split("'")[1]
                branch_id = branch_id_part.split("'")[1]
                
                # Get branch code from branch_id
                branch_code_query = f"SELECT external_id FROM branches WHERE id = '{branch_id}' LIMIT 1"
                use_cursor.execute(branch_code_query)
                branch_result = use_cursor.fetchone()
                
                if branch_result and len(branch_result) > 0:
                    branch_code = branch_result[0]
                    # Create composite external_id
                    composite_id = f"{branch_code}-{client_code}"
                    # Update condition
                    condition = f"external_id = '{composite_id}'"
        
        query = f"SELECT {column} FROM {table} WHERE {condition} LIMIT 1"
        use_cursor.execute(query)
        result = use_cursor.fetchone()
        
        if result and len(result) > 0:
            return result[0]
        else:
            if logger:
                logger.debug(f"No record found in {table} where {condition}")
            return None
            
    except Exception as e:
        error_msg = f"Error looking up record in {table} where {condition}: {str(e)}"
        if logger:
            logger.error(error_msg)
        print(error_msg)
        return None

def perform_cleanup(conn, table_name, condition=""):
    """
    Perform cleanup on the target table.

    Args:
        conn: Database connection
        table_name (str): Name of the table to clean up
        condition (str): Optional WHERE clause for conditional cleanup
    """
    cursor = conn.cursor()

    try:
        # Build the DELETE query with the optional condition
        query = f"DELETE FROM {table_name}"
        if condition:
            query += f" {condition}"

        cursor.execute(query)
        rows_deleted = cursor.rowcount
        print(f"Deleted {rows_deleted} rows from {table_name}")

    except Exception as e:
        conn.rollback()
        print(f"Error during cleanup of {table_name}: {str(e)}")
        raise  # Re-raise the exception to be caught by the caller
    finally:
        cursor.close()


def is_connection_alive(conn):
    """
    Check if a database connection is still alive.
    
    Args:
        conn: Database connection to check
        
    Returns:
        bool: True if connection is alive, False otherwise
    """
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        cursor.fetchone()
        cursor.close()
        return True
    except Exception:
        return False
