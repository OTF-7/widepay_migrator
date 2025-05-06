import pandas as pd
import mysql.connector
from mysql.connector import Error
from datetime import datetime
from sshtunnel import SSHTunnelForwarder
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Function to get database configuration based on environment choice
def get_db_config():
    # Get environment variables for server configuration
    server_config = {
        'db_type': 'mysql',
        'host': os.getenv('SERVER_MYSQL_HOST'),
        'user': os.getenv('SERVER_MYSQL_USER'),
        'password': os.getenv('SERVER_MYSQL_PASSWORD'),
        'database': os.getenv('SERVER_MYSQL_DATABASE'),
        'ssh_host': os.getenv('SERVER_MYSQL_SSH_HOST'),
        'ssh_user': os.getenv('SERVER_MYSQL_SSH_USER'),
        'ssh_password': os.getenv('SERVER_MYSQL_SSH_PASSWORD')
    }
    
    # Return server configuration directly without prompting
    return server_config

# Function to connect to the MySQL database
def create_db_connection(db_config):
    try:
        tunnel = None
        host = db_config.get('host')
        user = db_config.get('user')
        password = db_config.get('password')
        database = db_config.get('database')
        ssh_host = db_config.get('ssh_host')
        ssh_user = db_config.get('ssh_user')
        ssh_password = db_config.get('ssh_password')
        
        # Set up SSH tunnel if SSH details are provided
        if ssh_host and ssh_user:
            print(f"Setting up SSH tunnel to {ssh_host}...")
            tunnel = SSHTunnelForwarder(
                (ssh_host, 22),
                ssh_username=ssh_user,
                ssh_password=ssh_password if ssh_password else None,
                ssh_pkey=False,
                host_pkey_directories=[],
                remote_bind_address=('127.0.0.1', 3306)  # Connect to MySQL on the remote server
            )
            tunnel.start()
            host = "127.0.0.1"
            port = tunnel.local_bind_port
            print(f"SSH tunnel established on local port {port}")
        else:
            port = 3306
            print("Using direct database connection (no SSH tunnel)")
            
        # Connect to the database with timeout
        print(f"Connecting to database {database} at {host}:{port}...")
        connection = mysql.connector.connect(
            host=host,
            user=user,
            password=password,
            database=database,
            port=port,
            connection_timeout=30,  # Add timeout
            use_pure=True  # Use pure Python implementation
        )
        
        print(f"Connected to {database} database")
        return connection, tunnel
    except Error as e:
        print(f"Database connection error: {e}")
        if tunnel:
            print("Closing SSH tunnel due to database connection error")
            tunnel.stop()
        return None, None
    except Exception as e:
        print(f"Unexpected error: {e}")
        if tunnel:
            print("Closing SSH tunnel due to unexpected error")
            tunnel.stop()
        return None, None

# Function to process the Excel file
def process_users_excel(file_path):
    # Get database configuration
    db_config = get_db_config()
    
    # Read the Excel file
    df = pd.read_excel(file_path)
    
    # Connect to the database
    connection, tunnel = create_db_connection(db_config)
    if connection is None:
        return
    
    try:
        # Get all existing emails for faster lookups
        existing_emails = get_all_existing_emails(connection)
        
        # Track statistics
        total_users = 0
        successful_inserts = 0
        failed_inserts = 0
        skipped_emails = 0
        
        # Iterate over each row in the Excel file
        for index, row in df.iterrows():
            total_users += 1
            
            # Extract data from Excel
            user_name = row['user_name']
            user_full_name = row['user_full_name']
            branch_name = row['branch_name']
            start_date = row['start_date']
            user_status = row['user_status']
            
            # If user_full_name is NULL, use user_name instead
            if pd.isna(user_full_name) or user_full_name == 'NULL':
                print(f"Using username '{user_name}' as full name for user {user_name}")
                user_full_name = user_name
            
            # Format email
            email = f"{user_name}@sandah.org"
            
            # Check if email already exists (using the pre-loaded set)
            if email.lower() in existing_emails:
                print(f"Email {email} already exists in the database. Skipping...")
                skipped_emails += 1
                continue
            
            # Convert status to active flag (1 for Active, 0 for Not Active)
            active = 1 if user_status == 'Active' else 0
            
            # Convert start_date to datetime
            try:
                created_at = pd.to_datetime(start_date)
                # Convert to string format that MySQL accepts
                created_at = created_at.strftime('%Y-%m-%d %H:%M:%S')
            except:
                print(f"Error converting date for user {user_name}. Using current date.")
                created_at = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            # Get branch ID
            branch_id = get_branch_id(connection, branch_name)
            if not branch_id:
                print(f"Branch '{branch_name}' not found for user {user_name}. Skipping...")
                failed_inserts += 1
                continue
            
            # Insert user directly (no need to check email again)
            cursor = connection.cursor(buffered=True)
            try:
                # Set role_id to 4
                role_id = 4
                
                query = """
                INSERT INTO users (name, email, branch_id, created_at, updated_at, active, role_id)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                """
                
                values = (user_full_name, email, branch_id, created_at, created_at, active, role_id)
                
                cursor.execute(query, values)
                connection.commit()
                user_id = cursor.lastrowid
                
                # Add the new email to our set to prevent duplicates in this run
                existing_emails.add(email.lower())
                
                print(f"Successfully inserted user: {user_full_name} with ID: {user_id}")
                successful_inserts += 1
            except Error as e:
                print(f"Error inserting user {user_full_name}: {e}")
                failed_inserts += 1
            finally:
                cursor.close()
        
        # Print summary
        print(f"\nMigration Summary:")
        print(f"Total users processed: {total_users}")
        print(f"Successfully inserted: {successful_inserts}")
        print(f"Skipped (email exists): {skipped_emails}")
        print(f"Failed to insert: {failed_inserts}")
    
    finally:
        # Close the database connection and SSH tunnel
        if connection:
            connection.close()
        if tunnel:
            tunnel.stop()
        print("Database connection and SSH tunnel closed.")

# Path to your Excel file
excel_file_path = 'users.xlsx'

# Function to get branch ID by name
def get_branch_id(connection, branch_name):
    cursor = connection.cursor()
    try:
        query = "SELECT id FROM branches WHERE name = %s"
        cursor.execute(query, (branch_name,))
        result = cursor.fetchone()
        return result[0] if result else None
    finally:
        cursor.close()

# Function to check if email already exists
# Function to get all existing emails
def get_all_existing_emails(connection):
    cursor = connection.cursor(buffered=True)
    existing_emails = set()
    
    try:
        query = "SELECT email FROM users"
        cursor.execute(query)
        for row in cursor:
            email = row[0]
            if email is not None:  # Check if email is not None
                existing_emails.add(email.lower())
        print(f"Loaded {len(existing_emails)} existing emails from database")
        return existing_emails
    except Error as e:
        print(f"Error fetching existing emails: {e}")
        return set()
    finally:
        cursor.close()

# Process the Excel file
if __name__ == "__main__":
    print("Starting user migration...")
    process_users_excel(excel_file_path)
    print("User migration completed.")