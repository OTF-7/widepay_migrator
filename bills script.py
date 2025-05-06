import pandas as pd
import mysql.connector
from mysql.connector import Error
from datetime import datetime, timedelta
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Database connection details from environment variables
db_config = {
    'host': os.getenv('LOCAL_MYSQL_HOST'),
    'user': os.getenv('LOCAL_MYSQL_USER'),
    'password': os.getenv('LOCAL_MYSQL_PASSWORD'),
    'database': os.getenv('LOCAL_MYSQL_DATABASE')
}

# Function to connect to the MySQL database
def create_db_connection():
    try:
        connection = mysql.connector.connect(**db_config)
        return connection
    except Error as e:
        print(f"Error: {e}")
        return None

# Function to get the client ID by name
def get_client_id(connection, client_national_id):
    cursor = connection.cursor()
    query = "SELECT id FROM clients WHERE national_id = %s"
    cursor.execute(query, (client_national_id,))
    result = cursor.fetchone()
    return result[0] if result else None

# Function to insert a record into the revolving_credit_limits table
def insert_revolving_credit_limit(connection, client_id, applied_amount, approved_amount, created_at, status):
    status = 'approved' if status == 'active' else status
    cursor = connection.cursor()
    query = """
    INSERT INTO revolving_credit_limits (client_id, applied_amount, approved_amount, officer_id, created_by_id, branch_id,
     created_at, status, corporate_id, product_id, currency_id, fund_id, purpose_id, activity_type_id, activity_id, activity_name)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 0, 17, 1, 1, 1, 1, 1, '')
    """
    values = (client_id, applied_amount, approved_amount, 57368, 57368, 1, created_at, status)
    cursor.execute(query, values)
    connection.commit()
    return cursor.lastrowid  # Return the last inserted ID

# Function to calculate late days
def calculate_late_days(created_at, expected_maturity_date):
    today = datetime.today()
    if today < expected_maturity_date:
        return (today - created_at).days
    else:
        return (expected_maturity_date - created_at).days

# Function to insert a record into the loans table
def insert_loan(connection, client_id, created_at, approved_amount, revolving_credit_id):
    cursor = connection.cursor()
    expected_maturity_date = created_at + timedelta(days=120)
    late_days = calculate_late_days(created_at, expected_maturity_date)
    interest_rate = 0.00113
    interest = interest_rate * late_days
    penalties = 120 * 0.0005  # Total period of loan is 120 days
    fees = 150
    disbursement_charges = 75

    query = """
    INSERT INTO loans (
        client_id, branch_id, created_by_id, loan_officer_id, created_at, revolving_enabled, currency_id,
        loan_product_id, loan_transaction_processing_strategy_id, fund_id, loan_purpose_id, submitted_on_date,
        approved_on_date, submitted_by_user_id, approved_by_user_id, expected_maturity_date, disbursed_on_date,
        approved_amount, applied_amount, principal, interest_rate, flat_interest_rate, interest_disbursed_derived, fees_disbursed_derived, penalties_disbursed_derived, loan_term,
        applied_loan_term, repayment_frequency, repayment_frequency_type, interest_rate_type, disbursement_charges, revolving_credit_id, activity_name
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, '')
    """
    values = (
        client_id, 1, 57368, 57368, created_at, 1, 1, 17, 3, 1, 1, created_at, created_at, 57368, 57368,
        expected_maturity_date, created_at, approved_amount, approved_amount, approved_amount, interest_rate, interest_rate,
        interest, fees, penalties, 4, 4, 1, 'months', 'day', disbursement_charges, revolving_credit_id
    )
    cursor.execute(query, values)
    connection.commit()

# Main function to process the Excel file
def process_credits_excel(file_path):
    # Read the Excel file
    df = pd.read_excel(file_path)

    # Connect to the database
    connection = create_db_connection()
    if connection is None:
        return

    # Iterate over each row in the Excel file
    for index, row in df.iterrows():
        client_national_id = row.iloc[0]
        approved_amount = row.iloc[1]
        invoice_amount = row.iloc[2]
        transfer_date = row.iloc[3]
        active = row.iloc[4]

        # Get the client ID
        client_id = get_client_id(connection, client_national_id)
        if not client_id:
            print(f"Client '{client_national_id}' not found in the database. Skipping...")
            continue

        # Convert Transfer Date to datetime
        created_at = pd.to_datetime(transfer_date)

        # Determine status
        status = 'active' if active == 1 else 'closed'

        # Insert into revolving_credit_limits table
        revolving_credit_id = insert_revolving_credit_limit(
            connection, client_id, approved_amount, approved_amount, created_at, status
        )

        # Insert into loans table
        insert_loan(connection, client_id, created_at, approved_amount, revolving_credit_id)

    # Close the database connection
    connection.close()

# Path to your Excel file
excel_file_path = 'bills.xlsx'

# Process the Excel file
process_credits_excel(excel_file_path)