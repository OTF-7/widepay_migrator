from general_helper import *
from custom_helper import *
import datetime
from CustomLogic import CustomLogic
from logger_setup import setup_logger
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

class MigrationManager:
    def __init__(self, config_file):
        """
        Initialize the MigrationManager with a mapping file specifying migrations.
        Args:
            config_file (str): Path to the CSV configuration file.
        """
        self.logger = setup_logger()
        self.logger.info("Initializing MigrationManager")
        self.migration_config = load_migration_config(config_file)
        self.migrations = list(self.migration_config.keys())
        if not self.migrations:
            error_msg = "No migrations found in the configuration file."
            self.logger.error(error_msg)
            raise ValueError(error_msg)
        self.logger.info(f"Loaded {len(self.migrations)} migrations from config file")

    def list_migrations(self):
        """
        Print the list of available migrations with their indices.
        """
        print("\nAvailable Migrations:")
        for i, migration_name in enumerate(self.migrations, start=1):
            print(f"{i}: {migration_name}")

    def get_migration_by_index(self, index):
        """
        Get the migration mappings by its index.
        Args:
            index (int): Index of the migration.
        Returns:
            list[dict]: List of column mappings for the selected migration name.
        """
        if 0 <= index < len(self.migrations):
            migration_name = self.migrations[index]
            return self.migration_config[migration_name]
        else:
            raise ValueError("Invalid migration index. Please select a valid option.")

def run_script():
    import os
    
    logger = setup_logger()
    logger.info("=== DATABASE MIGRATION TOOL STARTED ===")
    print("\n=== DATABASE MIGRATION TOOL ===")

    # Get the directory where the script is located
    script_dir = os.path.dirname(os.path.abspath(__file__))
    
    # Create the full path to the mappings.csv file
    mappings_file = os.path.join(script_dir, "mappings.csv")
    
    logger.info(f"Loading migrations from: {mappings_file}")
    
    # Load migrations from the CSV file
    manager = MigrationManager(mappings_file)
    logic_processor = CustomLogic()
    logic_processor.logger = logger

    # Prompt for database connections (source and target), one-time setup
    logger.info("[Setup Database Connections]")
    print("\n[Setup Database Connections]")
    
    # Source database connection with retry loop
    src_conn = None
    src_ssh_tunnel = None
    while src_conn is None:
        # Use environment variables with correct names from .env
        src_conn_details = {
            "db_type": os.getenv('SOURCE_DB_TYPE'),
            "host": os.getenv('SOURCE_HOST'),
            "user": os.getenv('SOURCE_USER'),
            "password": os.getenv('SOURCE_PASSWORD'),
            "database": os.getenv('SOURCE_DATABASE'),
            "ssh_host": os.getenv('SOURCE_SSH_HOST') if os.getenv('SOURCE_USE_SSH') == 'y' else None,
            "ssh_user": os.getenv('SOURCE_SSH_USER') if os.getenv('SOURCE_USE_SSH') == 'y' else None,
            "ssh_password": os.getenv('SOURCE_SSH_PASSWORD') if os.getenv('SOURCE_USE_SSH') == 'y' else None
        }
        
        print(f"Connecting to source database: {src_conn_details['database']} ({src_conn_details['db_type']})")
        logger.info(f"Connecting to source database: {src_conn_details['database']} ({src_conn_details['db_type']})")
        
        connection_result = test_connection(src_conn_details)
        if connection_result is None:
            logger.warning("Source database connection failed")
            retry = input("\nSource database connection failed. Retry? (y/n): ").strip().lower()
            if retry != 'y':
                logger.info("Exiting the program due to source connection failure")
                print("Exiting the program.")
                return
        else:
            src_conn, src_ssh_tunnel = connection_result
            logger.info("Source database connection successful")

    # Target database connection with retry loop
    dest_conn = None
    dest_ssh_tunnel = None
    while dest_conn is None:
        # Use environment variables with correct names from .env
        dest_conn_details = {
            "db_type": os.getenv('DEST_DB_TYPE'),
            "host": os.getenv('DEST_HOST'),
            "user": os.getenv('DEST_USER'),
            "password": os.getenv('DEST_PASSWORD'),
            "database": os.getenv('DEST_DATABASE'),
            "ssh_host": os.getenv('DEST_SSH_HOST') if os.getenv('DEST_USE_SSH') == 'y' else None,
            "ssh_user": os.getenv('DEST_SSH_USER') if os.getenv('DEST_USE_SSH') == 'y' else None,
            "ssh_password": os.getenv('DEST_SSH_PASSWORD') if os.getenv('DEST_USE_SSH') == 'y' else None
        }
        
        print(f"Connecting to target database: {dest_conn_details['database']} ({dest_conn_details['db_type']})")
        logger.info(f"Connecting to target database: {dest_conn_details['database']} ({dest_conn_details['db_type']})")
        
        connection_result = test_connection(dest_conn_details)
        if connection_result is None:
            logger.warning("Target database connection failed")
            retry = input("\nTarget database connection failed. Retry? (y/n): ").strip().lower()
            if retry != 'y':
                logger.info("Exiting the program due to target connection failure")
                print("Exiting the program.")
                return
        else:
            dest_conn, dest_ssh_tunnel = connection_result
            logger.info("Target database connection successful")

    if not src_conn or not dest_conn:
        logger.error("One or both database connections failed")
        print("\nError: One or both database connections failed. Exiting.")
        return

    # Enter main loop
    while True:
        print("\nMAIN MENU:")
        manager.list_migrations()
        
        # Calculate dynamic option numbers for settlement options
        transactions_option = len(manager.migrations) + 1
        installments_option = len(manager.migrations) + 2
        
        print(f"{transactions_option}: Settle Transactions")
        print(f"{installments_option}: Settle Installments")
        print("0: Quit")
        choice = input("\nEnter the number of the migration or action: ").strip()

        if choice == "0":  # Quit
            logger.info("User chose to exit the program")
            print("Exiting the program.")
            break
        elif choice == str(transactions_option):  # Settle Transactions
            try:
                logger.info("Starting transaction settlement process")
                cursor_dest = dest_conn.cursor()
                cursor_dest.execute("START TRANSACTION")
                settle_transactions(dest_conn, src_conn, logger)
                logger.info("Committing transaction settlement changes")
                dest_conn.commit()
                logger.info("Transactions were settled successfully")
                print(f"Transactions were settled successfully")
            except Exception as e:
                error_msg = f"Error during transaction settlement: {str(e)}"
                logger.error(error_msg)
                dest_conn.rollback()
                print(error_msg)
            continue
        elif choice == str(installments_option):  # Settle Installments
            try:
                logger.info("Starting installment settlement process")
                cursor_dest = dest_conn.cursor()
                cursor_dest.execute("START TRANSACTION")
                settle_installments(dest_conn, src_conn, logger)
                logger.info("Committing installment settlement changes")
                dest_conn.commit()
                logger.info("Installments were settled successfully")
                print(f"Installments were settled successfully")
            except Exception as e:
                error_msg = f"Error during installment settlement: {str(e)}"
                logger.error(error_msg)
                dest_conn.rollback()
                print(error_msg)
            continue

        try:
            migration = manager.get_migration_by_index(int(choice) - 1)
            migration_name = manager.migrations[int(choice) - 1]
            logger.info(f"User selected migration: {migration_name}")
            print(f"\n=== Selected Migration: {migration_name} ===")
            print("1: Run Migration")
            print("2: Perform Cleanup (Target Table)")
            print("Press Any Other Number To Go Back To The Main Menu")
            action = input("Enter the action number: ").strip()

            if action == "1":
                # Run migration logic
                logger.info(f"Starting migration process for {migration_name}")
                print("Running migration...")
                
                # Create a migration-specific logger
                migration_logger = setup_logger(migration_name)
                logic_processor.logger = migration_logger

                migration_logger.info(f"=== Starting Migration: {migration_name} ===")

                # Ask about foreign key constraints
                fk_option = input("Migrate with foreign keys: \n1: Enabled (default)\n2: Disabled\nChoice: ").strip() or "1"
                migration_logger.info(f"Foreign key constraints: {'Enabled' if fk_option == '1' else 'Disabled'}")

                # Ask for record limit (for testing purposes)
                record_limit = input("Number of records to migrate (leave empty for all records): ").strip()
                if record_limit:
                    migration_logger.info(f"Record limit set to: {record_limit}")
                else:
                    migration_logger.info("No record limit set, migrating all records")

                # Fetch data from source
                src_cursor = src_conn.cursor()
                source_columns = [mapping["source_column"] for mapping in migration]
                
                # Build query based on database type and record limit
                if record_limit and record_limit.isdigit():
                    # SQL Server uses TOP syntax
                    if src_conn_details["db_type"] == "sqlserver":
                        query_src = f"SELECT TOP {record_limit} {', '.join(source_columns)} FROM {migration[0]['source_table']}"
                    # MySQL uses LIMIT syntax
                    else:
                        query_src = f"SELECT {', '.join(source_columns)} FROM {migration[0]['source_table']} LIMIT {record_limit}"
                else:
                    query_src = f"SELECT {', '.join(source_columns)} FROM {migration[0]['source_table']}"

                try:
                    print(f"Executing query: {query_src}")
                    migration_logger.info(f"Executing query: {query_src}")
                    src_cursor.execute(query_src)
                    data_to_migrate = src_cursor.fetchall()
                    total_records = len(data_to_migrate)
                    migration_logger.info(f"Found {total_records} records to migrate")
                    print(f"Found {total_records} records to migrate")
                except Exception as e:
                    error_msg = f"Error fetching data: {str(e)}"
                    migration_logger.error(error_msg)
                    print(error_msg)
                    continue
                
                # Use default migration logic with custom processing
                cursor_dest = dest_conn.cursor()
                
                # Disable foreign key checks if requested
                if fk_option == "2":
                    migration_logger.info("Disabling foreign key checks for this migration")
                    print("Disabling foreign key checks for this migration...")
                    cursor_dest.execute("SET FOREIGN_KEY_CHECKS=0")
                
                # Start a transaction for the entire migration
                migration_logger.info("Starting database transaction")
                cursor_dest.execute("START TRANSACTION")
                
                # Track unique branches to avoid duplicates if this is the branches migration
                successful_inserts = 0
                failed_inserts = 0
                total_records = len(data_to_migrate)
                
                for i, row in enumerate(data_to_migrate):
                    # Show progress
                    if i % 10 == 0 or i == total_records - 1:
                        print(f"Processing record {i+1}/{total_records} ({(i+1)/total_records*100:.1f}%)")
                    
                    try:
                        # Create a dictionary with target column names as keys
                        processed_row = {}
                        
                        # Also create a dictionary with source column names for reference
                        source_row = {}
                        
                        for idx, mapping in enumerate(migration):
                            # Store source values for reference
                            source_row[mapping["source_column"]] = row[idx]
                            
                            # Handle unsupported ODBC SQL types by converting to string
                            try:
                                value = row[idx]
                                # Convert problematic types to strings
                                if value is not None and not isinstance(value, (str, int, float, bool, datetime.date, datetime.datetime)):
                                    value = str(value)
                                processed_row[mapping["target_column"]] = value
                            except Exception as e:
                                print(f"Error processing column {mapping['source_column']}: {str(e)}")
                                processed_row[mapping["target_column"]] = None

                        # Apply custom logic with both source and target data
                        logic_processor.process_columns(
                            migration_name, 
                            processed_row, 
                            source_row=source_row, 
                            cursor=cursor_dest,
                            src_cursor=src_cursor
                        )
                        
                        # Skip this record if processed_row is None (e.g., transaction type not mapped)
                        if processed_row is None:
                            migration_logger.debug(f"Skipping record {i+1} - processing returned None")
                            continue
                        
                        # Extract charge data if present (before main insert)
                        charge_to_add = None
                        if migration_name == "loans" and "_charge_to_add" in processed_row:
                            # Pop removes the key and returns its value
                            charge_to_add = processed_row.pop("_charge_to_add") 

                        # Create dynamic SQL based on the processed row (without the charge data)
                        target_columns = list(processed_row.keys())
                        placeholders = ", ".join(["%s"] * len(target_columns))
                        query_dest = f"INSERT INTO {migration[0]['target_table']} ({', '.join(target_columns)}) VALUES ({placeholders})"
                        
                        # Execute main insert (loan or other record)
                        cursor_dest.execute(query_dest, list(processed_row.values()))
                        successful_inserts += 1
                        
                        # Get the ID of the just-inserted record
                        last_inserted_id = cursor_dest.lastrowid

                        if migration_name == "loans" and last_inserted_id:
                            # Insert linked charge if applicable (after main loan insert)
                            if charge_to_add:
                                charge_data = charge_to_add
                                charge_data["loan_id"] = last_inserted_id # Add the loan_id now
                                try:
                                    insert_record(cursor_dest, "loan_linked_charges", charge_data)
                                    migration_logger.debug(f"Successfully inserted linked charge for loan ID {last_inserted_id}")
                                except Exception as charge_e:
                                    failed_inserts += 1
                                    successful_inserts -= 1
                                    error_msg = f"Error inserting linked charge for loan ID {last_inserted_id}: {str(charge_e)}"
                                    migration_logger.error(error_msg)
                                    print(f"    {error_msg}")

                            # Insert loan profiles after loan creation
                            if "client_id" in processed_row:
                                client_id = processed_row["client_id"]
                                migration_logger.debug(f"Creating loan profiles for loan ID {last_inserted_id} from client ID {client_id}")

                                # Get client profiles
                                try:
                                    client_profiles_query = """
                                    SELECT id ,document_type_id, document_id, career
                                    FROM profiles 
                                    WHERE profileable_type = 'App\\\\Models\\\\Client' AND profileable_id = %s
                                    """
                                    cursor_dest.execute(client_profiles_query, (client_id,))
                                    client_profiles = cursor_dest.fetchall()

                                    if client_profiles:
                                        for profile in client_profiles:
                                            # Create loan profile with same data but different model type
                                            loan_profile_data = {
                                                "profile_id": profile[0],
                                                "document_type_id": profile[1],
                                                "document_id": profile[2],
                                                "career": profile[3],
                                                "model_type": "App\\Models\\Loan\\Loan",
                                                "model_id": last_inserted_id
                                            }
                                            insert_record(cursor_dest, "loan_profiles", loan_profile_data)

                                        migration_logger.debug(f"Created {len(client_profiles)} loan profiles for loan ID {last_inserted_id}")
                                    else:
                                        migration_logger.warning(f"No client profiles found for client ID {client_id}")
                                except Exception as e:
                                    error_msg = f"Error creating loan profiles for loan ID {last_inserted_id}: {str(e)}"
                                    migration_logger.error(error_msg)
                                    print(f"    {error_msg}")
                                    
                            try:
                                # Get application details with guarantor information
                                application_key = source_row.get('application_key')
                                app_columns = ["co_client_key", "co2_client_key"]
                                
                                application_details = get_record_details_by_id(
                                    src_cursor, 
                                    "ilts.c1_loan_application", 
                                    application_key,
                                    app_columns, 
                                    logger=migration_logger, 
                                    id_name='application_key'
                                )
                                
                                if application_details:
                                    # Process both guarantors
                                    guarantor_keys = [
                                        application_details.get("co_client_key"),
                                        application_details.get("co2_client_key")
                                    ]
                                    
                                    for idx, guarantor_key in enumerate(guarantor_keys):
                                        if guarantor_key:
                                            migration_logger.debug(f"Found guarantor key: {guarantor_key} for application {application_key}")
                                            
                                            # Find guarantor client by external_id
                                            guarantor_details = get_record_details_by_id(
                                                cursor_dest,
                                                "clients",
                                                guarantor_key,
                                                ["id", "created_at"],
                                                logger=migration_logger,
                                                id_name='external_id'
                                            )
                                            
                                            if guarantor_details:
                                                guarantor_id = guarantor_details.get("id")
                                                guarantor_created_at = guarantor_details.get("created_at")
                                                
                                                migration_logger.debug(f"Found guarantor client ID: {guarantor_id}")
                                                
                                                # Create loan guarantor record with the correct field structure
                                                guarantor_data = {
                                                    "model_type": "App\\Models\\Loan\\Loan",
                                                    "model_id": last_inserted_id,
                                                    "guarantor_id": guarantor_id,
                                                    "created_at": guarantor_created_at or datetime.datetime.now(),
                                                    "updated_at": guarantor_created_at or datetime.datetime.now()
                                                }
                                                
                                                insert_record(cursor_dest, "loan_guarantors", guarantor_data, migration_logger)
                                                migration_logger.info(f"Created loan guarantor record for loan ID {last_inserted_id} with guarantor ID {guarantor_id}")
                                            else:
                                                migration_logger.warning(f"Guarantor client not found for client key: {guarantor_key}")
                                        else:
                                            migration_logger.debug(f"No guarantor {idx+1} found for application {application_key}")
                                else:
                                    migration_logger.debug(f"No application details found for application key {application_key}")
                                    
                            except Exception as e:
                                error_msg = f"Error creating loan guarantors for loan ID {last_inserted_id}: {str(e)}"
                                migration_logger.error(error_msg)
                                print(f"    {error_msg}")

                            # Insert location data if latitude and longitude are available
                            if "latitude" in processed_row and "longitude" in processed_row:
                                location_data = {
                                    "latitude": processed_row["latitude"],
                                    "longitude": processed_row["longitude"],
                                    "locationable_type": "App\\Models\\Loan\\Loan",
                                    "locationable_id": last_inserted_id,
                                    "active": 1,
                                    "role_id": 3
                                }
                                insert_record(cursor_dest, "locations", location_data)
                                migration_logger.debug(f"Created location record for client ID {last_inserted_id}")

                        # Insert wallet after officer creation
                        if migration_name == "officers" and last_inserted_id:
                            migration_logger.debug(f"Creating wallet for officer ID {last_inserted_id}")
                            wallet_data = {
                                "user_id": last_inserted_id,
                                "role_id": 60,
                                "currency_id": 1,
                                "wallet_type": "cash",
                                "amount": 0,
                                "active": True,
                            }
                            insert_record(cursor_dest, "wallets", wallet_data)

                        # Insert profiles after client creation
                        if migration_name == "clients" and last_inserted_id:
                            migration_logger.debug(f"Creating profiles for client ID {last_inserted_id}")
                            if "national_id" in processed_row:
                                client_columns = ['bus_add_1', 'bus_add_2', 'bus_add_3', 'bus_name', 'id_date']

                                client_details = get_record_details_by_id(
                                    src_cursor, 
                                    "ilts.c1_client_info_table", 
                                    source_row.get('client_key'),
                                    client_columns, 
                                    logger=migration_logger, 
                                    id_name='client_key'
                                )
                                # Get document_issued_at date and calculate expiry date (8 years later)
                                document_issued_at = client_details.get('id_date', '')
                                document_expires_at = None
                                
                                if document_issued_at and isinstance(document_issued_at, datetime.datetime):
                                    document_expires_at = document_issued_at.replace(year=document_issued_at.year + 8)
                                elif document_issued_at and isinstance(document_issued_at, datetime.date):
                                    document_expires_at = datetime.datetime.combine(
                                        document_issued_at.replace(year=document_issued_at.year + 8),
                                        datetime.datetime.min.time()
                                    )
                                
                                national_profile_data = {
                                    "document_type_id": 1,
                                    "document_id": processed_row["national_id"],
                                    "document_issued_at": document_issued_at,
                                    "document_expires_at": document_expires_at,
                                    "profileable_type": "App\\Models\\Client",
                                    "profileable_id": last_inserted_id
                                }
                                insert_record(cursor_dest, "profiles", national_profile_data)

                                # Prepare business address and career for commercial profile
                                bus_address_parts = [
                                    client_details.get('bus_add_1', ''),
                                    client_details.get('bus_add_2', ''),
                                    client_details.get('bus_add_3', '')
                                ]
                                bus_address_parts = [part for part in bus_address_parts if part and str(part).strip()]
                                business_address = ", ".join(bus_address_parts) if bus_address_parts else None
                                career = client_details.get('bus_name', '')

                                # Insert commercial ID profile
                                commercial_profile_data = {
                                    "document_type_id": 2,
                                    "career": career,
                                    "employer_address": business_address,
                                    "profileable_type": "App\\Models\\Client",
                                    "profileable_id": last_inserted_id
                                }
                                insert_record(cursor_dest, "profiles", commercial_profile_data)
                                
                                # Insert location data if latitude and longitude are available
                                if "latitude" in processed_row and "longitude" in processed_row:
                                    location_data = {
                                        "latitude": processed_row["latitude"],
                                        "longitude": processed_row["longitude"],
                                        "locationable_type": "App\\Models\\Client",
                                        "locationable_id": last_inserted_id,
                                        "active": 1,
                                        "role_id": 3
                                    }
                                    insert_record(cursor_dest, "locations", location_data)
                                    migration_logger.debug(f"Created location record for client ID {last_inserted_id}")
                            else:
                                warning_msg = f"Cannot insert profile for client ID {last_inserted_id} due to missing national_id."
                                migration_logger.warning(warning_msg)
                                print(f"Warning: {warning_msg}")

                    except Exception as e:
                        failed_inserts += 1
                        error_msg = f"Error inserting {migration_name} data: {str(e)}"
                        migration_logger.error(error_msg)
                        migration_logger.error(f"Problematic row: {row}")
                        print(error_msg)
                        print(f"Problematic row: {row}")
                        continue
                
                # Commit the transaction if there were successful inserts
                if successful_inserts > 0:
                    migration_logger.info(f"Committing transaction with {successful_inserts} successful inserts")
                    dest_conn.commit()
                    summary_msg = f"Migration '{migration_name}' completed: {successful_inserts} successful, {failed_inserts} failed inserts out of {total_records} total records."
                    migration_logger.info(summary_msg)
                    print(summary_msg)
                else:
                    migration_logger.warning(f"Rolling back transaction - no successful inserts")
                    dest_conn.rollback()
                    summary_msg = f"Migration '{migration_name}' failed: No records were inserted out of {total_records} total records."
                    migration_logger.warning(summary_msg)
                    print(summary_msg)
                
                # Re-enable foreign key checks if they were disabled
                if fk_option == "2":
                    migration_logger.info("Re-enabling foreign key checks")
                    cursor_dest.execute("SET FOREIGN_KEY_CHECKS=1")
                    print("Foreign key checks re-enabled.")

            elif action == "2":
                # Perform cleanup on target table
                logger.info(f"Starting cleanup for {migration_name}")
                target_table = migration[0]["target_table"]
                
                # Define custom cleanup conditions based on migration type
                cleanup_condition = ""
                if migration_name == "officers":
                    try:
                        logger.info(f"Starting officers-specific cleanup")
                        cursor_dest = dest_conn.cursor()
                        cursor_dest.execute("START TRANSACTION")
                        # Then clean up wallets of clients
                        cleanup_condition = "WHERE role_id = 60"
                        logger.info(f"Cleaning up wallets {cleanup_condition}")
                        perform_cleanup(dest_conn, "wallets", cleanup_condition)
                        logger.info(f"Cleaning up {target_table} {cleanup_condition}")
                        perform_cleanup(dest_conn, target_table,  cleanup_condition)
                        
                        logger.info("Committing cleanup transaction")
                        dest_conn.commit()
                        logger.info(f"Cleanup completed successfully for users and {target_table}")
                        print(f"Cleanup completed successfully for users and {target_table}!")
                        continue  # Skip the standard cleanup below
                    except Exception as e:
                        error_msg = f"Error during cleanup: {str(e)}"
                        logger.error(error_msg)
                        dest_conn.rollback()
                        print(error_msg)
                        continue  # Skip the standard cleanup below
                elif migration_name == "loans":
                    try:
                        logger.info(f"Starting loans-specific cleanup")
                        cursor_dest = dest_conn.cursor()
                        cursor_dest.execute("START TRANSACTION")
                        
                        # First clean up profiles and locations related to loans
                        logger.info("Cleaning up loan profiles")
                        perform_cleanup(dest_conn, "loan_profiles", "WHERE model_type = 'App\\\\Models\\\\Loan\\\\Loan'")
                        logger.info("Cleaning up locations")
                        perform_cleanup(dest_conn, "locations", "WHERE locationable_type = 'App\\\\Models\\\\Loan\\\\Loan'")
                        logger.info("Cleaning up loan guarantors")
                        perform_cleanup(dest_conn, "loan_guarantors",
                                        "WHERE model_type = 'App\\\\Models\\\\Loan\\\\Loan'")
                        # Clean up linked charges
                        logger.info("Cleaning up loan linked charges")
                        perform_cleanup(dest_conn, "loan_linked_charges", "")
                        
                        # Finally clean up loans table
                        logger.info(f"Cleaning up {target_table}")
                        perform_cleanup(dest_conn, target_table, "")
                        
                        logger.info("Committing cleanup transaction")
                        dest_conn.commit()
                        logger.info(f"Cleanup completed successfully for {target_table} and related data")
                        print(f"Cleanup completed successfully for {target_table} and related data!")
                        continue  # Skip the standard cleanup below
                    except Exception as e:
                        error_msg = f"Error during cleanup: {str(e)}"
                        logger.error(error_msg)
                        dest_conn.rollback()
                        print(error_msg)
                        continue  # Skip the standard cleanup below
                elif migration_name == "clients":
                    try:
                        cursor_dest = dest_conn.cursor()
                        cursor_dest.execute("START TRANSACTION")
                        
                       # First clean up profiles and locations related to clients
                        perform_cleanup(dest_conn, "profiles", "WHERE profileable_type = 'App\\\\Models\\\\Client'")
                        perform_cleanup(dest_conn, "locations", "WHERE locationable_type = 'App\\\\Models\\\\Client'")
                        cleanup_condition = "WHERE role_id = 3"
                        # Then clean up users of clients
                        perform_cleanup(dest_conn, "users", cleanup_condition)
                        # Then clean up wallets of clients
                        perform_cleanup(dest_conn, "wallets", cleanup_condition)
                        # Finally clean up clients table
                        perform_cleanup(dest_conn, target_table, "")
                        
                        dest_conn.commit()
                        print(f"Cleanup completed successfully for users and {target_table}!")
                        continue  # Skip the standard cleanup below
                    except Exception as e:
                        dest_conn.rollback()
                        print(f"Error during cleanup: {str(e)}")
                        continue  # Skip the standard cleanup below
                
                # Standard cleanup for other migrations
                try:
                    cursor_dest = dest_conn.cursor()
                    cursor_dest.execute("START TRANSACTION")
                    perform_cleanup(dest_conn, target_table, cleanup_condition)
                    dest_conn.commit()
                    print(f"Cleanup completed successfully for table {target_table}!")
                except Exception as e:
                    dest_conn.rollback()
                    print(f"Error during cleanup: {str(e)}")
            
            else:
                 # Back to main menu
                continue

        except ValueError as e:
            error_msg = f"Value error: {e}"
            logger.error(error_msg)
            print(f"Error: {e}")
        except Exception as e:
            error_msg = f"Unexpected error: {e}"
            logger.error(error_msg)
            print(f"An unexpected error occurred: {e}")

    # Cleanup connections
    logger.info("Closing database connections and SSH tunnels")
    if src_ssh_tunnel:
        src_ssh_tunnel.stop()
    if dest_ssh_tunnel:
        dest_ssh_tunnel.stop()
    src_conn.close()
    dest_conn.close()
    logger.info("=== DATABASE MIGRATION TOOL FINISHED ===")


run_script()
