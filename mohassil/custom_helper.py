from __future__ import annotations

from datetime import datetime

from general_helper import insert_record


def get_governorate_from_national_id(national_id):
    """
    Extract governorate ID from national ID number.
    
    Args:
        national_id (str): National ID number (14 digits)
        
    Returns:
        int: Governorate ID or None if invalid
    """
    if not national_id or len(str(national_id)) != 14:
        return None
        
    # Extract governorate code (8th and 9th digits)
    gov_code = str(national_id)[7:9]
    
    # Handle special case for outside country
    if gov_code == "88":
        return 1
        
    try:
        # Convert governorate code to integer
        return int(gov_code)
    except ValueError:
        return None

def settle_transactions(conn, src_conn, logger=None):
    """
    Perform settlement on transactions by calculating and updating balance columns.
    
    This function processes loan transactions grouped by loan_id and sorted by id.
    It calculates running balances for principal, interest, fees, penalties, and total balance
    based on transaction types.
    
    Args:
        conn: Destination database connection
        src_conn: Source database connection
        logger (logging.Logger, optional): Logger instance for tracking progress
    """
    cursor = conn.cursor()
    src_cursor = src_conn.cursor()
    
    try:
        # Get all loan IDs that have transactions
        message = "Fetching loans with transactions..."
        if logger:
            logger.info(message)
        print(message)
        
        cursor.execute("SELECT DISTINCT loan_id FROM loan_transactions WHERE loan_id IS NOT NULL")
        loan_ids = [row[0] for row in cursor.fetchall()]
        
        if not loan_ids:
            message = "No loans with transactions found."
            if logger:
                logger.info(message)
            print(message)
            return
            
        message = f"Found {len(loan_ids)} loans with transactions to process."
        if logger:
            logger.info(message)
        print(message)
        
        # Process each loan's transactions
        for i, loan_id in enumerate(loan_ids):
            if i % 100 == 0:
                message = f"Processing loan {i+1}/{len(loan_ids)} (ID: {loan_id})"
                if logger:
                    logger.info(message)
                print(message)
                
            # Get all transactions for this loan, ordered by ID
            cursor.execute("""
                SELECT id, loan_transaction_type_id, principal_repaid_derived, interest_repaid_derived,
                       penalties_repaid_derived, amount
                FROM loan_transactions 
                WHERE loan_id = %s 
                ORDER BY id
            """, (loan_id,))
            
            transactions = cursor.fetchall()
            
            if logger:
                logger.debug(f"Processing {len(transactions)} transactions for loan ID {loan_id}")
            
            # Initialize running balances
            principal_balance = 0
            interest_balance = 0
            penalties_balance = 0
            
            # Process each transaction
            for tx in transactions:
                tx_id = tx[0]
                tx_type = tx[1]
                principal_amount = float(tx[2] or 0)
                interest_amount = float(tx[3] or 0)
                penalties_amount = float(tx[4] or 0)
                amount = float(tx[5] or 0)

                # Update balances based on transaction type
                if logger:
                    logger.debug(f"Processing transaction ID {tx_id}, type {tx_type}, amount {amount}")
                
                # Handle different transaction types
                if tx_type == 10:  # Apply Charges - increase penalties balance
                    penalties_balance += amount
                    
                    if logger:
                        logger.debug(f"Apply Charges: Adding penalties {amount}")
                    
                elif tx_type == 1:  # Disbursement - set principal and interest balances
                    principal_balance = principal_amount
                    interest_balance = interest_amount

                    if logger:
                        logger.debug(f"Disbursement: Setting principal to {principal_amount}, interest to {interest_amount}")
                    
                elif tx_type in (2, 6):  # Repayment or Write Off - decrease balances
                    # Decrease principal, interest, and penalties balances
                    principal_balance -= principal_amount
                    interest_balance -= interest_amount
                    penalties_balance -= penalties_amount
                    
                    if logger:
                        logger.debug(f"Repayment/Write-off: Decreasing principal by {principal_amount}, interest by {interest_amount}, penalties by {penalties_amount}")
                
                else:
                    # For any other transaction type, just maintain current balances
                    if logger:
                        logger.debug(f"Other transaction type {tx_type}: Maintaining current balances")
                
                # Calculate total balance
                total_balance = principal_balance + interest_balance + penalties_balance
                
                # Determine if we need to clear repaid values
                clear_repaid = tx_type == 1 or tx_type == 10  # Only clear for disbursements or apply charges
                
                # Build the SQL parameters
                sql_params = [principal_balance, interest_balance, penalties_balance, total_balance]
                
                # Build the SQL query
                sql_query = """
                    UPDATE loan_transactions 
                    SET principal_balance = %s, 
                        interest_balance = %s,
                        penalties_balance = %s,
                        total_balance = %s"""
                
                # Add clearing of repaid values if needed
                if clear_repaid:
                    sql_query += """,
                        principal_repaid_derived = 0,
                        interest_repaid_derived = 0"""
                
                # Add the WHERE clause
                sql_query += """
                    WHERE id = %s"""
                
                # Add the transaction ID to parameters
                sql_params.append(tx_id)
                
                # Execute the update
                cursor.execute(sql_query, sql_params)
            
            # Commit after each loan to avoid long transactions
            if (i + 1) % 100 == 0:
                conn.commit()
                message = f"Committed changes for {i+1} loans"
                if logger:
                    logger.info(message)
                print(message)
        
        # Final commit
        conn.commit()
        message = f"Successfully updated balances for {len(loan_ids)} loans"
        if logger:
            logger.info(message)
        print(message)
        
    except Exception as e:
        conn.rollback()
        error_msg = f"Error during transaction settlement: {str(e)}"
        if logger:
            logger.error(error_msg)
        print(error_msg)
        raise
    finally:
        cursor.close()
        src_cursor.close()

def settle_installments(conn, src_conn, logger=None):
    """
    Perform settlement on installments.
    
    Args:
        conn: Destination database connection
        src_conn: Source database connection
        logger (logging.Logger, optional): Logger instance for tracking progress
    """
    cursor = conn.cursor()
    src_cursor = src_conn.cursor()
    
    try:
        # Step 1: Get loan_keys from source database where resc_type = 3
        message = "Fetching loans with early settlement from source database..."
        if logger:
            logger.info(message)
        print(message)
        
        src_query = """
        SELECT loan_key 
        FROM ilts.c1_loan_info 
        WHERE resc_type = 3
        """
        src_cursor.execute(src_query)
        early_settled_loan_keys = [row[0] for row in src_cursor.fetchall()]
        
        if not early_settled_loan_keys:
            message = "No loans with early settlement found in source database."
            if logger:
                logger.info(message)
            print(message)
            return
            
        message = f"Found {len(early_settled_loan_keys)} loans with early settlement in source database."
        if logger:
            logger.info(message)
        print(message)
        
        # Convert list to comma-separated string for SQL IN clause
        loan_keys_str = "', '".join(str(key) for key in early_settled_loan_keys)
        
        # Step 2: Get corresponding loan IDs from destination database
        dest_query = f"""
        SELECT id, loan_term, external_id
        FROM loans
        WHERE external_id IN ('{loan_keys_str}')
        """
        cursor.execute(dest_query)
        early_settled_loans = cursor.fetchall()
        
        if not early_settled_loans:
            message = "No matching loans found in destination database."
            if logger:
                logger.info(message)
            print(message)
            return
            
        message = f"Found {len(early_settled_loans)} matching loans in destination database."
        if logger:
            logger.info(message)
        print(message)
        
        # Process each loan with early settlement
        for loan in early_settled_loans:
            loan_id = loan[0]
            expected_installments = loan[1]
            external_id = loan[2]
            
            message = f"Processing loan ID: {loan_id} (External ID: {external_id}) - Expected installments: {expected_installments}"
            if logger:
                logger.info(message)
            print(message)
            
            # Handle early settlement (reverse and apply in one function)
            handle_early_settlement(conn, loan_id, logger)
        
        rows_settled = len(early_settled_loans)
        message = f"Settled {rows_settled} loans with early settlement"
        if logger:
            logger.info(message)
        print(message)
        
    except Exception as e:
        conn.rollback()
        error_msg = f"Error during settlement: {str(e)}"
        if logger:
            logger.error(error_msg)
        print(error_msg)
        raise
    finally:
        cursor.close()
        src_cursor.close()

def handle_early_settlement(conn, loan_id, logger=None):
    """
    Handle early settlement for a loan by reversing any existing settlement and applying a new one.
    
    This function:
    1. Identifies and saves the values from the last installment (settlement installment)
    2. Deletes the settlement installment
    3. Decreases the loan term by 1
    4. Applies the settlement amount as fees to the first unpaid installment
    5. Marks that installment as an early settlement
    6. Sets principal_repaid_derived and interest_waived_derived for all installments
    7. Creates necessary transactions for early settlement
    
    Args:
        conn: Database connection
        loan_id: ID of the loan to handle settlement for
        logger (logging.Logger, optional): Logger instance for tracking progress
    """
    cursor = conn.cursor()
    
    try:
        # Find the settlement installment (the last one)
        message = f"Finding settlement installment for loan ID {loan_id}..."
        if logger:
            logger.info(message)
        
        query = """
        SELECT lrs.id, lrs.principal, lrs.interest, lrs.paid_by_date, l.branch_id, l.loan_officer_id
        FROM loan_repayment_schedules lrs
        JOIN loans l ON lrs.loan_id = l.id
        WHERE lrs.loan_id = %s 
        ORDER BY lrs.installment DESC 
        LIMIT 1
        """
        cursor.execute(query, (loan_id,))
        settlement = cursor.fetchone()
        
        if not settlement:
            message = f"No installments found for loan ID {loan_id}"
            if logger:
                logger.warning(message)
            print(f"  {message}")
            return
            
        settlement_id = settlement[0]
        settlement_principal = float(settlement[1] or 0)
        settlement_fee = float(settlement[2] or 0)  # This is actually the early settlement fee
        settlement_paid_by_date = settlement[3]     # Save paid_by_date of the settlement installment
        branch_id = settlement[4]                   # Get branch_id from the installment
        loan_officer_id = settlement[5]             # Get loan_officer_id from the installment
        
        message = f"Found settlement installment ID {settlement_id} with principal={settlement_principal}, early settlement fee={settlement_fee}"
        if logger:
            logger.info(message)
        print(f"  {message}")
        
        # Delete any transactions related to the settlement installment
        delete_tx_query = "DELETE FROM loan_transactions WHERE repayment_schedule_id = %s"
        cursor.execute(delete_tx_query, (settlement_id,))
        deleted_count = cursor.rowcount
        
        message = f"Deleted {deleted_count} transactions associated with settlement installment ID {settlement_id}"
        if logger:
            logger.info(message)
        print(f"  {message}")
        
        # Find the first unpaid installment
        unpaid_query = """
        SELECT id 
        FROM loan_repayment_schedules 
        WHERE loan_id = %s AND (
            (COALESCE(principal, 0) + COALESCE(interest, 0) + COALESCE(fees, 0) + COALESCE(penalties, 0)) - 
            (COALESCE(principal_repaid_derived, 0) + COALESCE(interest_repaid_derived, 0) + COALESCE(fees_repaid_derived, 0) + COALESCE(penalties_repaid_derived, 0)) -
            (COALESCE(principal_written_off_derived, 0) + COALESCE(interest_written_off_derived, 0) + COALESCE(fees_written_off_derived, 0) + COALESCE(penalties_written_off_derived, 0)) -
            (COALESCE(interest_waived_derived, 0) + COALESCE(fees_waived_derived, 0) + COALESCE(penalties_waived_derived, 0))
        ) > 0
        ORDER BY installment ASC
        LIMIT 1
        """
        cursor.execute(unpaid_query, (loan_id,))
        unpaid_result = cursor.fetchone()
        
        if not unpaid_result:
            message = f"No unpaid installments found for loan ID {loan_id}"
            if logger:
                logger.warning(message)
            print(f"  {message}")
            return
            
        unpaid_installment_id = unpaid_result[0]
        
        message = f"Found first unpaid installment ID {unpaid_installment_id}"
        if logger:
            logger.debug(message)
        print(f"  {message}")
        
        # Update the first unpaid installment to mark it as early settlement
        # and add the settlement fee as fees
        update_query = """
        UPDATE loan_repayment_schedules
        SET 
            fees = %s,
            fees_repaid_derived = %s,
            is_early_repayment = TRUE
        WHERE id = %s
        """
        cursor.execute(update_query, (settlement_fee, settlement_fee, unpaid_installment_id))
        
        message = f"Updated installment ID {unpaid_installment_id} with early settlement fee={settlement_fee}"
        if logger:
            logger.info(message)
        print(f"  {message}")
        
        # Delete the settlement installment
        delete_query = "DELETE FROM loan_repayment_schedules WHERE id = %s"
        cursor.execute(delete_query, (settlement_id,))
        
        message = f"Removed early settlement installment ID {settlement_id}"
        if logger:
            logger.info(message)
        print(f"  {message}")
        
        # Decrease loan term by 1
        update_loan_query = """
        UPDATE loans 
        SET loan_term = loan_term - 1
        WHERE id = %s
        """
        cursor.execute(update_loan_query, (loan_id,))
        
        message = f"Decreased loan term by 1 for loan ID {loan_id}"
        if logger:
            logger.info(message)
        print(f"  {message}")
        
        # APPLY EARLY SETTLEMENT PART
        
        # Set principal_repaid_derived, interest_waived_derived, and paid_by_date for installments from the early settled one to the last one
        update_all_installments_query = """
        UPDATE loan_repayment_schedules
        SET 
            principal_repaid_derived = principal,
            interest_waived_derived = interest,
            paid_by_date = %s,
            status = 'payoff'
        WHERE loan_id = %s AND id >= %s
        """
        cursor.execute(update_all_installments_query, (settlement_paid_by_date, loan_id, unpaid_installment_id))
        
        message = f"Updated all installments for loan ID {loan_id} with principal_repaid_derived, interest_waived_derived, paid_by_date, and status='payoff'"
        if logger:
            logger.info(message)
        print(f"  {message}")
        
        # Get total principal and interest for creating transactions
        totals_query = """
        SELECT 
            SUM(principal) as total_principal,
            SUM(interest) as total_interest
        FROM loan_repayment_schedules
        WHERE loan_id = %s AND id >= %s
        """
        cursor.execute(totals_query, (loan_id, unpaid_installment_id))
        totals = cursor.fetchone()
        
        if not totals:
            message = f"Could not calculate totals for loan ID {loan_id}"
            if logger:
                logger.warning(message)
            print(f"  {message}")
            return
            
        total_principal = float(totals[0] or 0)
        total_interest = float(totals[1] or 0)
        
        # Get the latest transaction to use as reference for balances
        last_tx_query = """
        SELECT principal_balance, interest_balance, fees_balance, penalties_balance
        FROM loan_transactions
        WHERE loan_id = %s
        ORDER BY id DESC
        LIMIT 1
        """
        cursor.execute(last_tx_query, (loan_id,))
        last_tx = cursor.fetchone()
        
        # Set default balances to 0 if no previous transaction exists
        last_principal_balance = float(last_tx[0] or 0) if last_tx else 0
        last_interest_balance = float(last_tx[1] or 0) if last_tx else 0
        last_fees_balance = float(last_tx[2] or 0) if last_tx else 0
        last_penalties_balance = float(last_tx[3] or 0) if last_tx else 0
        
        message = f"Last transaction balances - Principal: {last_principal_balance}, Interest: {last_interest_balance}, Fees: {last_fees_balance}, Penalties: {last_penalties_balance}"
        if logger:
            logger.debug(message)
        print(f"  {message}")
        
        # Get current date for transactions or use settlement date if available
        transaction_date = settlement_paid_by_date if settlement_paid_by_date else datetime.now().strftime('%Y-%m-%d')
        transaction_datetime = datetime.strptime(transaction_date, '%Y-%m-%d') if isinstance(transaction_date, str) else transaction_date
        
        # Create transaction for early settlement fee
        if settlement_fee > 0:
            # For fee transaction, increase the fees balance
            new_fees_balance = last_fees_balance + settlement_fee
            
            fee_tx_data = {
                "loan_id": loan_id,
                "amount": settlement_fee,
                "debit": settlement_fee,
                "loan_transaction_type_id": 15,  # Early Settlement Fee
                "created_at": transaction_datetime,
                "updated_at": transaction_datetime,
                "submitted_on": transaction_datetime,
                "branch_id": branch_id,
                "loan_officer_id": loan_officer_id,
                "principal_balance": last_principal_balance,
                "interest_balance": last_interest_balance,
                "fees_balance": new_fees_balance,
                "penalties_balance": last_penalties_balance,
                "total_balance": last_principal_balance + last_interest_balance + new_fees_balance + last_penalties_balance,
                "description": "Apply Early Settlement Fee"
            }
            
            insert_record(cursor, "loan_transactions", fee_tx_data, logger)
            
            # Update last balances for next transaction
            last_fees_balance = new_fees_balance
            
            message = f"Created early settlement fee transaction for loan ID {loan_id} with amount {settlement_fee}"
            if logger:
                logger.info(message)
            print(f"  {message}")
        
        # Create transaction for early settlement (principal + fees)
        total_settlement_amount = total_principal + settlement_fee
        if total_settlement_amount > 0:
            # For settlement transaction, reduce principal and fees balances
            new_principal_balance = 0 #last_principal_balance - total_principal
            new_fees_balance = 0 # last_fees_balance - settlement_fee
            
            settlement_tx_data = {
                "loan_id": loan_id,
                "amount": total_settlement_amount,
                "credit": total_settlement_amount,
                "principal_repaid_derived": total_principal,
                "fees_repaid_derived": settlement_fee,
                "loan_transaction_type_id": 14,  # Early Settlement
                "created_at": transaction_datetime,
                "updated_at": transaction_datetime,
                "submitted_on": transaction_datetime,
                "branch_id": branch_id,
                "loan_officer_id": loan_officer_id,
                "principal_balance": new_principal_balance,
                "interest_balance": last_interest_balance,
                "fees_balance": new_fees_balance,
                "penalties_balance": last_penalties_balance,
                "total_balance": new_principal_balance + last_interest_balance + new_fees_balance + last_penalties_balance,
                "description": "Early settlement"
            }
            
            insert_record(cursor, "loan_transactions", settlement_tx_data, logger)
            
            # Update last balances for next transaction
            last_principal_balance = new_principal_balance
            last_fees_balance = new_fees_balance
            
            message = f"Created early settlement transaction for loan ID {loan_id} with amount {total_settlement_amount} (principal: {total_principal}, fees: {settlement_fee})"
            if logger:
                logger.info(message)
            print(f"  {message}")
        
        # Create transaction for interest waiver
        if total_interest > 0:
            # For interest waiver, reduce interest balance
            new_interest_balance = 0 # last_interest_balance - total_interest
            
            interest_tx_data = {
                "loan_id": loan_id,
                "amount": total_interest,
                "credit": total_interest,
                "interest_repaid_derived": total_interest,
                "loan_transaction_type_id": 4,  # Waive Interest
                "created_at": transaction_datetime,
                "updated_at": transaction_datetime,
                "submitted_on": transaction_datetime,
                "branch_id": branch_id,
                "loan_officer_id": loan_officer_id,
                "principal_balance": 0, # last_principal_balance
                "interest_balance": new_interest_balance,
                "fees_balance": 0, # last_fees_balance,
                "penalties_balance": last_penalties_balance,
                # "total_balance": last_principal_balance + new_interest_balance + last_fees_balance + last_penalties_balance,
                "total_balance": 0,
                "description": "Waive Interest"
            }
            
            insert_record(cursor, "loan_transactions", interest_tx_data, logger)
            
            message = f"Created interest waiver transaction for loan ID {loan_id} with amount {total_interest}"
            if logger:
                logger.info(message)
            print(f"  {message}")
        
        # Update loan status to closed
        update_loan_status_query = """
        UPDATE loans 
        SET status = 'closed' 
        WHERE id = %s
        """
        cursor.execute(update_loan_status_query, (loan_id,))
        
        message = f"Updated loan status to closed for loan ID {loan_id}"
        if logger:
            logger.info(message)
        print(f"  {message}")
        
        conn.commit()
    except Exception as e:
        conn.rollback()
        error_msg = f"Error handling early settlement for loan ID {loan_id}: {str(e)}"
        if logger:
            logger.error(error_msg)
        print(f"  {error_msg}")
        raise
    finally:
        cursor.close()

def create_placeholder_application(cursor, loan_data, source_row, logger=None):
    """
    Create a placeholder loan application when a loan references an application that doesn't exist.
    
    Args:
        cursor: Database cursor
        loan_data (dict): The loan data being processed
        source_row (dict): The original source row data
        logger (logging.Logger, optional): Logger instance
        
    Returns:
        int: ID of the created application or None if creation failed
    """
    try:
        # Extract necessary data from loan
        application_data = {
            "client_id": loan_data.get("client_id"),
            "loan_product_id": loan_data.get("loan_product_id"),
            "branch_id": loan_data.get("branch_id"),
            "loan_officer_id": loan_data.get("loan_officer_id"),
            "created_by_id": loan_data.get("created_by_id"),
            "amount": loan_data.get("approved_amount"),
            "term": loan_data.get("term"),
            "status": "approved",
            "revolving_enabled": False,
            "created_at": loan_data.get("created_at"),
            "updated_at": loan_data.get("updated_at"),
            "external_id": source_row.get("application_key")
        }
        
        # Insert the application record
        application_id = insert_record(cursor, "loan_applications", application_data, logger)
        
        if logger:
            logger.info(f"Created placeholder application with ID: {application_id} for loan with external_id: {source_row.get('loan_key')}")
        
        return application_id
        
    except Exception as e:
        error_msg = f"Error creating placeholder application: {str(e)}"
        if logger:
            logger.error(error_msg)
        print(error_msg)
        return None

def extract_lat_lon_from_wkb(wkb_data, logger=None) -> tuple[float, float] | None:
    """
    Parses a SPECIFIC non-standard SQL Server geography Point hex string or binary data.

    Handles the observed format starting '0xE6100000010F...' or equivalent binary.
    Based *only* on analysis of the example string ending in '...3A40...3F40...',
    this function ASSUMES the following structure after the header:
      - Bytes 6-13 (1st Double): Represents Latitude.
      - Bytes 14-21 (2nd Double): Represents Longitude.
      - Subsequent doubles (Bytes 22-29, 30-37) are ignored.

    WARNING: This interpretation is an educated guess based on limited examples
    and might be incorrect or specific only to certain data sources. It does
    NOT handle the standard '...010C...' format.

    Args:
        wkb_data: The WKB data, either as hex string or binary bytes
        logger: Optional logger for debugging

    Returns:
        A tuple containing (latitude, longitude) as floats based on the assumed
        structure, or None if parsing fails or the prefix doesn't match.

    Raises:
        ValueError: If the hex string contains non-hexadecimal characters or
                    has an odd number of digits (after removing '0x').
    """
    import binascii
    import struct
    
    # Handle binary input
    if isinstance(wkb_data, bytes):
        if logger:
            logger.debug(f"Processing binary WKB data of length {len(wkb_data)}")
        binary_data = wkb_data
    # Handle string input
    elif isinstance(wkb_data, str):
        if logger:
            logger.debug(f"Processing WKB hex string: {wkb_data[:20]}...")

        if wkb_data.startswith('0x') or wkb_data.startswith('0X'):
            wkb_data = wkb_data[2:]
            if logger:
                logger.debug("Removed '0x' prefix from hex string.")

        # Specific non-standard prefix observed in the user example
        specific_prefix = "E6100000010F"
        if not wkb_data.startswith(specific_prefix):
            if logger:
                logger.warning(f"Input string does not start with the specific prefix '{specific_prefix}' this function handles.")
            print(f"Error: Input string does not start with the specific prefix '{specific_prefix}' this function handles.")
            return None

        # Check minimum length for Header(6) + Lat(8) + Lon(8) = 22 bytes = 44 hex chars
        # Let's check for the full 4 doubles observed = 6+32=38 bytes = 76 hex chars
        min_len = 76
        if len(wkb_data) < min_len:
            if logger:
                logger.warning(f"Hex string is too short: {len(wkb_data)} chars. Need at least {min_len} characters.")
            print(f"Error: Hex string is too short for the assumed '0F' structure (Header + 4 Doubles). Need at least {min_len} characters.")
            return None

        try:
            # Convert hex string to bytes
            binary_data = binascii.unhexlify(wkb_data)
            if logger:
                logger.debug(f"Successfully converted hex to {len(binary_data)} bytes of binary data.")
        except ValueError as e:
            # Catches errors from unhexlify (odd length, non-hex)
            if logger:
                logger.error(f"Error converting hex to bytes: {str(e)}")
            print(f"Error converting hex to bytes: {e}")
            return None
    else:
        if logger:
            logger.error(f"Input must be a string or bytes, got {type(wkb_data)}.")
        print(f"Error: Input must be a string or bytes, got {type(wkb_data)}.")
        return None

    try:
        # Check if we have enough data for the coordinates
        if len(binary_data) < 22:  # Need at least header + lat + lon
            if logger:
                logger.error(f"Binary data too short: {len(binary_data)} bytes")
            print(f"Error: Binary data too short: {len(binary_data)} bytes")
            return None
            
        # Extract bytes based on the ASSUMED structure for this '0F' variant:
        # Bytes 6-13 : Assumed Latitude (Double 1)
        # Bytes 14-21: Assumed Longitude (Double 2)
        lat_bytes = binary_data[6:14]
        lon_bytes = binary_data[14:22]

        if len(lon_bytes) != 8 or len(lat_bytes) != 8:
            if logger:
                logger.error("Could not extract 8 bytes for assumed Latitude and Longitude.")
            print("Error: Could not extract 8 bytes for assumed Latitude and Longitude.")
            return None

        # Unpack as little-endian doubles ('<d')
        latitude = struct.unpack('<d', lat_bytes)[0]
        longitude = struct.unpack('<d', lon_bytes)[0]
        if logger:
            logger.debug(f"Unpacked coordinates: Latitude={latitude}, Longitude={longitude}")

        # Optional: Basic validation print
        if not (-90 <= latitude <= 90):
            if logger:
                logger.warning(f"Parsed Latitude ({latitude}) is outside valid range [-90, 90].")
            print(f"Warning: Parsed Latitude ({latitude}) is outside valid range [-90, 90].")
        if not (-180 <= longitude <= 180):
            if logger:
                logger.warning(f"Parsed Longitude ({longitude}) outside valid range [-180, 180].")
            print(f"Warning: Parsed Longitude ({longitude}) outside valid range [-180, 180].")

        if logger:
            logger.info(f"Successfully extracted coordinates: Latitude={latitude}, Longitude={longitude}")
        return latitude, longitude

    except (struct.error, IndexError) as e:
        if logger:
            logger.error(f"Error parsing binary data structure: {str(e)}")
        print(f"Error parsing binary data structure: {e}")
        return None
