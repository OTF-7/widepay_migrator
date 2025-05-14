import logging
import datetime
from general_helper import (
    insert_record,
    get_record_details_by_id, 
    get_record_value,
)
from custom_helper import (
    get_governorate_from_national_id,
    create_placeholder_application,
    extract_lat_lon_from_wkb
)

class CustomLogic:
    def __init__(self):
        self.dest_conn = None
        self.logger = logging.getLogger('migration')  # Default logger
        self.skipped_transactions = 0
        self.inserted_transactions = 0
        self.skipped_types_count = {}
        self.logger.info("CustomLogic instance initialized")

    def process_columns(self, migration_name, row_data, source_row=None, cursor=None, src_cursor=None):
        """
        Override this method to add custom logic for specific migrations.
        Args:
            src_cursor:
            migration_name (str): The name of the migration.
            row_data (dict): The processed row data being inserted (target columns).
            source_row (dict, optional): The original source row data for reference.
            cursor: Database cursor to use for lookups (optional)
        Returns:
            dict: Modified row data after processing.
        """
        self.logger.debug(f"Processing columns for migration: {migration_name}")
        
        # Common lookups for multiple migration types
        if source_row:
            # Branch lookup - used in multiple migrations
            if source_row.get("branch_code") and migration_name in ["officers", "clients", "loan_applications", "loans", "transactions"]:
                self.logger.debug(f"Looking up branch with code: {source_row['branch_code']}")
                branch_id = get_record_value(
                    table="branches", 
                    condition=f"external_id = '{source_row['branch_code']}'",
                    column="id",
                    cursor=cursor,
                    conn=self.dest_conn,
                    logger=self.logger
                )
                if branch_id:
                    self.logger.debug(f"Found branch_id: {branch_id}")
                    row_data["branch_id"] = branch_id
                else:
                    self.logger.warning(f"Branch not found for code: {source_row['branch_code']}")
            
            if source_row.get("org_branch_code") and migration_name in ["clients", "loans", "transactions"]:
                self.logger.debug(f"Looking up original branch with code: {source_row['org_branch_code']}")
                org_branch_id = get_record_value(
                    table="branches", 
                    condition=f"external_id = '{source_row['org_branch_code']}'",
                    column="id",
                    cursor=cursor,
                    conn=self.dest_conn,
                    logger=self.logger
                )
                if org_branch_id:
                    self.logger.debug(f"Found original branch_id: {org_branch_id}")
                    row_data["old_branch_code"] = org_branch_id
                else:
                    self.logger.warning(f"Original branch not found for code: {source_row['org_branch_code']}")

            # Client lookup
            if source_row.get("client_key") and migration_name in ["loan_applications", "loans"]:
                self.logger.debug(f"Looking up client with key: {source_row['client_key']}")
                client_id = get_record_value(
                    table="clients", 
                    condition=f"external_id = '{source_row['client_key']}'",
                    column="id",
                    cursor=cursor,
                    conn=self.dest_conn,
                    logger=self.logger
                )
                if client_id:
                    self.logger.debug(f"Found client_id: {client_id}")
                    row_data["client_id"] = client_id
                else:
                    self.logger.warning(f"Client not found for key: {source_row['client_key']}")

            # Loan officer lookup
            if source_row.get("officer_key") and migration_name in ["clients", "loan_applications", "loans", "transactions"]:
                loan_officer_id = get_record_value(
                    table="users", 
                    condition=f"external_id = '{source_row['officer_key']}'",
                    column="id",
                    cursor=cursor,
                    conn=self.dest_conn,
                    logger=self.logger
                )
                if loan_officer_id:
                    row_data["loan_officer_id"] = loan_officer_id
                    row_data["created_by_id"] = loan_officer_id
            
            # Loan product lookup
            if source_row.get("loan_type_code") and migration_name in ["loan_applications", "loans"]:
                loan_product_id = get_record_value(
                    table="loan_products", 
                    condition=f"external_id = '{source_row['loan_type_code']}'",
                    column="id",
                    cursor=cursor,
                    conn=self.dest_conn,
                    logger=self.logger
                )
                if loan_product_id:
                    row_data["loan_product_id"] = loan_product_id
                    
            # Application lookup for loans
            if source_row.get("application_key") and migration_name == "loans":
                application_id = get_record_value(
                    table="loan_applications", 
                    condition=f"external_id = '{source_row['application_key']}'",
                    column="id",
                    cursor=cursor,
                    conn=self.dest_conn,
                    logger=self.logger
                )
                if application_id:
                    row_data["application_id"] = application_id
                elif "client_id" in row_data and "loan_product_id" in row_data:
                    # Create a placeholder application if it doesn't exist
                    application_id = create_placeholder_application(
                        cursor, 
                        row_data, 
                        source_row,
                        self.logger
                    )
                    if application_id:
                        row_data["application_id"] = application_id

        # Migration-specific logic
        if migration_name == "officers":
            # Add email field for officers
            if "name" in row_data:
                name_part = str(row_data["name"]).lower().replace(' ', '.')
                row_data["email"] = f"{name_part}@sandah.org"
            # Add gender field (1 male and 2 female)
            row_data["gender"] = bool(row_data['gender'] == 1)
            # Set role id to officers id type (60)
            row_data["role_id"] = 60

        elif migration_name == "clients":
            # Insert user using the general insert_record function
            user_data = {
                "name": row_data["name"],
                "national_id": row_data["national_id"],
                "branch_id": row_data["branch_id"],
                "gender": row_data["gender"],
                "created_at": row_data["created_at"],
                "role_id": 3
            }
            self.logger.debug(f"Inserting user record for client: {user_data['name']}")
            user_id = insert_record(cursor, "users", user_data, self.logger)
            row_data["user_id"] = user_id
            self.logger.debug(f"User created with ID: {user_id}")

            # Insert wallet for the user
            wallet_data = {
                "user_id": user_id,
                "role_id": 3,
                "currency_id": 1,
                "wallet_type": "cash",
                "amount": 0,
                "active": True,
                "created_at": row_data["created_at"]
            }
            insert_record(cursor, "wallets", wallet_data, self.logger)

            row_data["gender"] = bool(source_row['gender'] == 1)
            row_data["is_guarantor"] = bool(source_row['client_status'] == 0)
            
            # Create address from parts, filtering out empty values
            address_parts = [
                source_row['home_add_1'],
                source_row['home_add_2'],
                source_row['home_add_3']
            ]
            address_parts = [part for part in address_parts if part and str(part).strip()]
            row_data["address"] = ", ".join(address_parts) if address_parts else None
            
            # Get birthplace from national ID
            row_data["birthplace_id"] = get_governorate_from_national_id(row_data["national_id"])
            
            # Extract latitude and longitude from location if available
            if source_row.get('home_geography'):
                home_geography = source_row['home_geography']
                lat, lon = extract_lat_lon_from_wkb(home_geography, logger=self.logger)

                row_data["latitude"] = lat
                row_data["approved_latitude"] = lat
                row_data["longitude"] = lon
                row_data["approved_longitude"] = lon
            else:
                row_data["latitude"] = 0.00000000
                row_data["approved_latitude"] = 0.00000000
                row_data["longitude"] = 0.00000000
                row_data["approved_longitude"] = 0.00000000
            
            # default values
            row_data["corporate_id"] = 0
            row_data["country_id"] = 63
            row_data["display_name"] = row_data['name']
            row_data["third_name"] = ''
            row_data["active"] = True
            row_data["status"] = "active"
            marital_status = row_data.get("marital_status_id")
            row_data["marital_status_id"] = 6 if marital_status is None or marital_status == 0 or marital_status == "" else marital_status
            qualification = row_data.get("qualification_id")
            row_data["qualification_id"] = 8 if qualification is None or qualification == 0 or qualification == "" else qualification
        
        elif migration_name == "loan_products":
            row_data["decimals"] = 0
            row_data["product_type"] = "commercial"
            row_data["active"] = 1
            row_data["penalty_id"] = 3
            row_data["fund_id"] = 1
            
            def flat_to_declining(flat_rate, periods):
                """Convert flat interest rate to declining balance rate."""
                self.logger.debug(f"Converting flat rate {flat_rate}% over {periods} periods to declining balance")
                flat_rate = float(flat_rate) / 100  # Convert percentage to decimal
                # Formula: r = 2R/(n+1) where R is flat rate, n is number of periods
                declining_rate = (2 * flat_rate * periods) / (periods + 1)
                return round(declining_rate * 100, 4)  # Convert back to percentage
            
            # Convert flat interest rates to declining balance using respective terms
            flat_default = float(row_data.get("flat_default_interest_rate", 0))
            flat_minimum = float(row_data.get("flat_minimum_interest_rate", 0))
            flat_maximum = float(row_data.get("flat_maximum_interest_rate", 0))
            
            default_term = int(row_data.get("default_loan_term", 12))
            minimum_term = int(row_data.get("minimum_loan_term", 12))
            maximum_term = int(row_data.get("maximum_loan_term", 12))
            
            row_data["default_interest_rate"] = flat_to_declining(flat_default, default_term) if flat_default else 0.0
            row_data["minimum_interest_rate"] = flat_to_declining(flat_minimum, minimum_term) if flat_minimum else 0.0
            row_data["maximum_interest_rate"] = flat_to_declining(flat_maximum, maximum_term) if flat_maximum else 0.0
            
            row_data["interest_rate_type"] = "year"
            row_data["repayment_frequency"] = 1
            row_data["repayment_frequency_type"] = "months"
            row_data["amortization_method"] = "equal_installments"
            row_data["interest_methodology"] = "declining_balance"
            row_data["loan_transaction_processing_strategy_id"] = 24 if row_data["loan_transaction_processing_strategy_id"] == 2 else 23

        elif migration_name == "loan_applications":
            # Application status mapping
            application_status = source_row.get("application_status")
            if application_status == 1:
                status = 'approved'
            elif application_status == 2:
                status = 'rejected'
            else:
                status = 'pending'
            row_data["status"] = status
            row_data["revolving_enabled"] = False

        elif migration_name == "loans":
            if source_row and source_row.get("bs_div_2_code"):
                self.logger.debug(f"Looking up loan activity with category: {source_row['bs_div_1_code']} and integration ID: {source_row['bs_div_2_code']}")
                loan_activity_id = get_record_value(
                    table="loan_activities", 
                    condition=f"loan_activity_category_id = '{source_row['bs_div_1_code']}' AND integration_loan_activity_id = '{source_row['bs_div_2_code']}'",
                    column="id",
                    cursor=cursor,
                    conn=self.dest_conn,
                    logger=self.logger
                )
                if loan_activity_id:
                    self.logger.debug(f"Found loan_activity_id: {loan_activity_id}")
                    row_data["loan_activity_id"] = loan_activity_id
                else:
                    self.logger.warning(f"Loan activity not found for category: {source_row['bs_div_1_code']} and integration ID: {source_row['bs_div_2_code']}")

            # Determine if the loan is renewed
            client_id = row_data.get("client_id")
            if client_id:
                existing_loans_count = get_record_value(
                    table="loans",
                    condition=f"client_id = {client_id}",
                    column="COUNT(*)",
                    cursor=cursor,
                    conn=self.dest_conn,
                    logger=self.logger
                )
                row_data["is_renewed"] = existing_loans_count > 0
                self.logger.debug(f"Set is_renewed to {row_data['is_renewed']} for client_id {client_id}")

            # Loan status mapping
            loan_status = source_row.get("loan_status")
            loan_cond = source_row.get("loan_cond")
            
            if loan_cond == 2 and source_row.get("fully_paid_date") is not None:
                status = 'written_off'
                self.logger.info(f"Setting loan status to written_off based on loan_cond={loan_cond} and fully_paid_date is present")
            elif loan_status == 0:
                status = 'submitted'
            elif loan_status == 1:
                status = 'active'
            elif loan_status == 5:
                status = 'closed'
            elif loan_status == 6:
                status = 'withdrawn'
            else:
                status = 'pending'
            row_data["status"] = status

            if status != 'approved':
                row_data['principal_disbursed_derived'] = row_data['approved_amount']

            # Get loan application details if available
            if "application_id" in row_data:
                application_key = source_row.get('application_key')
                app_columns = [
                    "req_am", "req_no", "br_deputy_note", "officer_supervisor_note",
                    "loan_gen_user", "loan_gen_date", "user_name", "dec_user", "dec_date", "br_deputy_user_name",
                    "officer_supervisor_user_name", "br_deputy_bus_location"
                ]
                
                application_details = get_record_details_by_id(src_cursor, "ilts.c1_loan_application", application_key,
                                                            app_columns, logger=self.logger, id_name='application_key')
                    
                dest_app_columns = [
                    "loan_product_id", "client_id", "branch_id"
                ]
                dist_application_details = get_record_details_by_id(cursor, "loan_applications", row_data.get('application_id'),
                                                                    dest_app_columns, logger=self.logger)
                
                if application_details:
                    row_data["applied_amount"] = application_details.get("req_am")
                    row_data["applied_loan_term"] = application_details.get("req_no")
                    row_data["approved_notes"] = application_details.get("br_deputy_note")
                    row_data["report"] = application_details.get("officer_supervisor_note")
                    row_data["created_at"] = application_details.get("loan_gen_date")
                    # Extract latitude and longitude from location if available
                    if application_details.get('br_deputy_bus_location'):
                        br_deputy_bus_location = application_details.get('br_deputy_bus_location')
                        lat, lon = extract_lat_lon_from_wkb(br_deputy_bus_location, logger=self.logger)
                        row_data["latitude"] = lat
                        row_data["longitude"] = lon

                    row_data["created_by_id"] = get_record_value(
                                                    table="users", 
                                                    condition=f"name = '{application_details.get('loan_gen_user')}'",
                                                    column="id",
                                                    cursor=cursor,
                                                    logger=self.logger
                                                )
                    row_data["submitted_by_user_id"] = get_record_value(
                                                    table="users", 
                                                    condition=f"name = '{application_details.get('loan_gen_user')}'",
                                                    column="id",
                                                    cursor=cursor,
                                                    logger=self.logger
                                                )
                    row_data["approved_by_user_id"] = get_record_value(
                                                    table="users", 
                                                    condition=f"name = '{application_details.get('dec_user')}'",
                                                    column="id",
                                                    cursor=cursor,
                                                    logger=self.logger
                                                )

                    username = row_data.get('disbursed_by_user_id')
                    self.logger.debug(f"Looking up user with username: {username}")
                    
                    # Reset disbursed_by_user_id before setting new value
                    row_data["disbursed_by_user_id"] = 1
                    
                    disbursed_by_user_id = get_record_value(
                                                    table="users", 
                                                    condition=f"email = '{username}@sandah.org'",
                                                    column="id",
                                                    cursor=cursor,
                                                    logger=self.logger
                                                )
                    row_data["disbursed_by_user_id"] = disbursed_by_user_id
                    if "loan_product_id" not in row_data and dist_application_details.get("loan_product_id"):
                        row_data["loan_product_id"] = application_details.get("loan_product_id")
                    if "client_id" not in row_data and dist_application_details.get("client_id"):
                        row_data["client_id"] = application_details.get("client_id")
                    if "branch_id" not in row_data and dist_application_details.get("branch_id"):
                        row_data["branch_id"] = application_details.get("branch_id")
            
            # Get loan product details if available
            if "loan_product_id" in row_data:
                loan_product_id = row_data["loan_product_id"]
                product_columns = [
                    "product_type", "fund_id", "repayment_frequency", "repayment_frequency_type",
                    "interest_rate_type", "interest_methodology", "amortization_method",
                    "decimals", "loan_transaction_processing_strategy_id"
                ]
                product_details = get_record_details_by_id(cursor, "loan_products", loan_product_id, product_columns, logger=self.logger)
                
                if product_details:
                    row_data["fund_id"] = product_details.get("fund_id", 1)
                    row_data["repayment_frequency"] = product_details.get("repayment_frequency", 1)
                    row_data["repayment_frequency_type"] = product_details.get("repayment_frequency_type", "months")
                    row_data["interest_rate_type"] = product_details.get("interest_rate_type", "year")
                    row_data["interest_methodology"] = product_details.get("interest_methodology", "declining_balance")
                    row_data["amortization_method"] = product_details.get("amortization_method", "equal_installments")
                    row_data["decimals"] = product_details.get("decimals", 0)
                    row_data["loan_transaction_processing_strategy_id"] = product_details.get("loan_transaction_processing_strategy_id", 23)
                else:
                    row_data["fund_id"] = 1
                    row_data["repayment_frequency"] = 1
                    row_data["repayment_frequency_type"] = "months"
                    row_data["interest_rate_type"] = "year"
                    row_data["interest_methodology"] = "declining_balance"
                    row_data["amortization_method"] = "equal_installments"
                    row_data["decimals"] = 0
                    row_data["loan_transaction_processing_strategy_id"] = 24 if row_data.get("loan_transaction_processing_strategy_id") == 2 else 23

            # Get client details if client_id is available (e.g., to get wallet_id)
            if "client_id" in row_data:
                user_id = get_record_value(
                        table="clients", 
                        condition=f"id = '{row_data['client_id']}'",
                        column="user_id",
                        cursor=cursor,
                        conn=self.dest_conn,
                        logger=self.logger)
                wallet_id = get_record_value(
                        table="wallets", 
                        condition=f"user_id = '{user_id}'",
                        column="id",
                        cursor=cursor,
                        conn=self.dest_conn,
                        logger=self.logger)
                if wallet_id:
                    row_data["wallet_id"] = wallet_id

                client_key = get_record_value(
                    table="clients",
                    condition=f"id = '{row_data['client_id']}'",
                    column="external_id",
                    cursor=cursor,
                    conn=self.dest_conn,
                    logger=self.logger)

                client_columns = [
                    "bus_name", "bus_add_1", "bus_add_2", "bus_add_3"
                ]

                client_details = get_record_details_by_id(src_cursor, "ilts.c1_client_info_table", client_key, client_columns,
                                                          logger=self.logger, id_name="client_key")

                if client_details:
                    row_data["activity_name"] = client_details.get("bus_name") if client_details.get("bus_name") is not None else ''
                    # Create address from parts, filtering out empty values
                    address_parts = [
                        client_details.get("bus_add_1"),
                        client_details.get("bus_add_2"),
                        client_details.get("bus_add_3")
                    ]
                    address_parts = [part for part in address_parts if part and str(part).strip()]
                    row_data["project_address"] = ", ".join(address_parts) if address_parts else None

            # These fields are always set regardless of loan product or application
            row_data["loan_purpose_id"] = 1
            
            # Set first_payment_date to one month after disbursed_on_date without using relativedelta
            if "disbursed_on_date" in row_data and row_data["disbursed_on_date"]:
                # Get the disbursed date
                disbursed_date = row_data["disbursed_on_date"]
                
                # Calculate one month later using standard datetime
                month = disbursed_date.month + 1
                year = disbursed_date.year
                
                # Handle month overflow
                if month > 12:
                    month = 1
                    year += 1
                
                # Create new date with same day, but next month
                try:
                    # Handle edge cases like Jan 31 -> Feb 28
                    first_payment_date = disbursed_date.replace(year=year, month=month)
                except ValueError:
                    # If the day doesn't exist in the target month, use the last day of that month
                    if month == 12:
                        first_payment_date = datetime.datetime(year + 1, 1, 1) - datetime.timedelta(days=1)
                    else:
                        first_payment_date = datetime.datetime(year, month + 1, 1) - datetime.timedelta(days=1)
                
                row_data["first_payment_date"] = first_payment_date
                self.logger.debug(f"Set first_payment_date to {row_data['first_payment_date']}")
            else:
                self.logger.warning("disbursed_on_date not available, cannot set first_payment_date")

            # Prepare charge data if app_charge exists and is valid
            if source_row and "app_charge" in source_row and source_row["app_charge"] is not None:
                try:
                    app_charge_amount = float(source_row["app_charge"])
                    if app_charge_amount > 0:
                        # Use loan_date if available, otherwise use current time
                        created_at_val = source_row.get("loan_date", datetime.datetime.now()) 
                        # Ensure created_at_val is a datetime object if it comes from source_row
                        if isinstance(created_at_val, datetime.date) and not isinstance(created_at_val, datetime.datetime):
                             created_at_val = datetime.datetime.combine(created_at_val, datetime.datetime.min.time())
                             self.logger.debug("Converted date to datetime for charge creation")
                        elif not isinstance(created_at_val, datetime.datetime):
                             # Attempt conversion or default if format is unexpected
                             try:
                                 created_at_val = datetime.datetime.fromisoformat(str(created_at_val))
                                 self.logger.debug("Converted string to datetime for charge creation")
                             except (ValueError, TypeError):
                                 self.logger.warning(f"Could not parse date value: {created_at_val}, using current time")
                                 created_at_val = datetime.datetime.now()


                        charge_data = {
                            "loan_charge_id": 4,
                            "loan_charge_type_id": 1,
                            "loan_charge_option_id": 7,
                            "amount": app_charge_amount,
                            "amount_paid_derived": app_charge_amount,
                            "calculated_amount": app_charge_amount,
                            "is_paid": True,
                            "created_at": created_at_val, 
                            "updated_at": created_at_val # Set updated_at as well
                        }
                        # Store charge data temporarily, prefixed to avoid column name conflicts
                        row_data["_charge_to_add"] = charge_data 
                except ValueError:
                    print(f"Warning: Invalid app_charge value '{source_row['app_charge']}' for loan with external_id {source_row.get('loan_key')}. Skipping charge.")
                    self.logger.warning(f"Invalid app_charge value '{source_row['app_charge']}' for loan with external_id {source_row.get('loan_key')}. Skipping charge.")
        
        elif migration_name == "installments":
            # Convert negative values to positive
            for field in ['principal', 'principal_repaid_derived', 'interest', 'interest_repaid_derived', 'fees', 'fees_repaid_derived']:
                if field in row_data and row_data[field] is not None:
                    # Convert to float first
                    value = float(row_data[field])
                    # If negative, make positive
                    if value < 0:
                        row_data[field] = abs(value)
                        self.logger.info(f"Converted negative {field} to positive: {abs(value)}")

            principal = float(row_data['principal'])
            principal_repaid_derived = float(row_data['principal_repaid_derived'])
            
            if source_row and source_row.get("loan_key"):
                loan_id = get_record_value(
                    table="loans", 
                    condition=f"external_id = '{source_row['loan_key']}'",
                    column="id",
                    cursor=cursor,
                    conn=self.dest_conn,
                    logger=self.logger
                )
                if loan_id:
                    row_data["loan_id"] = loan_id
            # Determine status based on inst_cond and inst_status
            if source_row and 'inst_cond' in source_row:
                # Get loan status if loan_key is available
                loan_status = None
                if row_data.get("loan_id"):
                    loan_status = get_record_value(
                        table="loans", 
                        condition=f"id = '{row_data['loan_id']}'",
                        column="status",
                        cursor=cursor,
                        conn=self.dest_conn,
                        logger=self.logger
                    )
                    self.logger.debug(f"Found loan status: {loan_status} for loan_id: {row_data['loan_id']}")
                
                # Check both loan status and installment condition
                if loan_status == 'written_off' and source_row['inst_cond'] == 2:
                    row_data['status'] = 'written_off'
                    self.logger.info(f"Setting installment status to written_off based on loan_status={loan_status} and inst_cond={source_row['inst_cond']}")
                else:  # inst_cond is 0 (normal)
                    if source_row.get('inst_status') == 8:
                        row_data['status'] = 'rescheduled'
                        self.logger.info(f"Setting installment status to rescheduled based on inst_status={source_row['inst_status']}")
                    else:
                        # Determine if active or closed based on payment status
                        if principal > principal_repaid_derived:
                            row_data['status'] = 'active'
                            row_data['paid_by_date'] = None
                            self.logger.info(f"Setting installment status to active (principal={principal}, paid={principal_repaid_derived})")
                        else:
                            row_data['status'] = 'closed'
                            self.logger.info(f"Setting installment status to closed (principal={principal}, paid={principal_repaid_derived})")
            else:
                # Fallback if inst_cond is not available
                if principal > principal_repaid_derived:
                    row_data['status'] = 'active'
                    row_data['paid_by_date'] = None
                    self.logger.info("Setting installment status to active (fallback)")
                else:
                    row_data['status'] = 'closed'
                    self.logger.info("Setting installment status to closed (fallback)")

            interest = float(row_data['interest'])
            interest_repaid_derived = float(row_data['interest_repaid_derived'])
            principal -= interest
            principal_repaid_derived -= interest_repaid_derived
            row_data['interest'] = interest
            row_data['interest_repaid_derived'] = interest_repaid_derived
            row_data['principal'] = principal
            row_data['principal_repaid_derived'] = principal_repaid_derived
            
        elif migration_name == "transactions":
            if source_row:
                if source_row.get("loan_key"):
                    loan_id = get_record_value(
                        table="loans", 
                        condition=f"external_id = '{source_row['loan_key']}'",
                        column="id",
                        cursor=cursor,
                        conn=self.dest_conn,
                        logger=self.logger
                    )
                    if loan_id:
                        row_data["loan_id"] = loan_id
                        
                        # Check if the loan status is withdrawn or rejected
                        loan_status = get_record_value(
                            table="loans", 
                            condition=f"id = {loan_id}",
                            column="status",
                            cursor=cursor,
                            conn=self.dest_conn,
                            logger=self.logger
                        )
                        
                        if loan_status in ["withdrawn", "rejected"]:
                            row_data["reversed"] = True
                            self.logger.info(f"Setting transaction as reversed because loan (ID: {loan_id}) has status: {loan_status}")
                        else:
                            row_data["reversed"] = False

                    if source_row.get("installment_key"):
                        repayment_schedule_id = get_record_value(
                            table="loan_repayment_schedules",
                            condition=f"external_id = '{source_row['installment_key']}'",
                            column="id",
                            cursor=cursor,
                            conn=self.dest_conn,
                            logger=self.logger
                        )
                        if loan_id:
                            row_data["repayment_schedule_id"] = repayment_schedule_id
                
                # Ensure amount, interest_repaid_derived, or penalties_repaid_derived are not null, set to 0 if they are
                if row_data.get('amount') is None:
                    row_data['amount'] = 0

                if row_data.get('interest_repaid_derived') is None:
                    row_data['interest_repaid_derived'] = 0

                if row_data.get('penalties_repaid_derived') is None:
                    row_data['penalties_repaid_derived'] = 0
                
                  # Map transaction types from old to new system
                # Only process transactions with types that have a mapping
                trans_type_mapping = {
                    1: 2,           # Repayment
                    3: 1,           # Disbursement
                    7: 6,           # Write Off
                    17: 10,         # Apply Charges
                    # Cancellation transaction types map to the same target types
                    2: 2,           # Cancel Repayment
                    4: 1,           # Cancel Disbursement
                    8: 6,           # Cancel Write Off
                    18: 10,         # Cancel Apply Charges
                }
                
                # Define which transaction types are cancellations
                cancellation_types = {2, 4, 8, 18}
                
                # Get transaction type from source row
                trans_type = source_row.get('trans_act')
                
                # Add debugging to see what transaction types are being encountered
                self.logger.info(f"Processing transaction with type: {trans_type}")
                print(f"Processing transaction with type: {trans_type}")
                
                # Convert trans_type to int for dictionary lookup
                try:
                    trans_type_int = int(trans_type) if trans_type is not None else None
                    trans_type = trans_type_int  # Update trans_type to the integer version
                except (ValueError, TypeError):
                    self.logger.warning(f"Could not convert transaction type '{trans_type}' to integer")
                    
                # Create a separate transaction for penalties if penalties_repaid_derived is greater than zero
                penalties_amount = float(row_data.get('penalties_repaid_derived', 0))
                if penalties_amount > 0 and 'loan_id' in row_data:
                    self.logger.info(f"Creating separate penalties transaction with amount: {penalties_amount}")
                    
                    # Determine if this is a cancellation transaction
                    is_cancellation = trans_type_int in cancellation_types if trans_type_int is not None else False
                    
                    # Calculate adjusted amount based on cancellation status
                    adjusted_penalties_amount = -penalties_amount if is_cancellation else penalties_amount
                    
                    # Create transaction data for penalties
                    penalties_tx_data = {
                        "loan_id": row_data['loan_id'],
                        "amount": adjusted_penalties_amount,
                        "debit": adjusted_penalties_amount,
                        "penalties_repaid_derived": adjusted_penalties_amount,
                        "loan_transaction_type_id": 12,
                        "created_at": row_data.get('created_at', datetime.datetime.now()),
                        "updated_at": datetime.datetime.now(),
                        "submitted_on": row_data.get('submitted_on', datetime.datetime.now()),
                        "branch_id": row_data.get('branch_id'),
                        "loan_officer_id": row_data.get('loan_officer_id'),
                        "description": "Apply Penalty" if not is_cancellation else "Cancel Apply Penalty"
                    }
                    
                    # Insert the penalties transaction
                    try:
                        insert_record(cursor, "loan_transactions", penalties_tx_data, self.logger)
                        self.logger.info(f"Successfully created penalties transaction for loan ID {row_data['loan_id']} with amount {penalties_tx_data['amount']}")
                    except Exception as e:
                        self.logger.error(f"Failed to create penalties transaction: {str(e)}")
                
                # Create a separate transaction for apply charges if transaction type is 10 (Apply Charges)
                if trans_type_int in [17, 18] and 'loan_id' in row_data:  # Apply Charges or Cancel Apply Charges
                    fees_amount = float(row_data.get('amount', 0))
                    if fees_amount > 0:
                        self.logger.info(f"Creating separate apply charges transaction with amount: {fees_amount}")
                        
                        # Determine if this is a cancellation transaction (type 18)
                        is_cancellation = trans_type_int == 18
                        
                        # Calculate adjusted amount based on cancellation status
                        adjusted_fees_amount = -fees_amount if is_cancellation else fees_amount
                        
                        # Create transaction data for apply charges
                        fees_tx_data = {
                            "loan_id": row_data['loan_id'],
                            "amount": adjusted_fees_amount,
                            "credit": adjusted_fees_amount,
                            "fees_repaid_derived": adjusted_fees_amount,
                            "loan_transaction_type_id": 2,
                            "created_at": row_data.get('created_at', datetime.datetime.now()),
                            "updated_at": datetime.datetime.now(),
                            "submitted_on": row_data.get('submitted_on', datetime.datetime.now()),
                            "branch_id": row_data.get('branch_id'),
                            "loan_officer_id": row_data.get('loan_officer_id'),
                            "description": "Pay Charges" if not is_cancellation else "Cancel Pay Charges"
                        }
                        
                        # Insert the fees transaction
                        try:
                            insert_record(cursor, "loan_transactions", fees_tx_data, self.logger)
                            self.logger.info(f"Successfully created apply charges transaction for loan ID {row_data['loan_id']} with amount {fees_amount}")
                        except Exception as e:
                            self.logger.error(f"Failed to create apply charges transaction: {str(e)}")
                
                # Convert negative values to positive
                for field in ['amount', 'interest_repaid_derived', 'penalties_repaid_derived']:
                    if field in row_data and row_data[field] is not None:
                        # Convert to float first
                        value = float(row_data[field])
                        # If negative, make positive
                        if value < 0:
                            row_data[field] = abs(value)
                            self.logger.info(f"Converted negative {field} to positive: {abs(value)}")

                # Handle datetime fields properly to avoid insertion errors
                row_data['updated_at'] = datetime.datetime.now()
                
                # Increase created_at by one hour if it's a datetime
                if 'created_at' in row_data and isinstance(row_data['created_at'], datetime.datetime):
                    row_data['created_at'] = row_data['created_at'] + datetime.timedelta(hours=1)
                
                # Set submitted_on to created_at if not already set
                if 'submitted_on' not in row_data or row_data['submitted_on'] is None:
                    row_data['submitted_on'] = row_data['created_at']

                # Convert to float before calculation
                amount = float(row_data['amount'])
                interest_repaid = float(row_data['interest_repaid_derived'])
                row_data['principal_repaid_derived'] = amount - interest_repaid
                row_data['amount'] = amount + float(row_data['penalties_repaid_derived'])
                
                # Skip transactions that don't have a mapping
                if trans_type not in trans_type_mapping:
                    self.skipped_transactions += 1
                    skip_msg = f"Skipping transaction with type {trans_type} - no mapping available"
                    self.logger.info(skip_msg)
                    print(skip_msg)
                    
                    # Count transaction types that are being skipped
                    self.skipped_types_count[trans_type] = self.skipped_types_count.get(trans_type, 0) + 1
                    
                    # Print summary of skipped types every 100 transactions
                    if self.skipped_transactions % 100 == 0:
                        summary = f"Skipped transaction types summary: {self.skipped_types_count}"
                        self.logger.info(summary)
                        print(summary)
                    
                    # Set a special flag to indicate this row should be skipped
                    row_data["_skip_this_row"] = True
                    return row_data  # Return the modified row_data with the skip flag
                
                # For cancellation transaction types, make the amount negative if it's not already
                if trans_type in cancellation_types:
                    # Make amount negative if it's positive - ensure it's a float first
                    try:
                        amount_val = float(row_data['amount'])
                        if amount_val > 0:
                            row_data['amount'] = -amount_val
                            self.logger.info(f"Made amount negative for cancellation transaction type {trans_type}: {row_data['amount']}")
                    except (ValueError, TypeError):
                        self.logger.warning(f"Could not convert amount '{row_data['amount']}' to float for comparison")
                    
                    # Make interest_repaid_derived negative if it's positive
                    try:
                        interest_val = float(row_data['interest_repaid_derived'])
                        if interest_val > 0:
                            row_data['interest_repaid_derived'] = -interest_val
                            self.logger.info(f"Made interest_repaid_derived negative for cancellation transaction type {trans_type}: {row_data['interest_repaid_derived']}")
                    except (ValueError, TypeError):
                        self.logger.warning(f"Could not convert interest_repaid_derived '{row_data['interest_repaid_derived']}' to float for comparison")
                    
                    # Make penalties_repaid_derived negative if it's positive
                    try:
                        penalties_val = float(row_data['penalties_repaid_derived'])
                        if penalties_val > 0:
                            row_data['penalties_repaid_derived'] = -penalties_val
                            self.logger.info(f"Made penalties_repaid_derived negative for cancellation transaction type {trans_type}: {row_data['penalties_repaid_derived']}")
                    except (ValueError, TypeError):
                        self.logger.warning(f"Could not convert penalties_repaid_derived '{row_data['penalties_repaid_derived']}' to float for comparison")
                    
                    # Recalculate principal_repaid_derived with negative values
                    try:
                        amount = float(row_data['amount'])
                        interest = float(row_data['interest_repaid_derived'])
                        row_data['principal_repaid_derived'] = amount - interest
                        self.logger.info(f"Recalculated principal_repaid_derived for cancellation: {row_data['principal_repaid_derived']}")
                    except (ValueError, TypeError):
                        self.logger.warning("Could not recalculate principal_repaid_derived due to conversion errors")
                
                # Count this as a transaction that will be inserted
                self.inserted_transactions += 1
                
                # Set the transaction type in the target data
                row_data['loan_transaction_type_id'] = trans_type_mapping[trans_type]
                
                # Set debit/credit based on transaction type
                # Disbursement (1) and Apply Charges (10) are credits, others are debits
                if trans_type_mapping[trans_type] in [1, 10]:
                    self.logger.debug(f"Setting transaction as credit: {row_data['amount']}")
                    row_data['debit'] = row_data['amount']
                    row_data['credit'] = 0
                else:
                    self.logger.debug(f"Setting transaction as debit: {row_data['amount']}")
                    row_data['debit'] = 0
                    row_data['credit'] = row_data['amount']
                
                # Print summary after every 100 transactions
                if (self.skipped_transactions + self.inserted_transactions) % 100 == 0:
                    summary = f"Transactions progress: {self.inserted_transactions} inserted, {self.skipped_transactions} skipped"
                    self.logger.info(summary)
                    print(summary)
                
        self.logger.debug(f"Finished processing columns for {migration_name}")
        return row_data
