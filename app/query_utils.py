# query_utils.py

import re  # For using regular expressions for pattern matching
from typing import Optional, List, Dict  # For type hinting
import logging  # For better debugging

# Configure logging level (instead of print statements)
logging.basicConfig(level=logging.DEBUG)

# Define common synonyms for fields for SQL databases
FIELD_SYNONYMS = {
    "sale": "transaction_qty, unit_price",
    "sales": "transaction_qty, unit_price",
    "total sales": "transaction_qty * unit_price",
    "location": "store_location",
    "store location": "store_location",
    "units": "transaction_qty",
    # Changed to "product_name" to match with your collection fields
    "product": "product_name",
    "date": "transaction_date",
    "total sales": "transaction_qty, unit_price",
    "store": "store_location",
    "user name": "user_name",
    "customer": "customer_name",
    "registration date": "registration_date",
    "product name": "product_name",
    "order date": "order_date"
}

# Define common synonyms for fields for NoSQL databases
NOSQL_FIELD_SYNONYMS = {
    "sale": "sales_amount",
    "total sales": "sales_amount",
    "customer": "customer_name",
    "email": "customer_email",
    "phone": "phone_number",
    "product": "product_name",
    "category": "category",
    "price": "price",
    "quantity": "quantity",
    "order": "orders",
    "date": "order_date"
}

# Define potential fields in the SQL dataset
POTENTIAL_FIELDS = [
    "transaction_qty", "store_location", "unit_price",
    "product_category", "transaction_date", "user_name",
    "user_email", "registration_date", "customer_name",
    "product_name", "order_date"
]

# Define potential fields in the NoSQL dataset
NOSQL_POTENTIAL_FIELDS = [
    "sales_amount", "customer_name", "customer_email", "phone_number",
    "product_name", "category", "price", "quantity", "order_date", "order_id"
]

# Define synonyms for table names (applies for both SQL and NoSQL)
TABLE_SYNONYMS = {
    "sales": "transactions",
    "transactions": "transactions",
    "messages": "messages",
    "coffee_sales": "coffee_sales",
    "stores": "stores",
    "users": "users",
    "customers": "customers",
    "products": "products",
    "orders": "orders"
}


def extract_fields_from_input(input_text: str, db_type: str = 'general', context: str = 'general') -> Optional[List[str]]:
    # Convert input to lowercase for case-insensitive matching
    input_text = input_text.lower()

    # To hold extracted fields
    extracted_fields = set()

    # Choose the appropriate field synonym dictionary and potential fields based on db_type
    if db_type == 'sql':
        relevant_field_synonyms = FIELD_SYNONYMS
        potential_fields = POTENTIAL_FIELDS
    elif db_type == 'nosql':
        relevant_field_synonyms = NOSQL_FIELD_SYNONYMS
        potential_fields = NOSQL_POTENTIAL_FIELDS
    else:
        relevant_field_synonyms = FIELD_SYNONYMS
        potential_fields = POTENTIAL_FIELDS

    # Check for exact field matches first
    for field in potential_fields:
        if field.lower() in input_text:
            extracted_fields.add(field)

    # Check synonyms if exact match not found
    for synonym, actual_field in relevant_field_synonyms.items():
        if synonym.lower() in input_text:
            if ',' in actual_field:
                # Split fields if it contains multiple fields (e.g., "transaction_qty, unit_price")
                fields = [f.strip() for f in actual_field.split(',')]
                extracted_fields.update(fields)
            elif '*' in actual_field:
                # Add both fields if it involves multiplication for aggregation
                fields = actual_field.split('*')
                extracted_fields.update([f.strip() for f in fields])
            else:
                extracted_fields.add(actual_field)

    # Debug statement to print extracted fields before filtering
    logging.debug(
        f"[DEBUG] Extracted fields before filtering for context '{context}': {extracted_fields}")

    # If the context is aggregation, only return numeric fields for aggregation
    if context == 'aggregation':
        if db_type == 'sql':
            numeric_fields = {"transaction_qty", "unit_price"}
        elif db_type == 'nosql':
            numeric_fields = {"sales_amount", "price", "quantity"}
        extracted_fields = extracted_fields.intersection(numeric_fields)

    # Debug statement to print extracted fields after filtering (for aggregation)
    logging.debug(
        f"[DEBUG] Extracted fields after filtering for context '{context}': {extracted_fields}")

    # Return extracted fields if any exist; otherwise, return None
    return list(extracted_fields) if extracted_fields else None


def extract_table_from_input(input_text: str) -> Optional[str]:
    # Convert input to lowercase for case-insensitive matching
    input_text = input_text.lower()

    # Debug log to see input for extraction
    logging.debug(f"[DEBUG] Input for table extraction: '{input_text}'")

    # Sort synonyms by length in descending order to prioritize longer matches
    sorted_table_synonyms = sorted(
        TABLE_SYNONYMS.items(), key=lambda x: len(x[0]), reverse=True
    )

    # Check for table synonyms and match to actual table
    for synonym, actual_table in sorted_table_synonyms:
        if re.search(r'\b' + re.escape(synonym) + r'\b', input_text):
            logging.debug(
                f"[DEBUG] Matched table synonym: '{synonym}' -> '{actual_table}'")
            return actual_table

    # If no match found, attempt a fallback by splitting the input text into tokens
    tokens = input_text.split()
    for token in tokens:
        if token in TABLE_SYNONYMS:
            logging.debug(
                f"[DEBUG] Matched table synonym from tokens: '{token}' -> '{TABLE_SYNONYMS[token]}'")
            return TABLE_SYNONYMS[token]

    # If still no match found, try to guess the table from common keywords or set a default
    logging.debug(
        "[DEBUG] No table synonym matched. Unable to determine the table name. Defaulting to 'products'.")
    return "products"
