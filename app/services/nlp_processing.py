import re
import spacy
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
from nltk.stem import WordNetLemmatizer
from nltk import download

# Import extract_entities from new entity_extraction.py
from app.services.entity_extraction import extract_entities

from app.query_utils import (
    extract_fields_from_input,
    extract_table_from_input,
    FIELD_SYNONYMS,
    NOSQL_FIELD_SYNONYMS,
    POTENTIAL_FIELDS,
    NOSQL_POTENTIAL_FIELDS,
    TABLE_SYNONYMS
)

# Ensure that necessary NLTK corpora are downloaded
download('stopwords')
download('punkt')

# Initialize spaCy language model
nlp = spacy.load("en_core_web_sm")
lemmatizer = WordNetLemmatizer()


def preprocess_input(user_input):
    # Tokenize input and filter stopwords only if not essential
    tokens = word_tokenize(user_input)
    stop_words = set(stopwords.words('english'))
    essential_words = {"how", "many"}  # Essential words to keep

    filtered_tokens = [
        lemmatizer.lemmatize(word) for word in tokens
        if word.lower() not in stop_words or word.lower() in essential_words
    ]
    return filtered_tokens

# Import extract_entities from new entity_extraction.py


def match_query_pattern(preprocessed_input, db_type):
    # Debugging the preprocessed input tokens
    print(f"[DEBUG] Preprocessed Input Tokens: {preprocessed_input}")

    # Join preprocessed input back to a single string for pattern matching
    joined_input = ' '.join(preprocessed_input).lower()
    print(f"[DEBUG] Processing Input: {joined_input}")
    
    # Extract the potential fields and table from the input dynamically
    aggregate_fields = extract_fields_from_input(
        joined_input, db_type=db_type, context='aggregation')
    
    # Define field sets for SQL tables
    fields_from_transactions = {
        "transaction_qty", "unit_price", "transaction_date", "store_id", "user_id"}
    fields_from_users = {"user_name", "user_email", "registration_date"}

    # Step 1: Detect if 'customer details' is in the user query
    if 'customer details' in joined_input:
        if db_type == 'nosql':
            # Create an aggregation query for MongoDB to join orders with customer details
            query = {
                "operation": "aggregate",
                "collection": "orders",
                "pipeline": [
                    {
                        "$lookup": {
                            "from": "customers",  # Assuming the customer collection is named 'customers'
                            "localField": "customer_id",
                            "foreignField": "customer_id",
                            "as": "customer_details"
                        }
                    },
                    {
                        "$unwind": "$customer_details"  # Flatten the customer details if needed
                    }
                ]
            }
        elif db_type == 'sql':
            # SQL join query based on provided SQL table structure
            query = {
                "operation": "select_with_join",
                "table": "transactions",
                "join_table": "users",
                "on": "transactions.user_id = users.user_id",
                # Example columns to include in the result
                "columns": ["transactions.*", "users.user_name", "users.user_email"]
            }

        print(
            f"[DEBUG] Detected 'customer details' in query. Generated Query: {query}")
        return query
    
    # Check if fields from both the 'transactions' and 'users' tables are mentioned
    transaction_fields_in_query = fields_from_transactions.intersection(
        preprocessed_input)
    user_fields_in_query = fields_from_users.intersection(preprocessed_input)

    # Generate JOIN query if fields from both 'users' and 'transactions' are in the input
    if transaction_fields_in_query and user_fields_in_query:
        print(
            f"[DEBUG] Detected fields from both 'users' and 'transactions'. Generating JOIN query.")
        query = {
            "operation": "select_with_join",
            "table": "transactions",
            "join_table": "users",
            "on": "transactions.user_id = users.user_id",
            # Include all fields from both sets detected in the user input
            "columns": list(transaction_fields_in_query.union(user_fields_in_query))
        }
        print(f"[DEBUG] Generated JOIN Query: {query}")
        return query

    # Extract table using synonyms if it exists in the input
    selected_table = None
    for synonym, canonical_name in TABLE_SYNONYMS.items():
        if synonym in joined_input:
            selected_table = canonical_name
            print(
                f"[DEBUG] Matched table synonym: '{synonym}' -> '{canonical_name}'")
            break

    # Explicitly check for table names in the input as a fallback mechanism
    if not selected_table:
        if re.search(r'\b(transactions|messages|users|sales|coffee_sales|stores|customers|products|orders)\b', joined_input):
            selected_table = re.search(
                r'\b(transactions|messages|users|sales|coffee_sales|stores|customers|products|orders)\b', joined_input).group()
            print(
                f"[DEBUG] Fallback table name extraction found: {selected_table}")

    # Set default table if none is explicitly mentioned
    default_table = selected_table if selected_table else "transactions"
    print(f"[DEBUG] Default table set to: {default_table}")

    # Initialize group_by_field, order_by_field, and filter_conditions to None or empty list by default
    group_by_field = None
    order_by_field = None
    order_by_direction = 'ASC'  # Default to ascending if not specified
    filter_conditions = {}

    # Initialize aggregate function and selected_aggregate_field
    aggregate_function = None
    selected_aggregate_field = None

        # Check if user query is related to SUM operation
    if re.search(r'\b(total sales amount|total sales|sum|total amount)\b', joined_input) and aggregate_fields:
        # Here, aggregate_fields should contain both 'transaction_qty' and 'unit_price'
        if "transaction_qty" in aggregate_fields and "unit_price" in aggregate_fields:
            selected_aggregate_field = "transaction_qty * unit_price"
        else:
            selected_aggregate_field = next(iter(aggregate_fields), None)

        if not selected_aggregate_field:
            print("[ERROR] Aggregate field is required for this operation.")
            return {
                "error": "Required aggregate fields not found for SUM operation."
            }

        aggregate_function = "sum"
        print("[DEBUG] Matched AGGREGATE SUM query")
        return {
            "operation": "aggregate",
            "aggregate_function": aggregate_function,
            "aggregate_field": selected_aggregate_field,
            "table": default_table,
            "group_by_field": group_by_field,
            "order_by_field": order_by_field,
            "order_by_direction": order_by_direction,
            "filter_conditions": filter_conditions,
        }

    # Match SQL pattern queries for AVERAGE
    elif re.search(r'\b(average|avg)\b', joined_input) and aggregate_fields:
        selected_aggregate_field = (
            "transaction_qty * unit_price"
            if "transaction_qty" in aggregate_fields and "unit_price" in aggregate_fields
            else next((f for f in aggregate_fields if f in ["transaction_qty", "unit_price"]), None)
        )
        aggregate_function = "average"
        print("[DEBUG] Matched AVERAGE query")
        return {
            "operation": "aggregate",
            "aggregate_function": aggregate_function,
            "aggregate_field": selected_aggregate_field,
            "table": default_table,
            "group_by_field": group_by_field,
            "order_by_field": order_by_field,
            "order_by_direction": order_by_direction,
            "filter_conditions": filter_conditions,
        }

    # Identify group-by field if present
    potential_fields = POTENTIAL_FIELDS if db_type == 'sql' else NOSQL_POTENTIAL_FIELDS
    if "each" in joined_input or "by" in joined_input:
        for field in potential_fields:
            if field in joined_input and field not in aggregate_fields:
                group_by_field = field
                break  # Assume only one group-by field for simplicity

    print(f"[DEBUG] group_by_field detected: {group_by_field}")

    # Order-by pattern extraction
    order_by_pattern = re.search(
        r'(?:order\s+by|ordered\s+by|ordered)\s+([a-zA-Z_]+)(?:\s+(asc|desc))?', joined_input, re.IGNORECASE)
    if order_by_pattern:
        print(f"[DEBUG] ORDER BY pattern matched: {order_by_pattern.group()}")
        order_by_field = order_by_pattern.group(1)
        order_by_direction = order_by_pattern.group(
            2).upper() if order_by_pattern.group(2) else 'ASC'
        # Normalize order_by_field using FIELD_SYNONYMS if possible
        relevant_field_synonyms = FIELD_SYNONYMS if db_type == 'sql' else NOSQL_FIELD_SYNONYMS
        if order_by_field in relevant_field_synonyms:
            order_by_field = relevant_field_synonyms[order_by_field]
            print(
                f"[DEBUG] Normalized order_by_field using FIELD_SYNONYMS: {order_by_field}")
        # Validate the order_by_field
        if order_by_field not in potential_fields:
            print(
                f"[DEBUG] Order-by field '{order_by_field}' is not valid. Resetting.")
            order_by_field = None
    else:
        print(f"[DEBUG] ORDER BY pattern not found in input: '{joined_input}'")

    print(
        f"[DEBUG] Final order_by_field detected: {order_by_field}, order_by_direction: {order_by_direction}")

    # Extract filter conditions from the input text
    greater_than_pattern = re.search(
        r'(\w+)\s+(greater\s+than|>)\s+(\d+)', joined_input)
    less_than_pattern = re.search(
        r'(\w+)\s+(less\s+than|<)\s+(\d+)', joined_input)
    equal_to_pattern = re.search(
        r'(\w+)\s+(equal\s+to|=)\s+(\d+)', joined_input)

    # Extract greater than condition
    if greater_than_pattern:
        field = greater_than_pattern.group(1)
        value = int(greater_than_pattern.group(3))
        filter_conditions[field] = {"$gt": value}
        print(
            f"[DEBUG] Extracted filter condition (greater than): Field '{field}', Value '{value}'")

    # Extract less than condition
    if less_than_pattern:
        field = less_than_pattern.group(1)
        value = int(less_than_pattern.group(3))
        filter_conditions[field] = {"$lt": value}
        print(
            f"[DEBUG] Extracted filter condition (less than): Field '{field}', Value '{value}'")

    # Extract equal to condition
    if equal_to_pattern:
        field = equal_to_pattern.group(1)
        value = int(equal_to_pattern.group(3))
        filter_conditions[field] = {"$eq": value}
        print(
            f"[DEBUG] Extracted filter condition (equal to): Field '{field}', Value '{value}'")

    # Match COUNT pattern queries for SQL and NoSQL
    if re.search(r'\b(how\s+many|count|total\s+number\s+of|number\s+of)\b', joined_input):
        print("[DEBUG] Matched COUNT query")
        return {
            "operation": "count",
            "table" if db_type == 'sql' else "collection": default_table,
            "filter_conditions": filter_conditions,
        }

    # Match AGGREGATE queries for SQL and NoSQL
    if aggregate_function and selected_aggregate_field:
        print(
            f"[DEBUG] Matched AGGREGATE {aggregate_function.upper()} query with field: {selected_aggregate_field}")
        if db_type == 'sql':
            return {
                "operation": "aggregate",
                "aggregate_function": aggregate_function,
                "aggregate_field": selected_aggregate_field,
                "table": default_table,
                "group_by_field": group_by_field,
                "order_by_field": order_by_field,
                "order_by_direction": order_by_direction,
                "filter_conditions": filter_conditions,
            }
        elif db_type == 'nosql':
            pipeline = []
            if filter_conditions:
                filter_criteria = {field: condition for field,
                                   condition in filter_conditions.items()}
                pipeline.append({"$match": filter_criteria})
            pipeline_stage = {
                "$group": {
                    "_id": None,
                    f"{aggregate_function}_value": {f"${aggregate_function}": f"${selected_aggregate_field}"}
                }
            }
            pipeline.append(pipeline_stage)
            return {
                "operation": "aggregate",
                "collection": default_table,
                "pipeline": pipeline
            }

    # Match SELECT ALL pattern queries
    if re.search(r'\b(show|list|get|select)\s*(all|transactions|sales|messages)?\b', joined_input):
        print("[DEBUG] Matched SELECT ALL query")
        if db_type == 'sql':
            return {
                "operation": "select_all",
                "table": default_table,
                "order_by_field": order_by_field,
                "order_by_direction": order_by_direction,
                "filter_conditions": filter_conditions,
            }
        elif db_type == 'nosql':
            # For NoSQL, adjust to use find() operation equivalent
            print("[DEBUG] Executing SELECT ALL query for NoSQL")
            return {
                "operation": "find_all",
                "collection": default_table,
                "filter_conditions": filter_conditions,
                "order_by_field": order_by_field,
                "order_by_direction": order_by_direction
            }


    print("[DEBUG] No match found")
    return None



# Updated process_query function to include db_type parameter
def process_query(query, db_type):
    try:
        # Step 1: Preprocess the user input for consistent formatting
        preprocessed_input = preprocess_input(query)
        print(f"[DEBUG] Preprocessed Input Tokens: {preprocessed_input}")

    except Exception as e:
        raise ValueError(f"Error during preprocessing input: {str(e)}")

    try:
        # Step 2: Match the preprocessed input to a query pattern
        matched_query = match_query_pattern(preprocessed_input, db_type)
        print(f"[DEBUG] Matched Query Pattern: {matched_query}")

    except Exception as e:
        raise ValueError(f"Error during matching query pattern: {str(e)}")

    # Step 3: Extract entities from the original user input
    try:
        entities = extract_entities(query)
        print(f"[DEBUG] Extracted Entities: {entities}")

    except Exception as e:
        raise ValueError(f"Error during entity extraction: {str(e)}")

    # Check if a valid query pattern was found
    if not matched_query:
        raise ValueError("No valid query pattern found.")

    # Combine matched query and extracted entities into a response
    return {
        "matched_query": matched_query,
        "entities": entities
    }
