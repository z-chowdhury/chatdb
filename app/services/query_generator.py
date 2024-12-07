# app/services/query_generator.py
import re
from bson import ObjectId
from app.db import connect_mysql, connect_mongo
from app.services.nlp_processing import preprocess_input, match_query_pattern
from app.query_utils import (
    extract_fields_from_input,
    extract_table_from_input,
    FIELD_SYNONYMS,
    NOSQL_FIELD_SYNONYMS,
    POTENTIAL_FIELDS,
    NOSQL_POTENTIAL_FIELDS,
    TABLE_SYNONYMS
)


def validate_field_and_table(db_type, table_name, field_name=None):
    if db_type == 'sql':
        connection = connect_mysql()
        try:
            with connection.cursor() as cursor:
                # Check if table exists
                cursor.execute(f"SHOW TABLES LIKE '{table_name}'")
                if not cursor.fetchone():
                    raise ValueError(f"Table '{table_name}' does not exist.")

                # Check if field exists, if provided
                if field_name:
                    cursor.execute(
                        f"SHOW COLUMNS FROM {table_name} LIKE '{field_name}'")
                    if not cursor.fetchone():
                        raise ValueError(
                            f"Field '{field_name}' does not exist in table '{table_name}'.")
        except Exception as e:
            raise ValueError(f"Validation error: {str(e)}")
        finally:
            connection.close()
    elif db_type == 'nosql':
        db = connect_mongo()
        # Check if collection exists in NoSQL
        if table_name not in db.list_collection_names():
            raise ValueError(f"Collection '{table_name}' does not exist.")
        # Additional validation for fields can be added here if required

        
# Helper function to generate SQL WHERE clause


# Add the helper function here

def generate_nosql_filter_criteria(filter_conditions):
    filter_criteria = {}
    for field, condition in filter_conditions.items():
        operator, value = list(condition.items())[0]
        if operator == "$gt":
            filter_criteria[field] = {"$gt": value}
        elif operator == "$lt":
            filter_criteria[field] = {"$lt": value}
        elif operator == "$eq":
            filter_criteria[field] = value
    return filter_criteria


def generate_sql_where_clause(filter_conditions):
    """
    Generate a SQL WHERE clause from a list of filter conditions.
    
    Args:
        filter_conditions (list of tuples): A list where each tuple contains
                                            (field, operator, value), e.g., ('price', 'greater', 500).
                                             
    Returns:
        str: A WHERE clause for SQL, e.g., " WHERE price > 500".
    """
    if not filter_conditions:
        return ""

    where_clauses = []
    for field, operator, value in filter_conditions:
        if operator == "greater":
            where_clauses.append(f"{field} > {value}")
        elif operator == "less":
            where_clauses.append(f"{field} < {value}")
        elif operator == "equal":
            where_clauses.append(f"{field} = {value}")

    # Combine individual clauses with " AND "
    return " WHERE " + " AND ".join(where_clauses)

# Updated generate_query function


def generate_query(query_pattern, db_type):
    # Extract table/collection name, aggregate field, group-by field, order-by field, and order-by direction
    table_name = query_pattern.get('table') or query_pattern.get('collection')
    aggregate_function = query_pattern.get('aggregate_function')
    aggregate_field = query_pattern.get('aggregate_field')
    group_by_field = query_pattern.get('group_by_field')
    order_by_field = query_pattern.get('order_by_field')
    order_by_direction = query_pattern.get('order_by_direction', 'ASC')
    filter_conditions = query_pattern.get('filter_conditions', {})
    operation = query_pattern.get('operation')
    join_table = query_pattern.get('join_table')
    join_on = query_pattern.get('join_on')

    # Validate the operation and db_type
    if not table_name:
        raise ValueError(
            "Table or collection name is required for this operation.")

    # Handle mismatches in operation and db_type
    if db_type == 'sql' and operation == 'find':
        # Convert to select_all as appropriate for SQL
        operation = 'select_all'

    # Handle FIND_ALL operation for NoSQL
    if operation == 'find_all':
        if db_type == 'nosql':
            # Generate find_all query for NoSQL
            query = {
                "operation": "find",
                "collection": table_name,
                "filter": {}  # No filter for find_all
            }
            # Add sorting if applicable
            if order_by_field:
                sort_direction = 1 if order_by_direction.lower() == 'asc' else -1
                query["sort"] = {order_by_field: sort_direction}
            return query
        else:
            raise ValueError(
                f"Unsupported operation 'find_all' for SQL database type.")

    # Handle SELECT ALL (FIND) operation for SQL and NoSQL
    if operation == 'select_all':
        if db_type == 'sql':
            query = f"SELECT * FROM {table_name}"
            # Add JOIN if applicable
            if join_table and join_on:
                query += f" JOIN {join_table} ON {join_on}"
            # Add ORDER BY if applicable
            if order_by_field:
                query += f" ORDER BY {order_by_field} {order_by_direction}"
            return query
        elif db_type == 'nosql':
            # Convert 'select_all' to a 'find' operation with no filter
            return {
                "operation": "find",
                "collection": table_name,
                "filter": {}  # No filter conditions imply selecting all
            }

    elif operation == 'find':
        if db_type == 'nosql':
            # Handle NoSQL FIND operation
            filter_criteria = generate_nosql_filter_criteria(filter_conditions)
            query = {
                "operation": "find",
                "collection": table_name,
                "filter": filter_criteria
            }
            # Add sorting if applicable
            if order_by_field:
                sort_direction = 1 if order_by_direction.lower() == 'asc' else -1
                query["sort"] = {order_by_field: sort_direction}
            return query
        else:
            raise ValueError(
                f"Unsupported operation 'find' for SQL database type.")

    # Handle COUNT operation
    elif operation == 'count':
        if db_type == 'sql':
            query = f"SELECT COUNT(*) AS total_count FROM {table_name}"
            query += generate_sql_where_clause(filter_conditions)
            return query

        elif db_type == 'nosql':
            filter_criteria = generate_nosql_filter_criteria(filter_conditions)
            return {
                "operation": "count",
                "collection": table_name,
                "filter": filter_criteria
            }

    # Handle AGGREGATE operations
    elif operation == 'aggregate':
        if not aggregate_function or not aggregate_field:
            raise ValueError(
                "Aggregate function and field are required for this operation.")

        if db_type == 'sql':
            if aggregate_function in ['sum', 'average', 'max', 'min']:
                function_sql = {
                    'sum': 'SUM',
                    'average': 'AVG',
                    'max': 'MAX',
                    'min': 'MIN'
                }[aggregate_function]

                alias_name = f"{aggregate_function}_value"
                query = f"SELECT {function_sql}({aggregate_field}) AS {alias_name} FROM {table_name}"

                # Add GROUP BY if applicable
                if group_by_field:
                    query = f"SELECT {group_by_field}, {function_sql}({aggregate_field}) AS {alias_name} FROM {table_name} GROUP BY {group_by_field}"

                # Add filter conditions if they exist
                query += generate_sql_where_clause(filter_conditions)

                # Add ORDER BY if applicable
                if order_by_field:
                    query += f" ORDER BY {order_by_field} {order_by_direction}"

                return query

            else:
                raise ValueError(
                    f"Unsupported aggregate function: {aggregate_function}")

        elif db_type == 'nosql':
            # Define the pipeline for NoSQL (e.g., MongoDB)
            pipeline = []

            # Add match stage if filter conditions exist
            if filter_conditions:
                filter_criteria = generate_nosql_filter_criteria(
                    filter_conditions)
                pipeline.append({"$match": filter_criteria})

            # Add group stage
            operation_field = {
                'average': '$avg',
                'sum': '$sum',
                'max': '$max',
                'min': '$min'
            }[aggregate_function]

            group_stage = {
                "$group": {
                    "_id": None if not group_by_field else f"${group_by_field}",
                    f"{aggregate_function}_{aggregate_field}": {
                        operation_field: f"${aggregate_field}"
                    }
                }
            }
            pipeline.append(group_stage)

            # Add sort stage if applicable
            if order_by_field:
                sort_direction = 1 if order_by_direction.lower() == 'asc' else -1
                pipeline.append({"$sort": {order_by_field: sort_direction}})

            return {
                "operation": "aggregate",
                "collection": table_name,
                "pipeline": pipeline
            }


    # Handle JOIN operations for SQL and NoSQL
    elif operation == 'join':
        if not join_table or not join_on:
            raise ValueError(
                "Join table and join condition are required for join operations.")

        if db_type == 'sql':
            query = f"SELECT * FROM {table_name} JOIN {join_table} ON {join_on}"
            # Add filter conditions if they exist
            query += generate_sql_where_clause(filter_conditions)
            return query

        elif db_type == 'nosql':
            # Define the pipeline for NoSQL join (e.g., MongoDB $lookup)
            local_field, foreign_field = join_on.split('=')
            local_field = local_field.strip()
            foreign_field = foreign_field.strip()

            pipeline = [
                {
                    "$lookup": {
                        "from": join_table,
                        "localField": local_field,
                        "foreignField": foreign_field,
                        "as": "joined_result"
                    }
                }
            ]

            # Add match stage if filter conditions exist
            if filter_conditions:
                filter_criteria = generate_nosql_filter_criteria(filter_conditions)
                pipeline.append({"$match": filter_criteria})

            return {
                "operation": "aggregate",
                "collection": table_name,
                "pipeline": pipeline
            }


    # If the operation or db_type is unsupported, raise an error
    raise ValueError(
        f"Unsupported operation or database type: {operation}, {db_type}")


def execute_query(query):
    if isinstance(query, tuple):  # SQL query with parameters
        connection = connect_mysql()
        try:
            with connection.cursor() as cursor:
                cursor.execute(query[0], query[1])
                result = cursor.fetchall()
            connection.commit()
        except Exception as e:
            raise ValueError(f"SQL execution error: {str(e)}")
        finally:
            connection.close()
        return result

    elif isinstance(query, dict):  # MongoDB query (NoSQL)
        db = connect_mongo()
        collection = db[query['collection']]
        try:
            if query.get('operation') == 'count':
                result = [
                    {"count": collection.count_documents(query.get('filter', {}))}]
            elif query.get('operation') == 'find':
                result = list(collection.find(query.get('filter', {})))

                # Convert ObjectId to string for each document
                for doc in result:
                    if '_id' in doc and isinstance(doc['_id'], ObjectId):
                        doc['_id'] = str(doc['_id'])
            elif query.get('operation') == 'aggregate':
                result = list(collection.aggregate(query.get('pipeline', [])))

                # Convert ObjectId to string for each document
                for doc in result:
                    if '_id' in doc and isinstance(doc['_id'], ObjectId):
                        doc['_id'] = str(doc['_id'])
            elif query.get('operation') == 'find_with_customer_details':
                # Join "orders" with "customers" using aggregation pipeline
                pipeline = [
                    {
                        "$lookup": {
                            "from": "customers",  # Name of the other collection
                            "localField": "customer_id",  # Field in "orders" collection
                            "foreignField": "customer_id",  # Field in "customers" collection
                            "as": "customer_details"  # Alias for joined data
                        }
                    },
                    {
                        "$unwind": "$customer_details"  # Unwind the result to simplify data structure
                    }
                ]
                result = list(collection.aggregate(pipeline))

                # Convert ObjectId to string for each document in the aggregation result
                for doc in result:
                    if '_id' in doc and isinstance(doc['_id'], ObjectId):
                        doc['_id'] = str(doc['_id'])
            else:
                raise ValueError(
                    f"Unsupported MongoDB operation: {query.get('operation')}")
        except Exception as e:
            raise ValueError(f"NoSQL execution error: {str(e)}")

        print(f"[DEBUG] Result after processing: {result}")
        return result

    else:
        raise ValueError("Unsupported query type.")




def handle_user_query(user_input, db_type):
    # Validate db_type before proceeding
    if db_type not in ['sql', 'nosql']:
        return f"Error: Unsupported database type '{db_type}'. Valid options are 'sql' or 'nosql'."

    # Step 1: Preprocess the user input and match it to a query pattern
    try:
        print(
            f"[DEBUG] User input received: {user_input}, Database type: {db_type}")
        preprocessed_input = preprocess_input(user_input)
        print(f"[DEBUG] Preprocessed input: {preprocessed_input}")

        matched_query = match_query_pattern(preprocessed_input, db_type)
        if not matched_query:
            return "No matching query pattern found."
    except Exception as e:
        print(
            f"[ERROR] Error during preprocessing or matching query pattern: {str(e)}")
        return f"Error: Unable to process query. Details: {str(e)}"

    # Step 2: Generate the query based on the matched pattern and db_type
    try:
        final_query = generate_query(matched_query, db_type)
        print(f"[DEBUG] Generated query: {final_query}")
    except Exception as e:
        print(f"[ERROR] Error generating query: {str(e)}")
        return f"Error: Unable to generate query. Details: {str(e)}"

    # Step 3: Execute the query and return the result
    try:
        result = execute_query(final_query)
        print(f"[DEBUG] Query execution result: {result}")
        return result
    except Exception as e:
        print(f"[ERROR] Error executing query: {str(e)}")
        return f"Error: Unable to execute query. Details: {str(e)}"
