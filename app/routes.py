# app/routes.py
from flask import Blueprint, render_template, request, jsonify, redirect, url_for
from app.db import connect_mysql, connect_mongo
from app.services.query_generator import generate_query
from app.services.nlp_processing import process_query
from bson import json_util
import pandas as pd
import json
from bson import ObjectId
from app.query_utils import extract_fields_from_input, extract_table_from_input

# Define the Blueprint for routes
main_routes = Blueprint('main_routes', __name__)


@main_routes.route('/')
def index():
    return render_template('index.html')  # Render the index page

# Step 1: Add a Route to Handle Dataset Uploads


@main_routes.route('/upload_dataset', methods=['GET', 'POST'])
def upload_dataset():
    if request.method == 'POST':
        if 'file' not in request.files:
            return jsonify({"error": "No file part in the request"}), 400

        file = request.files['file']
        if file.filename == '':
            return jsonify({"error": "No file selected"}), 400

        # Assuming the uploaded file is a CSV
        try:
            df = pd.read_csv(file)
            db_type = request.form.get('db_type')  # 'sql' or 'nosql'

            if db_type == 'sql':
                with connect_mysql() as conn:
                    cursor = conn.cursor()

                    # Insert into the correct table based on the file being uploaded
                    if 'store_location' in df.columns:
                        # Assuming it's the stores CSV
                        for _, row in df.iterrows():
                            cursor.execute(
                                "INSERT INTO stores (store_id, store_location) VALUES (%s, %s)",
                                (row['store_id'], row['store_location'])
                            )
                    elif 'user_name' in df.columns:
                        # Assuming it's the users CSV
                        for _, row in df.iterrows():
                            cursor.execute(
                                "INSERT INTO users (user_id, user_name, user_email, registration_date) VALUES (%s, %s, %s, %s)",
                                (row['user_id'], row['user_name'],
                                 row['user_email'], row['registration_date'])
                            )
                    elif 'transaction_qty' in df.columns:
                        # Assuming it's the transactions CSV
                        for _, row in df.iterrows():
                            cursor.execute(
                                "INSERT INTO transactions (transaction_id, product_category, transaction_qty, unit_price, transaction_date, store_id, user_id) VALUES (%s, %s, %s, %s, %s, %s, %s)",
                                (row['transaction_id'], row['product_category'], row['transaction_qty'],
                                 row['unit_price'], row['transaction_date'], row['store_id'], row['user_id'])
                            )
                    else:
                        return jsonify({"error": "CSV file does not match expected tables"}), 400

                    conn.commit()
                return jsonify({"success": "Data inserted successfully into SQL database"}), 200

            elif db_type == 'nosql':
                db = connect_mongo()
                collection_name = None

                # Decide the collection name based on the CSV columns
                if 'customer_name' in df.columns:
                    collection_name = 'customers'
                elif 'product_name' in df.columns:
                    collection_name = 'products'
                elif 'order_date' in df.columns:
                    collection_name = 'orders'
                else:
                    # Return an error if the columns do not match expected collections
                    return jsonify({"error": "CSV file does not match expected collections"}), 400

                # If a valid collection is determined, insert data
                collection = db[collection_name]
                # Convert the dataframe to dictionary format for MongoDB insertion
                data_dict = df.to_dict(orient='records')
                try:
                    collection.insert_many(data_dict)
                    print(
                        f"[INFO] Data inserted into collection: {collection_name}")
                except Exception as e:
                    print(
                        f"[ERROR] Error inserting data into MongoDB: {str(e)}")
                    return jsonify({"error": f"Error inserting data into MongoDB: {str(e)}"}), 500

                return jsonify({"success": f"Data inserted successfully into {collection_name} collection"}), 200

        except Exception as e:
            print(f"[ERROR] Error processing file: {str(e)}")
            return jsonify({"error": f"Error processing file: {str(e)}"}), 500

    return render_template('upload.html')


@main_routes.route('/submit_query', methods=['POST'])
def submit_query():
    user_query = request.form.get('query')
    db_type = request.form.get('db_type')  # 'sql' or 'nosql'

    # Step 1: Process the user query using NLP to identify the pattern
    try:
        print(f"[DEBUG] User Query Received: {user_query}, DB Type: {db_type}")

        query_response = process_query(user_query, db_type)
        query_pattern = query_response.get('matched_query')

        if not query_pattern:
            raise ValueError("No valid query pattern found.")

        print(f"[DEBUG] Processed query pattern: {query_pattern}")

        # Extract the table name dynamically from the user query
        extracted_table = extract_table_from_input(user_query)

        # Ensure collection/table name is present for SQL and NoSQL
        if not extracted_table:
            extracted_table = 'messages'  # Default collection/table

        # Update query pattern with the extracted table
        query_pattern['table'] = extracted_table
        print(f"[DEBUG] Extracted table: {extracted_table}")

        # Extract fields if any are present in the user query
        extracted_fields = extract_fields_from_input(user_query)
        if extracted_fields:
            query_pattern['fields'] = extracted_fields
            print(f"[DEBUG] Extracted fields: {extracted_fields}")

        # Extract tokens for potential filter conditions
        tokens = user_query.lower().split()
        filter_criteria = {}

        if 'greater' in tokens:
            try:
                field_index = tokens.index('greater') - 1
                value_index = tokens.index('greater') + 1
                filter_field = tokens[field_index]
                filter_value = tokens[value_index]

                if filter_field and filter_value.isdigit():
                    filter_criteria[filter_field] = {"$gt": int(filter_value)}

            except (ValueError, IndexError):
                pass

        if filter_criteria:
            query_pattern['filter'] = filter_criteria
            print(f"[DEBUG] Extracted filter: {filter_criteria}")

    except Exception as e:
        print(f"[ERROR] Error processing query: {str(e)}")
        return jsonify({"error": f"Error processing query: {str(e)}"}), 500

    # Step 2: Generate the appropriate SQL/NoSQL query
    try:
        final_query = generate_query(query_pattern, db_type)
        print(f"[DEBUG] Generated query: {final_query}")
    except Exception as e:
        print(f"[ERROR] Error generating query: {str(e)}")
        return jsonify({"error": f"Error generating query: {str(e)}"}), 500

    results = None

    # Step 3: Execute the query
    try:
        print(f"[DEBUG] Starting Query Execution for DB Type: {db_type}")

        if db_type == 'sql':
            with connect_mysql() as conn:
                cursor = conn.cursor()
                if isinstance(final_query, tuple):
                    cursor.execute(final_query[0], final_query[1])
                else:
                    cursor.execute(final_query)
                results = cursor.fetchall()

                # Insert user query into `user_queries` table for record-keeping
                cursor.execute(
                    "INSERT INTO user_queries (query_text, db_type) VALUES (%s, %s)", (user_query, db_type))
                conn.commit()
            print(
                f"[DEBUG] SQL Query executed successfully. Results: {results}")

        elif db_type == 'nosql':
            db = connect_mongo()
            if isinstance(final_query, dict):
                collection = db[final_query['collection']]
                print(f"[DEBUG] NoSQL Collection: {collection.name}")

                if final_query['operation'] == 'aggregate':
                    results = list(collection.aggregate(
                        final_query['pipeline']))
                elif final_query['operation'] == 'find':
                    results = list(collection.find(
                        final_query.get('filter', {})))
                    for doc in results:
                        if '_id' in doc and isinstance(doc['_id'], ObjectId):
                            print(
                                f"[DEBUG] Before conversion, ObjectId: {doc['_id']}")
                            doc['_id'] = str(doc['_id'])
                            print(
                                f"[DEBUG] Converted ObjectId to string: {doc['_id']}")
                elif final_query['operation'] == 'count':
                    results = [{"count": collection.count_documents(
                        final_query.get('filter', {}))}]
                else:
                    raise ValueError(
                        f"Unsupported NoSQL operation: {final_query['operation']}")

            else:
                raise ValueError("Invalid query format for NoSQL")

            results = json.loads(json_util.dumps(results))
            print(
                f"[DEBUG] NoSQL Query executed successfully. Results: {results}")

    except Exception as e:
        print(f"[ERROR] Query Execution Error: {str(e)}")
        return jsonify({"error": f"Query Execution Error: {str(e)}"}), 500

    # Step 4: Return the result to the user
    response = {
        "generated_query": final_query,
        "result": results if results else []
    }
    print(f"[DEBUG] Final Response: {response}")
    return jsonify(response)
