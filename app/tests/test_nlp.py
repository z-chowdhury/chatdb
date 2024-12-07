# test_nlp.py
#from app.services.nlp_processing import preprocess_input, match_query_pattern
from ..services.nlp_processing import preprocess_input, match_query_pattern

import sys
print(sys.path)
import os

# Add the project root to the system path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))


test_cases = {
    "Can you find all messages?": "SELECT * FROM messages;",
    "Show me the average price.": "SELECT AVG(some_field) FROM messages;",
    "Count the total messages.": "SELECT COUNT(*) FROM messages;",
    "Insert a new message.": {"action": "insert", "collection": "messages", "data": {"content": "your_message"}},
}

for query, expected in test_cases.items():
    preprocessed = preprocess_input(query)
    matched_query = match_query_pattern(preprocessed)
    print(
        f"Query: {query}\nPreprocessed: {preprocessed}\nMatched Query: {matched_query}\n")

    assert matched_query == expected, f"Test failed for query: {query}"

print("All tests passed!")
