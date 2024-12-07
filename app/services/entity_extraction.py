import spacy
from app.query_utils import NOSQL_FIELD_SYNONYMS, FIELD_SYNONYMS

# Initialize spaCy language model
nlp = spacy.load("en_core_web_sm")


def extract_entities(user_input, db_type='nosql'):
    # Extract entities from the user's input using spaCy
    doc = nlp(user_input)
    extracted_entities = [(ent.text, ent.label_) for ent in doc.ents]

    # Custom rule-based extraction for database fields
    input_lower = user_input.lower()
    field_synonyms = NOSQL_FIELD_SYNONYMS if db_type == 'nosql' else FIELD_SYNONYMS

    # Extract database-specific fields using synonyms
    for synonym, field_name in field_synonyms.items():
        if synonym in input_lower:
            extracted_entities.append((field_name, 'FIELD'))

    # Remove duplicates if necessary
    unique_entities = list(set(extracted_entities))

    return unique_entities


# Example Usage
user_input = "What is the total sales in the last month?"
entities = extract_entities(user_input)
print(entities)
