from flask import Flask
from app.routes import main_routes  # Import the routes from routes.py

# Initialize Flask app with custom static and template folder paths
flask_app = Flask(__name__, static_folder='app/static',
                  template_folder='app/templates')

# Register routes (Blueprints)
flask_app.register_blueprint(main_routes)

# Main entry point
if __name__ == "__main__":
    flask_app.run(debug=True)
