FEATURE_FLAGS = {
    "ENABLE_TEMPLATE_PROCESSING": True,
}

SQLALCHEMY_DATABASE_URI = "sqlite:////app/superset_home/superset.db"
SECRET_KEY = "Q2g8sV6P2L8O0K7uW7s8b4v6kE9c4p3aYx9Zt1fJgKp5mTqVdA"

# Disable Celery - run queries synchronously
RESULTS_BACKEND = None
CLASS_PERMISSION_NAME = None

# Override the executor to use synchronous execution
SQLLAB_CTAS_NO_LIMIT = True
SQLLAB_TIMEOUT = 300

# This is the key setting - disables async query execution
CELERY_CONFIG = None
