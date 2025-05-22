# The __init__ file for the querygen package.
# The current implementation is based on Google Cloud Vertex AI and assumes the
# following environment variables are set:
# GOOGLE_CLOUD_PROJECT=the name of the GCP project with access to Vertex AI
# GOOGLE_CLOUD_LOCATION=the project region, e.g., us-central1
# GOOGLE_GENAI_USE_VERTEXAI=True

# These are used for debugging purposes and to understand how the final SQL
# is generated. They are in the package namespace and apply to all instances
# of classes and functions in this package.
PRINT_DB_DESC = False
PRINT_CLOSE_CONCEPTS = False
PRINT_SAMPLE_VALUES = False
PRINT_FINAL_PROMPT = False
PRINT_MODEL_RESPONSE = False
