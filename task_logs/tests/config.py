import os

ELASTICSEARCH_URL = os.getenv("ELASTICSEARCH_URL", "http://localhost:9200")
CI = os.getenv("CI", "0") == "1"
