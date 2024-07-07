from flask import Flask, render_template
from azure.cosmos import CosmosClient
import os

app = Flask(__name__)

# Load environment variables
endpoint = os.getenv("COSMOS_DB_ENDPOINT")
key = os.getenv("COSMOS_DB_KEY")
database_name = os.getenv("COSMOS_DB_DATABASE_NAME")
container_name = os.getenv("COSMOS_DB_CONTAINER_NAME")

# Initialize the Cosmos client
client = CosmosClient(endpoint, key)
database = client.get_database_client(database_name)
container = database.get_container_client(container_name)


@app.route('/')
def index():
    query = "SELECT * FROM c"
    items = list(container.query_items(
        query=query,
        enable_cross_partition_query=True
    ))

    return render_template('index.html', items=items)


if __name__ == "__main__":
    app.run(debug=True)
