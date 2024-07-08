import os
from azure.cosmos import CosmosClient
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Cosmos DB configuration
endpoint = os.getenv("COSMOS_DB_ENDPOINT")
key = os.getenv("COSMOS_DB_KEY")
database_name = os.getenv("COSMOS_DB_DATABASE_NAME")
container_name = os.getenv("COSMOS_DB_CONTAINER_NAME")

# Initialize the Cosmos client
client = CosmosClient(endpoint, key)
database = client.get_database_client(database_name)
container = database.get_container_client(container_name)


def delete_all_documents():
    query = "SELECT c.id, c.author_id FROM c"
    items = list(container.query_items(
        query=query,
        enable_cross_partition_query=True
    ))

    for item in items:
        container.delete_item(item=item['id'], partition_key=item['author_id'])
        print(f"Deleted document with id: {item['id']}")


if __name__ == "__main__":
    delete_all_documents()
