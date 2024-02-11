from dotenv import dotenv_values
from pymongo.mongo_client import MongoClient

secrets = dotenv_values(".env")


class AtlasConnection:
    def __init__(self):
        # Load the secrets from the .env file on the root folder
        username = secrets['USERNAME']
        password = secrets['PASSWORD']
        uri_template = secrets['URI']
        uri = uri_template.replace('<username>', username).replace('<password>', password)

        self.client = MongoClient(uri)
        self.db = self.client.Intercambios

    def ping(self):
        """
        Pings the MongoDB deployment to check if the connection is successful
        :return:  None
        """
        try:
            self.client.admin.command('ping')
            print("Pinged your deployment. You successfully connected to MongoDB!")
        except Exception as e:
            print(e)

    def create_collection(self, collection_name='default'):
        """
        Creates a collection in the database
        :param collection_name:  Name of the collection
        :return:  None
        """
        try:
            self.db.create_collection(collection_name)
            print(f"Collection '{collection_name}' created successfully!")
        except Exception as e:
            print(e)

    def insert_many(self, collection_name, documents):
        """
        Inserts a list of documents into a collection
        :param collection_name:  Name of the collection
        :param documents:  List of documents to insert
        :return:  None
        """
        try:
            self.db[collection_name].insert_many(documents, ordered=False)
            print(f"Documents inserted into '{collection_name}' successfully!")
        except Exception as e:
            print(e)

    def truncate_collection(self, collection_name):
        """
        Truncates a collection
        :param collection_name:  Name of the collection
        :return:  None
        """
        try:
            self.db[collection_name].drop()
            self.db.create_collection(collection_name)
            print(f"Collection '{collection_name}' truncated successfully!")
        except Exception as e:
            print(e)
