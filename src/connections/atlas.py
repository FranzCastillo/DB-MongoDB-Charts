from dotenv import dotenv_values
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi

secrets = dotenv_values(".env")


class AtlasConnection:
    def __init__(self):
        username = secrets['USERNAME']
        password = secrets['PASSWORD']
        uri_template = secrets['URI']
        uri = uri_template.replace('<username>', username).replace('<password>', password)

        self.client = MongoClient(uri, server_api=ServerApi('1'))
        # Send a ping to confirm a successful connection
        try:
            self.client.admin.command('ping')
            print("Pinged your deployment. You successfully connected to MongoDB!")
        except Exception as e:
            print(e)