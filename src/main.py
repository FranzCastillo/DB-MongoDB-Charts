from src.connections.atlas import AtlasConnection
import pandas as pd


def main():
    atlas = AtlasConnection()
    create_collections(atlas)
    # prepare_documents()
    # bulk_insert()


def create_collections(connection: AtlasConnection):
    # Create collections
    connection.create_collection('faculties')
    connection.create_collection('careers')
    connection.create_collection('continents')
    connection.create_collection('countries')
    connection.create_collection('universities')
    connection.create_collection('students')
    connection.create_collection('exchanges')


if __name__ == "__main__":
    main()
