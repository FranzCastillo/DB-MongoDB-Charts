import csv
import codecs
import random

from faker import Faker
from src.connections.atlas import AtlasConnection


def main():
    atlas = AtlasConnection()

    # Uncomment to create collections
    # create_collections(atlas)

    collections = {
        'careers': get_career_documents(),
        'countries': get_countries_documents(),
        'universities': get_universities_documents(),
        'students': get_students_documents(),
        'exchanges': get_exchanges_documents(),
    }

    for collection_name, documents in collections.items():
        bulk_insert(atlas, collection_name, documents)

    # Uncomment to Truncate collections
    # for collection_name in collections.keys():
    #     atlas.truncate_collection(collection_name)


def create_collections(connection: AtlasConnection):
    """
    Creates the needed collections in the database
    :param connection:  AtlasConnection object
    :return:
    """
    connection.create_collection('careers')
    connection.create_collection('countries')
    connection.create_collection('universities')
    connection.create_collection('students')
    connection.create_collection('exchanges')


def bulk_insert(connection: AtlasConnection, collection_name: str, documents_list: list):
    """
    Inserts a list of documents into a collection
    :param connection:  AtlasConnection object
    :param collection_name:  Name of the collection
    :param documents_list:  List of documents to insert
    :return:  None
    """
    connection.insert_many(collection_name, documents_list)


def get_career_documents():
    """
    Reads the careers.csv file and returns a list of documents
    :return:  List of documents
    """
    documents = []
    with codecs.open('src/data/careers.csv', 'r', encoding='utf-8', errors='ignore') as file:
        careers = csv.DictReader(file)
        for row in careers:
            career = {
                '_id': row["_id"],
                'name': row["name"],
                'faculty': {
                    '_id': row["faculty_id"],
                    'name': row["faculty_name"],
                    'short_name': row["faculty_short_name"],
                }
            }
            documents.append(career)
    return documents


def get_countries_documents():
    """
    Reads the countries.csv file and returns a list of documents
    :return:  List of documents
    """
    documents = []
    with codecs.open('src/data/countries.csv', 'r', encoding='utf-8', errors='ignore') as file:
        countries = csv.DictReader(file)
        for row in countries:
            country = {
                '_id': row["_id"],
                'name': row["countryName"],
                'iso': row["countryIso3"],
                'continent': {
                    '_id': row["continent_id"],
                    'name': row["continentName"],
                }
            }
            documents.append(country)
    return documents


def get_universities_documents():
    """
    Reads the universities.csv file and returns a list of documents
    :return:  List of documents
    """
    documents = []
    with codecs.open('src/data/universities.csv', 'r', encoding='utf-8', errors='ignore') as file:
        universities = csv.DictReader(file)
        for row in universities:
            university = {
                '_id': row["_id"],
                'country_id': row["country_id"],
                'name': row["name"],
                'acronym': row["short_name"],
            }
            documents.append(university)
    return documents


def get_students_documents():
    """
    Reads the students.csv file and returns a list of documents
    :return:  List of documents
    """
    documents = []
    with codecs.open('src/data/students.csv', 'r', encoding='utf-8', errors='ignore') as file:
        students = csv.DictReader(file)
        for row in students:
            student = {
                '_id': row["_id"],
                'name': row["name"],
                'email': row["email"],
                'career_id': row["career_id"],
            }
            documents.append(student)
    return documents


def get_exchanges_documents():
    """
    Reads the exchanges.csv file and returns a list of documents
    :return:  List of documents
    """
    documents = []
    with codecs.open('src/data/exchanges.csv', 'r', encoding='utf-8', errors='ignore') as file:
        exchanges = csv.DictReader(file)
        for row in exchanges:
            exchange = {
                "_id": row["_id"],
                "student_id": row["student_id"],
                "university_id": row["university_id"],
                "details": {
                    "year": row["year"],
                    "semester": row["semester"],
                    "modality": row["modality"],
                    "status": row["status"],
                    "start_date": row["start_date"],
                    "end_date": row["end_date"],
                    "comments": row["comments"],
                }
            }
            documents.append(exchange)
    return documents


# All the functions below are used to mock exchange documents

def mock_exchanges(num_documents=1000):
    """
    Mocks exchange documents
    :param num_documents:  Number of documents to mock
    :return:  List of documents
    """
    fake = Faker()
    documents = []
    student_ids = get_student_ids()
    university_ids = get_university_ids()
    modalities = ["ELAP", "Beca", "Convenio", "Visitante"]
    statuses = ["En curso", "Finalizada", "Cancelada"]
    start_date = fake.date_between(start_date='-10y', end_date='today')
    end_date = fake.date_between(start_date=start_date, end_date='+1y')

    for i in range(num_documents):
        document = {
            "_id": i,
            "student_id": random.choice(student_ids),
            "university_id": random.choice(university_ids),
            "details": {
                "year": random.randint(2000, 2024),
                "semester": random.randint(1, 2),
                "modality": random.choice(modalities),
                "status": random.choice(statuses),
                "start_date": start_date.strftime("%Y-%m-%d"),
                "end_date": end_date.strftime("%Y-%m-%d"),
                "comments": fake.text(max_nb_chars=200),
            }
        }
        documents.append(document)

    return documents


def get_student_ids():
    """
    Reads the students.csv file and returns a list of student ids
    :return:  List of student ids
    """
    with codecs.open('src/data/students.csv', 'r', encoding='utf-8', errors='ignore') as file:
        students = csv.DictReader(file)
        return [int(row["_id"]) for row in students]


def get_university_ids():
    """
    Reads the universities.csv file and returns a list of university ids
    :return:  List of university ids
    """
    with codecs.open('src/data/universities.csv', 'r', encoding='utf-8', errors='ignore') as file:
        universities = csv.DictReader(file)
        return [int(row["_id"]) for row in universities]


def parse_exchanges(documents):
    """
    Writes the exchange documents to a csv file
    :param documents:  List of exchange documents (from mock_exchanges)
    :return:  None
    """
    with open('src/data/exchanges.csv', 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(["_id", "student_id", "university_id", "year", "semester", "modality", "status", "start_date",
                         "end_date", "comments"])
        for document in documents:
            writer.writerow([document["_id"], document["student_id"], document["university_id"],
                             document["details"]["year"], document["details"]["semester"],
                             document["details"]["modality"],
                             document["details"]["status"], document["details"]["start_date"],
                             document["details"]["end_date"],
                             document["details"]["comments"]])


if __name__ == "__main__":
    main()
