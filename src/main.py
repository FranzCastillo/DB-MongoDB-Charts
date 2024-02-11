import csv
import codecs

from src.connections.atlas import AtlasConnection


def main():
    atlas = AtlasConnection()
    collections = {
        'careers': get_career_documents(),
        'countries': get_countries_documents(),
        'universities': get_universities_documents(),
        'students': get_students_documents(),
        # 'exchanges': None,
    }

    for collection_name, documents in collections.items():
        bulk_insert(atlas, collection_name, documents)

    # for collection_name in collections.keys():
    #     atlas.truncate_collection(collection_name)


def create_collections(connection: AtlasConnection):
    connection.create_collection('careers')
    connection.create_collection('countries')
    connection.create_collection('universities')
    connection.create_collection('students')
    connection.create_collection('exchanges')


def bulk_insert(connection: AtlasConnection, collection_name: str, documents_list: list):
    connection.insert_many(collection_name, documents_list)


def get_career_documents():
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


if __name__ == "__main__":
    main()
