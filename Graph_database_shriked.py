# Библиотеки
# Версия Pandas: 2.1.4
# Версия neo4j: 5.16.0
# Версия Python: 3.9.0 (tags/v3.9.0:9cf6752, Oct  5 2020, 15:34:40) [MSC v.1927 64 bit (AMD64)]
import pandas as pd
from kinopoisk_unofficial.kinopoisk_api_client import KinopoiskApiClient # connection with API
from kinopoisk_unofficial.request.films.film_request import FilmRequest # film name
from kinopoisk_unofficial.request.staff.staff_request import StaffRequest # People from film
from kinopoisk_unofficial.client.exception.not_found import NotFound

# датафрейм для хранения результатов запроса
# будем использовать только наименование фильма, режиссера, первого актера и первого сценариста из списка
data = pd.DataFrame(columns=['Film', 'Actor', 'Director', 'Screenwriter'])

for i in range(1195, 1200):
    try:
    ## 62b2be73-aae3-4751-a556-742d02d31d96 2nd api
    ## c6d5218b-d822-4f5f-850f-4b74b69d0499 1st api
        api_client = KinopoiskApiClient("62b2be73-aae3-4751-a556-742d02d31d96") # мой токен
        film_request = FilmRequest(i) # запрос на название фильма с использованием film id
        staff_request = StaffRequest(i) # запрос на персон фильма с использованием film id
        film_response = api_client.films.send_film_request(film_request)
        staff_response = api_client.staff.send_staff_request(staff_request)

        staff_by_profession = {}
        for staff in staff_response.items:
            profession = staff.profession_text
            if profession not in staff_by_profession:
                staff_by_profession[profession] = staff.name_ru
        print(f"Film with ID {i} found")
    except NotFound:
        print(f"Film with ID {i} not found. Skipping...")

    # Создаем словарь куда будем складывать информацию по фильмам
    data_dict = {'Film': film_response.film.name_ru}

    # Достаем только Актера, Режиссера и Сценариста
    for profession, name_ru in staff_by_profession.items():
        if profession == 'Актеры':
            data_dict['Actor'] = name_ru
        elif profession == 'Режиссеры':
            data_dict['Director'] = name_ru
        elif profession == 'Сценаристы':
            data_dict['Screenwriter'] = name_ru

    # Складываем все в датафрейм
    data = pd.concat([data, pd.DataFrame(data_dict, index=[0])], ignore_index=True)

df1 = pd.read_csv(r"C:\Users\shuva\OneDrive\Desktop\films_info.csv")
df1 = pd.concat([df1,data])
df1 = df1.dropna()
df1 = df1.drop_duplicates()
df1.to_csv(r"C:\Users\shuva\OneDrive\Desktop\films_info.csv", index=0)


data = pd.read_csv(r"C:\Users\shuva\OneDrive\Desktop\films_info.csv")
data = data.drop_duplicates()
data = data.dropna()
relationships = []

# Iterate through each row in the DataFrame
for _, row in data.iterrows():
    film = row['Film']
    actor = row['Actor']
    director = row['Director']
    screenwriter = row['Screenwriter']

    # Create relationships
    relationships.append((film, actor, 'ACTED_IN'))
    relationships.append((film, director, 'DIRECTED'))
    relationships.append((film, screenwriter, 'WRITTEN_BY'))

# Display relationships
for relationship in relationships:
    print(relationship)


from neo4j import GraphDatabase
# Connect to the Neo4j database
# 9bf936d8.databases.neo4j.io:7687
uri = "neo4j+s://9bf936d8.databases.neo4j.io:7687"  # server URI
user = "neo4j"  # Neo4j username
password = "wIeMFbVDn0n4lJRnABhWIv9NmUa7sE4WuJGXPk9pAHk"  # Neo4j password

# Create a Neo4j driver instance
driver = GraphDatabase.driver(uri, auth=(user, password))

def create_relationships(tx, film, person, relationship):
    # Cypher query to create relationships between films and people
    query = (
        "MERGE (f:Film {title: $film}) "
        "MERGE (p:Person {name: $person}) "
        "MERGE (p)-[:%s]->(f)" % relationship
    )
    tx.run(query, film=film, person=person)

# Iterate through relationships and import them to Neo4j
with driver.session() as session:
    for relationship in relationships:
        film, person, rel_type = relationship
        session.write_transaction(create_relationships, film, person, rel_type)

session.close()
driver.close()