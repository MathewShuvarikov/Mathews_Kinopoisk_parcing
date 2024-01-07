from kinopoisk_unofficial.kinopoisk_api_client import KinopoiskApiClient
from kinopoisk_unofficial.request.films.film_request import FilmRequest
from kinopoisk_unofficial.request.staff.staff_request import StaffRequest
from neo4j import GraphDatabase

# Создаем клиента для API Кинопоиска
api_client = KinopoiskApiClient("62b2be73-aae3-4751-a556-742d02d31d96")

# Создаем подключение к базе данных Neo4j
uri = "neo4j+s://9bf936d8.databases.neo4j.io:7687"
user = "neo4j"
password = "wIeMFbVDn0n4lJRnABhWIv9NmUa7sE4WuJGXPk9pAHk"
driver = GraphDatabase.driver(uri, auth=(user, password))

def get_film_staff_relationships(film_id):
    try:
        # Отправляем запрос для информации о фильме и его персонале
        film_request = FilmRequest(film_id)
        staff_request = StaffRequest(film_id)
        film_response = api_client.films.send_film_request(film_request)
        staff_response = api_client.staff.send_staff_request(staff_request)

        # Собираем данные о персонале фильма
        staff_by_profession = {}
        for staff in staff_response.items:
            profession = staff.profession_text
            if profession not in staff_by_profession:
                staff_by_profession[profession] = staff.name_ru

        # Создаем связи между фильмом и персоналом
        relationships = []
        for profession, name_ru in staff_by_profession.items():
            if profession == 'Актеры':
                relationships.append((film_response.film.name_ru, name_ru, 'ACTED_IN'))
            elif profession == 'Режиссеры':
                relationships.append((film_response.film.name_ru, name_ru, 'DIRECTED'))
            elif profession == 'Сценаристы':
                relationships.append((film_response.film.name_ru, name_ru, 'WRITTEN_BY'))

        return relationships
    except Exception as e:
        print(f"Error processing film with ID {film_id}: {e}")
        return []

# Функция для создания связей в базе данных Neo4j
# Функция для создания связей в базе данных Neo4j
def create_relationships(tx, film, person, relationship):
    # Проверяем, что название фильма не пустое или равно 'null'
    if film and film != 'null':
        query = (
            "MERGE (f:Film {title: $film}) "
            "MERGE (p:Person {name: $person}) "
            "MERGE (p)-[:%s]->(f)" % relationship
        )
        tx.run(query, film=film, person=person)


# Итерируемся по ID фильмов и создаем связи в Neo4j
with driver.session() as session:
    for film_id in range(1190, 1200):
        relationships = get_film_staff_relationships(film_id)
        for relationship in relationships:
            film, person, rel_type = relationship
            session.write_transaction(create_relationships, film, person, rel_type)

driver.close()
