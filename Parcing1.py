import pandas as pd
from kinopoisk_unofficial.kinopoisk_api_client import KinopoiskApiClient
from kinopoisk_unofficial.request.films.film_request import FilmRequest
from kinopoisk_unofficial.client.exception.not_found import NotFound

# Create an empty DataFrame
df = pd.DataFrame(columns=['Film_name', 'Genre', 'Poster_URL', 'Description'])

## я успел вытащить с 300 по 1224
for i in range(1226, 1400):
    try:
        api_client = KinopoiskApiClient("c6d5218b-d822-4f5f-850f-4b74b69d0499")
        request = FilmRequest(i)
        response = api_client.films.send_film_request(request)
        print(response.film.name_ru, i)

        # Append data to DataFrame
        df = pd.concat([df, pd.DataFrame({'Film_name': response.film.name_ru,
                                          'Genre': [', '.join([genre.genre for genre in response.film.genres])],
                                          'Poster_URL': response.film.poster_url,
                                          'Description': response.film.description}, index=[0])], ignore_index=True)

    except NotFound:
        print(f"Film with ID {i} not found. Skipping...")

data = pd.read_csv(r"C:\Users\shuva\OneDrive\Desktop\2023-24\ВКР\Movies\movies_list.csv")
data = pd.concat([data, df])
data.to_csv(r"C:\Users\shuva\OneDrive\Desktop\2023-24\ВКР\Movies\movies_list.csv", index=0)


from kinopoisk_unofficial.request.staff.staff_request import StaffRequest
api_client = KinopoiskApiClient("c6d5218b-d822-4f5f-850f-4b74b69d0499")
request = StaffRequest(507)
response = api_client.staff.send_staff_request(request)


from kinopoisk_unofficial.kinopoisk_api_client import KinopoiskApiClient
from kinopoisk_unofficial.request.persons.person_by_name_request import PersonByNameRequest
api_client = KinopoiskApiClient("c6d5218b-d822-4f5f-850f-4b74b69d0499")
request = PersonByNameRequest("Джеймс Кэмерон")
response = api_client.persons.send_person_by_name_request(request)


from kinopoisk_unofficial.kinopoisk_api_client import KinopoiskApiClient
from kinopoisk_unofficial.request.staff.person_request import PersonRequest
api_client = KinopoiskApiClient("c6d5218b-d822-4f5f-850f-4b74b69d0499")
request = PersonRequest(27977)
response = api_client.staff.send_person_request(request)