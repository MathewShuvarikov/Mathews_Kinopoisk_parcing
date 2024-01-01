import pandas as pd
from kinopoisk_unofficial.kinopoisk_api_client import KinopoiskApiClient
from kinopoisk_unofficial.request.films.film_request import FilmRequest

# api_client = KinopoiskApiClient("c6d5218b-d822-4f5f-850f-4b74b69d0499")
# request = FilmRequest(507)
# response = api_client.films.send_film_request(request)
# response.film.description
# genres_list = [genre.genre for genre in response.film.genres]
# print(genres_list)

df = pd.DataFrame(columns=['Film_name', 'Genre', 'Poster_URL', 'Description'], index=[0])
for i in range(300, 700):
    api_client = KinopoiskApiClient("c6d5218b-d822-4f5f-850f-4b74b69d0499")
    request = FilmRequest(i)
    response = api_client.films.send_film_request(request)
    print(response.film.name_ru, i)
    df = pd.concat([df, pd.DataFrame({'Film_name': response.film.name_ru,
                                      'Genre': [', '.join([genre.genre for genre in response.film.genres])],
                                      'Poster_URL': response.film.poster_url,
                                      'Description': response.film.description}, index=[0])], ignore_index=True)