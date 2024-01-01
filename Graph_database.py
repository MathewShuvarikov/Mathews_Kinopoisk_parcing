# libraries
import pandas as pd
from kinopoisk_unofficial.kinopoisk_api_client import KinopoiskApiClient # connection with API
from kinopoisk_unofficial.request.films.film_request import FilmRequest # film name
from kinopoisk_unofficial.request.staff.staff_request import StaffRequest # People from film

# empty dataframe to keep all the records
df = pd.DataFrame(columns=['Film', 'Actor', 'Director', 'Screenwriter'])

api_client = KinopoiskApiClient("c6d5218b-d822-4f5f-850f-4b74b69d0499") # my tocken
film_request = FilmRequest(507) # create a query for the film name using film id
staff_request = StaffRequest(507) # create a query for the film staff using film id
film_response = api_client.films.send_film_request(film_request)
staff_response = api_client.staff.send_staff_request(staff_request)

staff_by_profession = {}
for staff in staff_response.items:
    profession = staff.profession_text
    if profession not in staff_by_profession:
        staff_by_profession[profession] = staff.name_ru

# Create a dictionary to hold data for the DataFrame
data_dict = {'Film': film_response.film.name_ru}

# Assign values to respective columns based on profession
for profession, name_ru in staff_by_profession.items():
    if profession == 'Актеры':
        data_dict['Actor'] = name_ru
    elif profession == 'Режиссеры':
        data_dict['Director'] = name_ru
    elif profession == 'Сценаристы':
        data_dict['Screenwriter'] = name_ru

# Convert dictionary to DataFrame
df = pd.DataFrame(data_dict, index=[0])
