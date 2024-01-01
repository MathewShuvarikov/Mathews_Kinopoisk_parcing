# libraries
import pandas as pd
from kinopoisk_unofficial.kinopoisk_api_client import KinopoiskApiClient # connection with API
from kinopoisk_unofficial.request.films.film_request import FilmRequest # film name
from kinopoisk_unofficial.request.staff.staff_request import StaffRequest # People from film

# empty dataframe to keep all the records
data = pd.DataFrame(columns=['Film', 'Actor', 'Director', 'Screenwriter'])

for i in range(500, 600):
    api_client = KinopoiskApiClient("c6d5218b-d822-4f5f-850f-4b74b69d0499") # my tocken
    film_request = FilmRequest(i) # create a query for the film name using film id
    staff_request = StaffRequest(i) # create a query for the film staff using film id
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
    data = pd.concat([data, df], ignore_index=True)
    print(i)

# data.to_csv(r"C:\Users\shuva\Downloads\films1.csv", index=0)

# Let's say df contains your data in the format you mentioned earlier
data = pd.read_csv(r"C:\Users\shuva\OneDrive\Desktop\films1.csv")

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


# Сохранение DataFrame в CSV файл
df.to_csv(r"C:\Users\shuva\OneDrive\Desktop\films2.csv", index=False, encoding='utf-8')
