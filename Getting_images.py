import requests
import os
import pandas as pd
# List of image URLs
df = pd.read_csv(r"C:\Users\shuva\OneDrive\Desktop\2023-24\ВКР\Movies\movies_list.csv")
image_urls = df.Poster_URL.values

# Folder to save the downloaded images
folder_path = r"C:\Users\shuva\OneDrive\Desktop\2023-24\ВКР\Movies\photos"  # Replace with your folder path

# Create the folder if it doesn't exist
if not os.path.exists(folder_path):
    os.makedirs(folder_path)

# Download and save images to the specified folder
for idx, url in enumerate(image_urls):
    try:
        response = requests.get(url)
        if response.status_code == 200:
            # Extract the file extension from the URL
            file_extension = os.path.splitext(url)[1]

            # Save the image to the folder with a unique name
            file_name = f"{df.Film_name.values[idx]}{file_extension}"
            file_path = os.path.join(folder_path, file_name)

            with open(file_path, 'wb') as file:
                file.write(response.content)
            print(f"Downloaded: {file_name}")
        else:
            print(f"Failed to download: {url}")
    except Exception as e:
        print(f"Error downloading {url}: {e}")
