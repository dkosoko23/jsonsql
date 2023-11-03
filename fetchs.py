import requests
import json

# URL of the JSON data
url = "https://plondogroup.com/tranzact/uploads/q6/36_catalog.json"

# Send an HTTP GET request to the URL
with requests.get(url, stream=True) as response:
    if response.status_code == 200:
        # Parse the JSON data from the response
        json_data = response.json()

        # Specify the file name to save the JSON data
        file_name = "product_catalog.json"

        # Save the JSON data to a file
        with open(file_name, "w") as file:
            json.dump(json_data, file, indent=4)

        print(f"JSON data saved to {file_name}")
    else:
        print(f"Failed to retrieve JSON data. Status code: {response.status_code}")
