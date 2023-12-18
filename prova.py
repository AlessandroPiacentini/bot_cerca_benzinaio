import csv
import requests

url_csv_coordinate = "https://www.mimit.gov.it/images/exportCSV/anagrafica_impianti_attivi.csv"

def get_coordinates_by_id(id_list):
    coordinates = []

    response = requests.get(url_csv_coordinate)
    response.raise_for_status()

    # Leggi il file CSV direttamente dal contenuto della risposta HTTP
    csv_reader = csv.DictReader(response.text.splitlines(), delimiter=';')

    # Print the header row to check column names
    print(csv_reader.fieldnames)

    for row in csv_reader:
        if 'idImpianto' in row and int(row['idImpianto']) in id_list:
            # Append (Longitude, Latitude) as a tuple
            coordinates.append((float(row['Longitudine']), float(row['Latitudine'])))

    return coordinates

# Example Usage
id_list = [52829
, 55389, 56865, 57421, 56802]
result_coordinates = get_coordinates_by_id(id_list)

print(result_coordinates)
