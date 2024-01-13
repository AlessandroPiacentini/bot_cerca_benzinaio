import requests
from time import sleep
from datetime import datetime
from connector_db import DatabaseSingleton as db
import json
from io import StringIO
import csv
import pandas as pd
from geopy.geocoders import Nominatim
from telegram_library import TelegramLibrary as TL




BOT_TOKEN = "6928800593:AAGWTWod4qh-t3QN-KX5EhD7SmTpV8oSwaI"
database = db()
msgXinfo="use these commands to find the right petrol station for you: \n/setStartPosition to set the starting position \n/setHowMuchFuel to set how much fuel you have\n/getGasStation to receive directions to gas stations"
database.connect("localhost", "root", "", "bot_benzinaio")
startMsg="Welcome to the bot, use these commands to set your info: \n/setTipoCarburante to set the fuel type\n/setCapacita to set the capacity of your tank\n/setMaxKm to set the max km you can do with a full tank\n/setName to set your name\n/getInfo to get your info"

api_key_openroute = '5b3ce3597851110001cf624862e1e632e8f14dbd8d0cc56c25ef35dc'

url_csv_benzinaii="https://www.mimit.gov.it/images/exportCSV/prezzo_alle_8.csv"
url_csv_coordinate="https://www.mimit.gov.it/images/exportCSV/anagrafica_impianti_attivi.csv"
bitly_access_token='1750be64b415c7100884d2ec50d897bfeeb0afb9'

RW_telegram= TL(BOT_TOKEN)




def get_comune_from_coordinates(latitudine, longitudine):
    geolocator = Nominatim(user_agent="my_geocoder")
    location = geolocator.reverse((latitudine, longitudine), language="it")

    # Estrai il comune dalla risposta
    comune = location.raw.get('address', {}).get('city')
    if comune is None:
        comune = location.raw.get('address', {}).get('town')

    return comune


def get_id_by_comune(id_list, _comune):
    id_list_comune = []

    response = requests.get(url_csv_coordinate)
    response.raise_for_status()

    # Leggi il file CSV direttamente dal contenuto della risposta HTTP
    csv_reader = response.text.splitlines('\n')
    
    # Skip the header row
    header = csv_reader[0].split(';')
    for row in csv_reader[2:]:
        # Use the correct separator (comma) and remove leading/trailing whitespaces
        row_array = row.replace('\n', '').split(';')
        row_array = [item.strip() for item in row_array]
        
        # print(row_array[5])
        if (int(row_array[0]) in id_list) and (row_array[6].lower().strip() == _comune.lower().strip()):
            # Append (Longitude, Latitude) as a tuple
            try:
                id_list_comune.append(int(row_array[0]))
            except ValueError:
                print(f"Errore durante la conversione in int: {row_array[0]}")
                
                pass  # Continue processing the next row
        # sleep(0.01)  
    return id_list_comune

def get_coordinates_by_id(id_list, _comune):
    coordinates = []

    response = requests.get(url_csv_coordinate)
    response.raise_for_status()

    # Leggi il file CSV direttamente dal contenuto della risposta HTTP
    csv_reader = response.text.splitlines('\n')
    
    # Skip the header row
    header = csv_reader[0].split(';')
    for row in csv_reader[2:]:
        # Use the correct separator (comma) and remove leading/trailing whitespaces
        row_array = row.replace('\n', '').split(';')
        row_array = [item.strip() for item in row_array]
        
        # print(row_array[5])
        if (int(row_array[0]) in id_list) and (row_array[6].lower().strip() == _comune.lower().strip()):
            # Append (Longitude, Latitude) as a tuple
            try:
                # print(row_array[8], row_array[9])
                coordinates.append([float(row_array[-1]), float(row_array[-2])])
            except ValueError:
                print(f"Errore durante la conversione di coordinate in float: {row_array[-1]}, {row_array[-2]}")
                
                pass  # Continue processing the next row
        # sleep(0.01)  
    return coordinates


def get_coordinate_by_id(id):
    coordinates =None

    response = requests.get(url_csv_coordinate)
    response.raise_for_status()

    # Leggi il file CSV direttamente dal contenuto della risposta HTTP
    csv_reader = response.text.splitlines('\n')
    
    # Skip the header row
    header = csv_reader[0].split(';')
    for row in csv_reader[2:]:
        # Use the correct separator (comma) and remove leading/trailing whitespaces
        row_array = row.replace('\n', '').split(';')
        row_array = [item.strip() for item in row_array]
        
        # print(row_array[5])
        if (int(row_array[0]) == id):
            # Append (Longitude, Latitude) as a tuple
            try:
                # print(row_array[8], row_array[9])
                coordinates=[float(row_array[-1]), float(row_array[-2])]
            except ValueError:
                print(f"Errore durante la conversione di coordinate in float: {row_array[-1]}, {row_array[-2]}")
    return coordinates

def all_are_set(chat_id):
    global database
    check_query = "SELECT * FROM users WHERE chat_id = %s"
    result = database.execute_query(check_query, (chat_id,))
    if result:
        if result[0][1] and result[0][2] and result[0][3] and result[0][4] and result[0][5] and result[0][6]:
            return True
        else:
            return False
    else:
        return False

def shortest_route_coordinates( start_coords, end_coords_list):
    global api_key_openroute
    try:
        route_lengths = []
        i=0
        max_i=len(end_coords_list)
        for end_coords in end_coords_list:
            # Slightly adjusted coordinates
            headers = {'Accept': 'application/json, application/geo+json, application/gpx+xml, img/png; charset=utf-8',}
            response_openroute = requests.get(f'https://api.openrouteservice.org/v2/directions/driving-car?api_key={api_key_openroute}&start={start_coords[0]},{start_coords[1]}&end={end_coords[0]},{end_coords[1]}', headers=headers)
            if response_openroute.status_code == 200:
                directions_data = response_openroute.json()
                route_length = directions_data['features'][0]['properties']['segments'][0]['distance']
                route_lengths.append((end_coords, route_length))
                percentuale=(i/max_i)*100
                i+=1
                # sleep(3)
            else:
                print(f"Errore nella richiesta di direzione da OpenRouteService: {response_openroute.status_code}")
                print(response_openroute.text)  # Print the response text for debugging

        shortest_route = min(route_lengths, key=lambda x: x[1])
        return shortest_route[0]

    except requests.exceptions.RequestException as req_exc:
        print(f"Errore durante la richiesta HTTP: {req_exc}")
        return None


def find_station_ids_by_fuel_type(desc_carburante):
    try:
        # Esegui la richiesta HTTP per ottenere il contenuto del file CSV
        response = requests.get(url_csv_benzinaii)
        response.raise_for_status()

        # Leggi il file CSV direttamente dal contenuto della risposta HTTP
        csv_reader = response.text.splitlines(';')

        # Inizializza una lista per tracciare gli idImpianto
        station_ids = []

        # Itera attraverso le righe del CSV
        for row in csv_reader[2:]:
            row_array = row.split(';')
            if row_array[1].lower() == desc_carburante:
                # Aggiungi l'idImpianto alla lista
                station_ids.append(int(row_array[0]))

        # Restituisci la lista di idImpianto associati al descCarburante
        return station_ids

    except requests.exceptions.RequestException as req_exc:
        print(f"Errore durante la richiesta HTTP: {req_exc}")
        return None

def find_min_price_station(id_list):
    try:
        # Esegui la richiesta HTTP per ottenere il contenuto del file CSV
        response = requests.get(url_csv_benzinaii)
        response.raise_for_status()
        csv_data = response.text

        # Leggi il CSV
        csv_reader = response.text.splitlines(';')

        # Inizializza variabili per tracciare l'idImpianto e il prezzo minore
        min_price = float('inf')
        min_price_station_id = None

        # Itera attraverso le righe del CSV
        for row in csv_reader[2:]:
            row_array = row.split(';')
            # Confronta solo le righe corrispondenti al descCarburante specificato
            if int(row_array[0]) in id_list:
                # Confronta il prezzo e aggiorna se trovi un prezzo minore
                current_price = float(row_array[2])
                if current_price < min_price:
                    min_price = current_price
                    min_price_station_id = int(row_array[0])

        # Restituisci l'idImpianto con il prezzo minore
        return min_price_station_id

    except requests.exceptions.RequestException as req_exc:
        print(f"Errore durante la richiesta HTTP: {req_exc}")
        return None


def get_lat_long_by_id(id_impianto):
    # Esegui la richiesta HTTP per ottenere il contenuto del file CSV
    response = requests.get(url_csv_coordinate)
    response.raise_for_status()
    
    # Crea un oggetto StringIO per simulare un file
    csv_content = StringIO(response.text)
    
    # Leggi il file CSV
    csv_reader = csv.DictReader(csv_content, delimiter=';')
    
    # Cerca il record con l'idImpianto specificato
    for row in csv_reader:
        if row['idImpianto'] == id_impianto:
            return float(row['Longitudine'],float(row['Latitudine']))
    
    # Restituisci None se l'idImpianto non è stato trovato
    return None



def get_directions(start_coords, end_coords):
    global api_key_openroute
    url_openroute = f'https://api.openrouteservice.org/v2/directions/driving-car?api_key={api_key_openroute}&start={start_coords[0]},{start_coords[1]}&end={end_coords[0]},{end_coords[1]}'
    
    # Esegui la richiesta HTTP
    response_openroute = requests.get(url_openroute)

    # Controlla lo stato della risposta
    if response_openroute.status_code == 200:
        # La richiesta è andata a buon fine, ottieni le indicazioni
        directions_data = response_openroute.json()

        # Estrai le coordinate di inizio e fine
        start_location = f'{start_coords[1]},{start_coords[0]}'  # Inverti latitudine e longitudine
        end_location = f'{end_coords[1]},{end_coords[0]}'  # Inverti latitudine e longitudine

        # Costruisci l'URL di Google Maps
        google_maps_url = f'https://www.google.com/maps/dir/{start_location}/{end_location}/'

        print(f'Link per le indicazioni su Google Maps: {google_maps_url}')
        return google_maps_url
    else:
        # Gestisci gli errori in modo appropriato
        print(f"Errore nella richiesta di direzione da OpenRouteService: {response_openroute.status_code}")
        return None


def is_info_set(chat_id):
    global database
    check_query = "SELECT * FROM users WHERE chat_id = %s"
    result = database.execute_query(check_query, (chat_id,))
    if result:
        if result[0][1] and result[0][2] and result[0][3] and result[0][4]:
            return True
        else:
            return False
    else:
        return False
    
def setHowMuchFuel(chat_id, callback_data):
    global database
    check_query = "SELECT * FROM users WHERE chat_id = %s"
    result = database.execute_query(check_query, (chat_id,))
    if result:
        update_query = "UPDATE users SET how_much_fuel = %s WHERE chat_id = %s"
        database.execute_query(update_query, (float(callback_data), chat_id))
    else:
        database.execute_query("INSERT INTO users (chat_id, how_much_fuel) VALUES (%s, %s)", (chat_id, float(callback_data)))

while True:
    try:
        data=RW_telegram.getUpdates()

        for update in data["result"]:
            if "message" in update and "text" in update["message"]:
                message_text = str(update["message"]["text"]).split(" ")
                message_str = str(update["message"]["text"])
                chat_id = update["message"]["chat"]["id"]

                if message_text[0] == "/start":
                    RW_telegram.sendMassage(startMsg, chat_id)

                    database.create_table(f"chat_{chat_id}", ["id INT AUTO_INCREMENT PRIMARY KEY", "message VARCHAR(255)", "date DATETIME"])
                    database.create_table("users", ["chat_id INT", "fuel_type VARCHAR(255)", "capacity VARCHAR(255)", "max_km VARCHAR(255)", "name VARCHAR(255)", "start_position VARCHAR(255)", "how_much_fuel FLOAT"])

                    database.execute_query(f"INSERT INTO chat_{chat_id} (message, date) VALUES (%s, %s)", (message_str, datetime.now()))
                    
                elif message_text[0] == "/setTipoCarburante":
                    if len(message_text) > 1:
                        message_text[1] = message_text[1].lower()
                        check_query = "SELECT chat_id FROM users WHERE chat_id = %s"
                        carbiranti = ["benzina", "gpl", "diesel"]
                        result = database.execute_query(check_query, (chat_id,))
                        if message_text[1] in carbiranti:
                            if result:
                                update_query = "UPDATE users SET fuel_type = %s WHERE chat_id = %s"
                                database.execute_query(update_query, (message_text[1], chat_id))
                            else:
                                database.execute_query("INSERT INTO users (chat_id, fuel_type) VALUES (%s, %s)", (chat_id, message_text[1]))
                            if is_info_set(chat_id):
                                RW_telegram.sendMassage(msgXinfo, chat_id)
                                
                            else:
                                RW_telegram.sendMassage("Fuel type setted, continue to set the other parameters", chat_id)

                        else:
                            RW_telegram.sendMassage("Fuel type not valid", chat_id)

                            
                    else:
                        RW_telegram.sendMassage("Fuel type not valid", chat_id)
                        
                    database.execute_query(f"INSERT INTO chat_{chat_id} (message, date) VALUES (%s, %s)", (message_str, datetime.now()))
                elif message_text[0] == "/setCapacita":
                    if len(message_text) > 1:
                        check_query = "SELECT chat_id FROM users WHERE chat_id = %s"
                        result = database.execute_query(check_query, (chat_id,))
                        if result:
                            update_query = "UPDATE users SET capacity = %s WHERE chat_id = %s"
                            database.execute_query(update_query, (message_text[1], chat_id))
                        else:
                            database.execute_query("INSERT INTO users (chat_id, capacity) VALUES (%s, %s)", (chat_id, message_text[1]))
                        if is_info_set(chat_id):
                            RW_telegram.sendMassage(msgXinfo, chat_id)
                            
                        else:
                            RW_telegram.sendMassage("Capacity setted, continue to set the other parameters", chat_id)
                            
                    else:
                        RW_telegram.sendMassage("Capacity not valid", chat_id)
                        
                    database.execute_query(f"INSERT INTO chat_{chat_id} (message, date) VALUES (%s, %s)", (message_str, datetime.now()))
                elif message_text[0] == "/setMaxKm":
                    if len(message_text) > 1:
                        check_query = "SELECT chat_id FROM users WHERE chat_id = %s"
                        result = database.execute_query(check_query, (chat_id,))
                        if result:
                            update_query = "UPDATE users SET max_km = %s WHERE chat_id = %s"
                            database.execute_query(update_query, (message_text[1], chat_id))
                        else:
                            database.execute_query("INSERT INTO users (chat_id, max_km) VALUES (%s, %s)", (chat_id, message_text[1]))
                        if is_info_set(chat_id):
                            RW_telegram.sendMassage(msgXinfo, chat_id)
                            
                        else:
                            RW_telegram.sendMassage("Max km setted, continue to set the other parameters", chat_id)
                            
                    else:
                        RW_telegram.sendMassage("Max km not valid", chat_id)
                        
                    database.execute_query(f"INSERT INTO chat_{chat_id} (message, date) VALUES (%s, %s)", (message_str, datetime.now()))
                elif message_text[0] == "/setName":
                    if len(message_text) > 1:
                        check_query = "SELECT chat_id FROM users WHERE chat_id = %s"
                        result = database.execute_query(check_query, (chat_id,))
                        if result:
                            update_query = "UPDATE users SET name = %s WHERE chat_id = %s"
                            database.execute_query(update_query, (message_text[1], chat_id))
                        else:
                            database.execute_query("INSERT INTO users (chat_id, name) VALUES (%s, %s)", (chat_id, message_text[1]))
                        if is_info_set(chat_id):
                            RW_telegram.sendMassage(msgXinfo, chat_id)
                        else:
                            RW_telegram.sendMassage("Name setted, continue to set the other parameters", chat_id)
                            
                    else:
                        RW_telegram.sendMassage("Name not valid", chat_id)
                        
                    database.execute_query(f"INSERT INTO chat_{chat_id} (message, date) VALUES (%s, %s)", (message_str, datetime.now()))
                elif message_text[0] == "/getInfo":
                    check_query = "SELECT * FROM users WHERE chat_id = %s"
                    result = database.execute_query(check_query, (chat_id,))
                    if result:
                        RW_telegram.sendMassage(f"Name: {result[0][4]}\nFuel type: {result[0][1]}\nCapacity: {result[0][2]}\nMax km: {result[0][3]}", chat_id)
                        
                    else:
                        RW_telegram.sendMassage("You must set your info", chat_id)
                        
                    database.execute_query(f"INSERT INTO chat_{chat_id} (message, date) VALUES (%s, %s)", (message_str, datetime.now()))
                elif message_text[0] == "/setStartPosition":
                    if is_info_set(chat_id):
                        RW_telegram.sendMassage("Send your position", chat_id)
                        
                    else:
                        RW_telegram.sendMassage("You must set your info", chat_id)
                        
                elif message_text[0] == "/setHowMuchFuel":
                    if is_info_set(chat_id):
                        inline_keyboard = {
                            'inline_keyboard': [
                                [{'text': '1/4', 'callback_data': '0.25'}],
                                [{'text': '2/4', 'callback_data': '0.50'}],
                                [{'text': '3/4', 'callback_data': '0.75'}],
                            ]
                        }
                        response_text = 'Choose how much fuel you have'
                        RW_telegram.sendMassage(response_text, chat_id, json.dumps(inline_keyboard))
                        
                    else:
                        response_text = 'You must set your info'
                        RW_telegram.sendMassage(response_text, chat_id)
                        

                    database.execute_query(f"INSERT INTO chat_{chat_id} (message, date) VALUES (%s, %s)", (message_str, datetime.now()))
                
                elif message_text[0] == "/getGasStation":
                    if all_are_set(chat_id):
                        inline_keyboard = {
                            'inline_keyboard': [
                                [{'text': 'più vicino', 'callback_data': 'vicino'}],
                                [{'text': 'più economico', 'callback_data': 'economico'}],
                            ]
                        }
                        response_text = 'Choose how much fuel you have'
                        RW_telegram.sendMassage(response_text, chat_id, json.dumps(inline_keyboard))
                    else:
                        response_text = 'You must set your info'
                        RW_telegram.sendMassage(response_text, chat_id)
                        
                    database.execute_query(f"INSERT INTO chat_{chat_id} (message, date) VALUES (%s, %s)", (message_str, datetime.now()))
                
                
                else:
                    RW_telegram.sendMassage("Command not valid", chat_id)
                    
                    database.execute_query(f"INSERT INTO chat_{chat_id} (message, date) VALUES (%s, %s)", (message_str, datetime.now()))
                print("Messages processed")
            elif "location" in update.get("message", {}):
                    # Gestisci la posizione
                    location = update["message"]["location"]
                    latitude = location["latitude"]
                    longitude = location["longitude"]
                    chat_id = update["message"]["chat"]["id"]

                    start_coords_str =str(longitude)+";"+str(latitude) # Longitudine, Latitudine del punto di partenza
                    check_query = "SELECT * FROM users WHERE chat_id = %s"
                    result = database.execute_query(check_query, (chat_id,))
                    if result:
                        update_query = "UPDATE users SET start_position = %s WHERE chat_id = %s"
                        database.execute_query(update_query, (start_coords_str, chat_id))
                    else:
                        insert_query = "INSERT INTO users (chat_id,start_coords) VALUES (%s,%s));"

                        database.execute_query(insert_query, (chat_id,start_coords_str))
                    RW_telegram.sendMassage( f"Location received: Latitude {latitude}, Longitude {longitude}", chat_id)
                      
            elif "callback_query" in update:
                # Gestione del clic del pulsante
                callback_query = update["callback_query"]
                callback_data = callback_query["data"]
                chat_id = callback_query["message"]["chat"]["id"]

                # Ora puoi fare qualcosa con il callback_data
                if callback_data == '0.25':
                    setHowMuchFuel(chat_id, callback_data)
                    RW_telegram.sendMassage( "fuel quantity set", chat_id)
                   
                elif callback_data == '0.50':
                    setHowMuchFuel(chat_id, callback_data)
                    RW_telegram.sendMassage( "fuel quantity set", chat_id)
                    
                elif callback_data == '0.75':
                    setHowMuchFuel(chat_id, callback_data)
                    RW_telegram.sendMassage( "fuel quantity set", chat_id)
                    
                elif callback_data == 'economico':
                    RW_telegram.sendMassage("ricerca in corso...", chat_id)
                    
                    risultato_query_sql=database.execute_query("SELECT start_position FROM users WHERE chat_id = %s", (chat_id,))[0][0]
                    # Utilizza una regular expression per estrarre i valori di latitudine e longitudine
                    
                    risultato_query_sql=str(
                                            ).split(";")
                    # Estrai latitudine e longitudine dai gruppi corrispondenti
                    longitudine= float(risultato_query_sql[0])
                    latitudine= float(risultato_query_sql[1])

                    # Crea un array [latitudine, longitudine]
                    start_coords = [longitudine,latitudine]
                    comune=get_comune_from_coordinates(latitudine, longitudine)  
                    type_fuel=database.execute_query("SELECT fuel_type FROM users WHERE chat_id = %s", (chat_id,))[0][0]
                    idImpianti_list = find_station_ids_by_fuel_type(type_fuel)
                    idImpianti_list=get_id_by_comune(idImpianti_list, comune)
                    id_min_price=find_min_price_station(idImpianti_list)
                    end_coord= get_coordinate_by_id(id_min_price)
                    google_link=get_directions(start_coords, end_coord)
                    RW_telegram.sendMassage(google_link, chat_id)
                    
                
                elif callback_data == 'vicino':
                    RW_telegram.sendMassage("ricerca in corso...", chat_id)
                    risultato_query_sql=database.execute_query("SELECT start_position FROM users WHERE chat_id = %s", (chat_id,))[0][0]
                    # Utilizza una regular expression per estrarre i valori di latitudine e longitudine
                    
                    risultato_query_sql=str(risultato_query_sql).split(";")
                    # Estrai latitudine e longitudine dai gruppi corrispondenti
                    longitudine= float(risultato_query_sql[0])
                    latitudine= float(risultato_query_sql[1])

                    # Crea un array [latitudine, longitudine]
                    start_coords = [longitudine,latitudine]

                    comune=get_comune_from_coordinates(latitudine, longitudine)                        
                    type_fuel=database.execute_query("SELECT fuel_type FROM users WHERE chat_id = %s", (chat_id,))[0][0]
                    idImpianti_list = find_station_ids_by_fuel_type(type_fuel)
                    end_coords_list = get_coordinates_by_id(idImpianti_list, comune)
                    end_coord= shortest_route_coordinates(start_coords, end_coords_list)
                    google_link=get_directions(start_coords, end_coord)
                    RW_telegram.sendMassage(google_link, chat_id)
                    
                
                
                print("clic bottone gestito")
        

        sleep(5)

    except requests.exceptions.RequestException as req_exc:
        print(f"Error during HTTP request: {req_exc}")

    except db.MySQLConnectionError as db_exc:
        print(f"Error connecting to MySQL: {db_exc}")

    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        
        
