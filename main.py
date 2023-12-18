import requests
from time import sleep
from datetime import datetime
from connector_db import DatabaseSingleton as db
import json
from io import StringIO
import csv
import pandas as pd



BOOT_TOKEN = "6928800593:AAGWTWod4qh-t3QN-KX5EhD7SmTpV8oSwaI"
URL_TELEGRAM_BOT = f"https://api.telegram.org/bot{BOOT_TOKEN}/"
parametri = {"offset": 0}
database = db()
msgXinfo="use these commands to find the right petrol station for you: \n/setStartPosition to set the starting position \n/setHowMuchFuel to set how much fuel you have\n/getGasStation to receive directions to gas stations"
database.connect("localhost", "root", "", "db_bot_benzinaio")
startMsg="Welcome to the bot, use these commands to set your info: \n/setTipoCarburante to set the fuel type\n/setCapacita to set the capacity of your tank\n/setMaxKm to set the max km you can do with a full tank\n/setName to set your name\n/getInfo to get your info"

api_key_openroute = '5b3ce3597851110001cf624840b7de0666c541c4ad0c21d37ada9a52'

url_csv_benzinaii="https://www.mimit.gov.it/images/exportCSV/prezzo_alle_8.csv"
url_csv_coordinate="https://www.mimit.gov.it/images/exportCSV/anagrafica_impianti_attivi.csv"


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


def shortest_route_coordinates(start_coords, end_coords_list):
    try:
        # Inizializza la lista per tracciare le lunghezze dei percorsi
        route_lengths = []

        # Itera attraverso le coordinate di arrivo
        for end_coords in end_coords_list:
            # Costruisci l'URL di OpenRouteService per ottenere le direzioni
            url_openroute = f'https://api.openrouteservice.org/v2/directions/driving-car?api_key={api_key_openroute}&start={start_coords[0]},{start_coords[1]}&end={end_coords[0]},{end_coords[1]}'
            
            # Esegui la richiesta HTTP
            response_openroute = requests.get(url_openroute)

            # Controlla lo stato della risposta
            if response_openroute.status_code == 200:
                # La richiesta è andata a buon fine, ottieni le indicazioni
                directions_data = response_openroute.json()
                
                # Estrai la lunghezza del percorso
                route_length = directions_data['features'][0]['properties']['segments'][0]['distance']
                route_lengths.append((end_coords, route_length))
            else:
                # Gestisci gli errori in modo appropriato
                print(f"Errore nella richiesta di direzione da OpenRouteService: {response_openroute.status_code}")
                return None

        # Trova le coordinate corrispondenti al percorso più corto
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
        csv_data = response.text

        # Leggi il CSV
        csv_reader = csv.DictReader(StringIO(csv_data), delimiter=';')

        # Inizializza una lista per tracciare gli idImpianto
        station_ids = []

        # Itera attraverso le righe del CSV
        for row in csv_reader:
            # Confronta solo le righe corrispondenti al descCarburante specificato
            if row["descCarburante"] == desc_carburante:
                # Aggiungi l'idImpianto alla lista
                station_ids.append(row["idImpianto"])

        # Restituisci la lista di idImpianto associati al descCarburante
        return station_ids

    except requests.exceptions.RequestException as req_exc:
        print(f"Errore durante la richiesta HTTP: {req_exc}")
        return None

def find_min_price_station(desc_carburante):
    try:
        # Esegui la richiesta HTTP per ottenere il contenuto del file CSV
        response = requests.get(url_csv_benzinaii)
        response.raise_for_status()
        csv_data = response.text

        # Leggi il CSV
        csv_reader = csv.DictReader(StringIO(csv_data), delimiter=';')

        # Inizializza variabili per tracciare l'idImpianto e il prezzo minore
        min_price = float('inf')
        min_price_station_id = None

        # Itera attraverso le righe del CSV
        for row in csv_reader:
            # Confronta solo le righe corrispondenti al descCarburante specificato
            if row["descCarburante"] == desc_carburante:
                # Confronta il prezzo e aggiorna se trovi un prezzo minore
                current_price = float(row["prezzo"])
                if current_price < min_price:
                    min_price = current_price
                    min_price_station_id = row["idImpianto"]

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
            return float(row['Latitudine']), float(row['Longitudine'])
    
    # Restituisci None se l'idImpianto non è stato trovato
    return None


def get_directions(start_coords, end_coords):
    url_openroute = f'https://api.openrouteservice.org/v2/directions/driving-car?api_key={api_key_openroute}&start={start_coords[0]},{start_coords[1]}&end={end_coords[0]},{end_coords[1]}'
    # Esegui la richiesta HTTP
    response_openroute = requests.get(url_openroute)

    # Controlla lo stato della risposta
    if response_openroute.status_code == 200:
        # La richiesta è andata a buon fine, ottieni le indicazioni
        directions_data = response_openroute.json()
        
        # Estrai le coordinate dei punti di passaggio
        waypoints = directions_data['features'][0]['geometry']['coordinates']

        # Costruisci l'URL di Google Maps
        google_maps_url = f'https://www.google.com/maps/dir/'
        
        # Aggiungi i punti di passaggio all'URL
        for waypoint in waypoints:
            google_maps_url += f'{waypoint[1]},{waypoint[0]}/'

        # Apri il link in Google Maps
        print(f'Link per le indicazioni su Google Maps: {google_maps_url[:-1]}')  # Rimuovi l'ultimo carattere "/"
        return google_maps_url[:-1]
    else:
        # Gestisci gli errori in modo appropriato
        print(f"Errore nella richiesta di direzione da OpenRouteService: {response_openroute.status_code}")


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
        resp = requests.get(URL_TELEGRAM_BOT+"getUpdates", params=parametri)
        resp.raise_for_status()
        data = resp.json()

        if "result" in data and data["result"]:
            last_update_id = data["result"][-1]["update_id"]
            parametri["offset"] = last_update_id + 1

            for update in data["result"]:
                if "message" in update and "text" in update["message"]:
                    message_text = str(update["message"]["text"]).split(" ")
                    message_str = str(update["message"]["text"])
                    chat_id = update["message"]["chat"]["id"]

                    if message_text[0] == "/start":
                        requests.post(
                            URL_TELEGRAM_BOT+"sendMessage",
                            data={"chat_id": chat_id, "text": startMsg}
                        )

                        database.create_table(f"chat_{chat_id}", ["id INT AUTO_INCREMENT PRIMARY KEY", "message VARCHAR(255)", "date DATETIME"])
                        database.create_table("users", ["chat_id INT", "fuel_type VARCHAR(255)", "capacity VARCHAR(255)", "max_km VARCHAR(255)", "name VARCHAR(255)", "start_position POINT", "how_much_fuel FLOAT"])

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
                                    requests.post(
                                        URL_TELEGRAM_BOT+"sendMessage",
                                        data={"chat_id": chat_id, "text": msgXinfo}
                                    )
                                else:
                                    requests.post(
                                        URL_TELEGRAM_BOT+"sendMessage",
                                        data={"chat_id": chat_id, "text": "Fuel type setted, continue to set the other parameters"}
                                    )
                            else:
                                requests.post(
                                    URL_TELEGRAM_BOT+"sendMessage",
                                    data={"chat_id": chat_id, "text": "Fuel type not valid"}
                                )
                        else:
                            requests.post(
                                URL_TELEGRAM_BOT+"sendMessage",
                                data={"chat_id": chat_id, "text": "Fuel type not valid"}
                            )
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
                                    requests.post(
                                        URL_TELEGRAM_BOT+"sendMessage",
                                        data={"chat_id": chat_id, "text": msgXinfo}
                                    )
                            else:
                                requests.post(
                                    URL_TELEGRAM_BOT+"sendMessage",
                                    data={"chat_id": chat_id, "text": "Capacity setted, continue to set the other parameters"}
                                )
                        else:
                            requests.post(
                                URL_TELEGRAM_BOT+"sendMessage",
                                data={"chat_id": chat_id, "text": "Capacity not valid"}
                            )
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
                                    requests.post(
                                        URL_TELEGRAM_BOT+"sendMessage",
                                        data={"chat_id": chat_id, "text": msgXinfo}
                                    )
                            else:
                                requests.post(
                                    URL_TELEGRAM_BOT+"sendMessage",
                                    data={"chat_id": chat_id, "text": "Max km setted, continue to set the other parameters"}
                                )
                        else:
                            requests.post(
                                URL_TELEGRAM_BOT+"sendMessage",
                                data={"chat_id": chat_id, "text": "Max km not valid"}
                            )
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
                                    requests.post(
                                        URL_TELEGRAM_BOT+"sendMessage",
                                        data={"chat_id": chat_id, "text": msgXinfo}
                                    )
                            else:
                                requests.post(
                                    URL_TELEGRAM_BOT+"sendMessage",
                                    data={"chat_id": chat_id, "text": "Name setted, continue to set the other parameters"}
                                )
                        else:
                            requests.post(
                                URL_TELEGRAM_BOT+"sendMessage",
                                data={"chat_id": chat_id, "text": "Name not valid"}
                            )
                        database.execute_query(f"INSERT INTO chat_{chat_id} (message, date) VALUES (%s, %s)", (message_str, datetime.now()))
                    elif message_text[0] == "/getInfo":
                        check_query = "SELECT * FROM users WHERE chat_id = %s"
                        result = database.execute_query(check_query, (chat_id,))
                        if result:
                            requests.post(
                                URL_TELEGRAM_BOT+"sendMessage",
                                data={"chat_id": chat_id, "text": f"Name: {result[0][4]}\nFuel type: {result[0][1]}\nCapacity: {result[0][2]}\nMax km: {result[0][3]}"}
                            )
                        else:
                            requests.post(
                                URL_TELEGRAM_BOT+"sendMessage",
                                data={"chat_id": chat_id, "text": "You must set your info"}
                            )
                        database.execute_query(f"INSERT INTO chat_{chat_id} (message, date) VALUES (%s, %s)", (message_str, datetime.now()))
                    elif message_text[0] == "/setStartPosition":
                        if is_info_set(chat_id):
                            requests.post(
                                URL_TELEGRAM_BOT+"sendMessage",
                                data={"chat_id": chat_id, "text": "Send your position"}
                            )
                        else:
                            requests.post(
                                URL_TELEGRAM_BOT+"sendMessage",
                                data={"chat_id": chat_id, "text": "You must set your info"}
                            )
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
                            response = requests.post(
                                URL_TELEGRAM_BOT + 'sendMessage',
                                data={'chat_id': chat_id, 'text': response_text, 'reply_markup': json.dumps(inline_keyboard)}
                            )
                        else:
                            response_text = 'You must set your info'
                            requests.post(
                                URL_TELEGRAM_BOT + "sendMessage",
                                data={"chat_id": chat_id, "text": response_text}
                            )

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
                            response = requests.post(
                                URL_TELEGRAM_BOT + 'sendMessage',
                                data={'chat_id': chat_id, 'text': response_text, 'reply_markup': json.dumps(inline_keyboard)}
                            )
                        else:
                            response_text = 'You must set your info'
                            requests.post(
                                URL_TELEGRAM_BOT + "sendMessage",
                                data={"chat_id": chat_id, "text": response_text}
                            )

                        database.execute_query(f"INSERT INTO chat_{chat_id} (message, date) VALUES (%s, %s)", (message_str, datetime.now()))
                    
                    
                    else:
                        requests.post(
                            URL_TELEGRAM_BOT+"sendMessage",
                            data={"chat_id": chat_id, "text": "Command not valid"}
                        )
                        database.execute_query(f"INSERT INTO chat_{chat_id} (message, date) VALUES (%s, %s)", (message_str, datetime.now()))
                    print("Messages processed")
                elif "location" in update.get("message", {}):
                        # Gestisci la posizione
                        location = update["message"]["location"]
                        latitude = location["latitude"]
                        longitude = location["longitude"]
                        chat_id = update["message"]["chat"]["id"]

                        start_coords =[longitude,latitude] # Longitudine, Latitudine del punto di partenza
                        check_query = "SELECT * FROM users WHERE chat_id = %s"
                        result = database.execute_query(check_query, (chat_id,))
                        if result:
                            update_query = "UPDATE users SET start_position = POINT(%s, %s) WHERE chat_id = %s"
                            database.execute_query(update_query, (start_coords[0],start_coords[1], chat_id))
                        else:
                            insert_query = "INSERT INTO users (chat_id,start_coords) VALUES (%s,POINT(%s, %s));"

                            database.execute_query(insert_query, (chat_id,start_coords[0],start_coords[1]))
                        requests.post(
                            URL_TELEGRAM_BOT + "sendMessage",
                            data={"chat_id": chat_id, "text": f"Location received: Latitude {latitude}, Longitude {longitude}"}
                            )   
                elif "callback_query" in update:
                    # Gestione del clic del pulsante
                    callback_query = update["callback_query"]
                    callback_data = callback_query["data"]
                    chat_id = callback_query["message"]["chat"]["id"]

                    # Ora puoi fare qualcosa con il callback_data
                    if callback_data == '0.25':
                        setHowMuchFuel(chat_id, callback_data)
                        requests.post(
                            URL_TELEGRAM_BOT+"sendMessage",
                            data={"chat_id": chat_id, "text": "fuel quantity set"}
                        )
                    elif callback_data == '0.50':
                        setHowMuchFuel(chat_id, callback_data)
                        requests.post(
                            URL_TELEGRAM_BOT+"sendMessage",
                            data={"chat_id": chat_id, "text": "fuel quantity set"}
                        )
                    elif callback_data == '0.75':
                        setHowMuchFuel(chat_id, callback_data)
                        requests.post(
                            URL_TELEGRAM_BOT+"sendMessage",
                            data={"chat_id": chat_id, "text": "fuel quantity set"}
                        )
                    elif callback_data == 'economico':
                        pass
                    elif callback_data == 'vicino':
                        start_coords=database.execute_query("SELECT start_position FROM users WHERE chat_id = %s", (chat_id,))[0][0]
                        type_fuel=database.execute_query("SELECT fuel_type FROM users WHERE chat_id = %s", (chat_id,))[0][0]
                        end_coords_list = find_station_ids_by_fuel_type(database)
                        end_coord= shortest_route_coordinates(start_coords, end_coords_list)
                        google_link=get_directions(start_coords, end_coord)
                        requests.post(
                            URL_TELEGRAM_BOT+"sendMessage",
                            data={"chat_id": chat_id, "text": google_link}
                        )
                        
                    
                    
                    print("clic bottone gestito")
        else:
            print("No updates")

        sleep(5)

    except requests.exceptions.RequestException as req_exc:
        print(f"Error during HTTP request: {req_exc}")

    except db.MySQLConnectionError as db_exc:
        print(f"Error connecting to MySQL: {db_exc}")

    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        
        
