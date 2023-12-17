import requests


def shortest_route_coordinates(api_key, start_coords, end_coords_list):
    try:
        # Inizializza la lista per tracciare le lunghezze dei percorsi
        route_lengths = []

        # Itera attraverso le coordinate di arrivo
        for end_coords in end_coords_list:
            # Costruisci l'URL di OpenRouteService per ottenere le direzioni
            url_openroute = f'https://api.openrouteservice.org/v2/directions/driving-car?api_key={api_key}&start={start_coords[0]},{start_coords[1]}&end={end_coords[0]},{end_coords[1]}'
            
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

# Esempio di utilizzo
start_coords = [40.7128, -74.0060]  # Sostituisci con le tue coordinate di partenza
end_coords_list = [[41.8781, -87.6298], [34.0522, -118.2437], [37.7749, -122.4194]]  # Sostituisci con le tue coordinate di arrivo
api_key_openroute = '5b3ce3597851110001cf624840b7de0666c541c4ad0c21d37ada9a52'
result = shortest_route_coordinates(api_key_openroute, start_coords, end_coords_list)

if result:
    print(f"Le coordinate di arrivo per il percorso più corto sono: {result}")
else:
    print("Errore nella ricerca del percorso più corto.")