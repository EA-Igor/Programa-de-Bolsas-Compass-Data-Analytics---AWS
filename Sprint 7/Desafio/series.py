import os
import json
import requests
from datetime import datetime
from dotenv import load_dotenv

# variáveis de ambiente
load_dotenv('.env')

tmdb_api_key = os.getenv('TMDB_API_KEY')

# URL da API do TMDb
base_url = "https://api.themoviedb.org/3"

# converção de objetos para dicionários
def convert_to_dict(obj):
    if hasattr(obj, '__dict__'):
        return obj.__dict__
    else:
        raise TypeError(f"Objeto {obj} não pode ser convertido para dicionário")

# Função para obter detalhes da série e do ator principal
def get_tv_show_details(tv_id):
    details_url = f"{base_url}/tv/{tv_id}"
    credits_url = f"{base_url}/tv/{tv_id}/credits"

    details_response = requests.get(details_url, params={'api_key': tmdb_api_key})
    credits_response = requests.get(credits_url, params={'api_key': tmdb_api_key})

    details = details_response.json()
    credits = credits_response.json()
    main_actor = credits['cast'][0]['name'] if credits['cast'] else "N/A"

    tv_show_data = {
        "id": details['id'],
        "name": details['name'],
        "first_air_date": details['first_air_date'],
        "original_language": details['original_language'],
        "overview": details['overview'],
        "popularity": details['popularity'],
        "vote_average": details['vote_average'],
        "vote_count": details['vote_count'],
        "main_actor": main_actor
    }
    
    return tv_show_data

# Função para buscar dados do TMDb
def fetch_romance_drama_tv_shows():
    tmdb_data = []

    # IDs dos gêneros Romance e Drama
    romance_genre_id = 10749
    drama_genre_id = 18

    # URL de descoberta de séries
    discover_url = f"{base_url}/discover/tv"

    # Busca pelas séries de romance e drama
    page = 1
    while True:
            response = requests.get(discover_url, params={
                'api_key': tmdb_api_key,
                'with_genres': f"{romance_genre_id},{drama_genre_id}",
                'page': page
            })
            tv_data = response.json().get('results', [])
            if not tv_data:
                break
            
            for tv_show in tv_data:
                    tv_show_details = get_tv_show_details(tv_show['id'])
                    tmdb_data.append(tv_show_details)


            print(f"Página {page} de séries obtida com sucesso.")
            page += 1

            if len(tmdb_data) >= 100:
                yield tmdb_data[:100]
                tmdb_data = tmdb_data[100:]

    if tmdb_data:
        yield tmdb_data
