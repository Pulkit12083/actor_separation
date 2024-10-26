import requests
import json

from typing import Dict, Any, Optional
from urllib.request import Request
from requests.exceptions import RequestException

MOVIEBUFF_URL = 'https://data.moviebuff.com/'


# use config file to set parameters
def get_json_data(requested_url, timeout = 30, verify_ssl = True):
    moviebuff_data_url = MOVIEBUFF_URL + requested_url
    try:
        response = requests.get(
            moviebuff_data_url,
            timeout= timeout,
            verify= verify_ssl
        )
        response.raise_for_status()
        return response.json()

    except RequestException as error:
        return None
    except json.JSONDecodeError as error:
        return None


def check_valid_url(person_url):
    person_data = get_json_data(person_url)
    if person_data is None:
        return False
    return True