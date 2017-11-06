import os
import base64

from chalice import Chalice
from sodapy import Socrata
import boto3

ANIMAL_DATASET = 'yaai-7frk'
ENDPOINT = 'data.kingcounty.gov'
_SOCRATA_CLIENT = None

app = Chalice(app_name='dog-tracker')
kms = boto3.client('kms')


@app.route('/')
def adoptable_dogs():
    dog_data = get_adoptable_dogs()
    return categorize_dogs(dog_data)


@app.route('/count')
def color_count():
    params = app.current_request.query_params or {}
    colors = params.get('colors')
    dog_data = get_adoptable_dogs()
    if not colors:
        return len(dog_data)

    colors = [c.upper() for c in colors.split(',') if c]
    dog_data = categorize_dogs(dog_data)
    return sum(len(dogs) for color, dogs in dog_data.items()
               if color in colors)


def get_socrata_client():
    if _SOCRATA_CLIENT is not None:
        return _SOCRATA_CLIENT
    return _setup_socrata_client()


def _setup_socrata_client():
    global _SOCRATA_CLIENT
    blob = base64.b64decode(os.environ['APP_TOKEN'])
    app_token = kms.decrypt(CiphertextBlob=blob)['Plaintext']
    _SOCRATA_CLIENT = Socrata(ENDPOINT, app_token)
    return _SOCRATA_CLIENT


def get_adoptable_dogs():
    return get_socrata_client().get(
        ANIMAL_DATASET, record_type='ADOPTABLE', animal_type='Dog'
    )


def categorize_dogs(dogs):
    categorized = {}
    for dog in dogs:
        color = categorize_dog(dog)
        if color not in categorized:
            categorized[color] = []
        categorized[color].append(dog)
    return categorized


def categorize_dog(dog):
    description = dog.get('memo')
    if description is None:
        return
    colors = ['RED', 'GREEN', 'BLUE']
    for color in colors:
        if color in description:
            return color

