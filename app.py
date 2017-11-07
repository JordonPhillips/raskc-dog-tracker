import os
import base64

from chalice import Chalice
from sodapy import Socrata
from twilio.rest import Client as TwilioClient
import boto3

ANIMAL_DATASET = 'yaai-7frk'
ENDPOINT = 'data.kingcounty.gov'
_SOCRATA_CLIENT = None
_TWILIO_CLIENT = None

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


@app.route('/text')
def text_stats():
    _text_stats(_decrypt_env("DEST_PHONE_NUMBER"))

def _text_stats(dest_number):
    dog_data = categorize_dogs(get_adoptable_dogs())
    dog_data = dict((k, len(v)) for k, v in dog_data.items())
    message = (
        'There are currently %s green dogs, %s blue dogs, %s red dogs, and '
        '%s unassigned dogs.'
    )
    message = message % (
        dog_data.get('GREEN'), dog_data.get('BLUE'), dog_data.get('RED'),
        dog_data.get('NOCOLOR')
    )
    client = get_twilio_client()
    client.messages.create(
        to=dest_number,
        from_=_decrypt_env("SOURCE_PHONE_NUMBER"),
        body=message
    )


def get_socrata_client():
    if _SOCRATA_CLIENT is not None:
        return _SOCRATA_CLIENT
    return _setup_socrata_client()


def _setup_socrata_client():
    global _SOCRATA_CLIENT
    app_token = _decrypt_env('APP_TOKEN')
    _SOCRATA_CLIENT = Socrata(ENDPOINT, app_token)
    return _SOCRATA_CLIENT


def _decrypt_env(key):
    blob = base64.b64decode(os.environ[key])
    return kms.decrypt(CiphertextBlob=blob)['Plaintext'].decode('utf-8')


def get_twilio_client():
    if _TWILIO_CLIENT is not None:
        return _TWILIO_CLIENT
    return _setup_twilio_client()


def _setup_twilio_client():
    global _TWILIO_CLIENT
    account_sid = _decrypt_env('TWILIO_ACCOUNT_SID')
    auth_token = _decrypt_env('TWILIO_AUTH_TOKEN')
    return TwilioClient(account_sid, auth_token)


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
    return 'NOCOLOR'

