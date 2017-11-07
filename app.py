import os
import base64

from chalice import Chalice, Cron
from chalicelib.cache import Cache
from chalicelib.store import DogDB
from sodapy import Socrata
from twilio.rest import Client as TwilioClient
import boto3

ENDPOINT = 'data.kingcounty.gov'

app = Chalice(app_name='dog-tracker')
kms = boto3.client('kms')
cache = Cache()


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


# I wanted to do every hour within range of business hours, but that would
# have required multiple crons, which isn't something you can do for a single
# function unfortunately. Splitting it up into multiple functions would also
# have sucked because then I would be creating a new lambda function for it.
# At the end of the day, it's not that much extra data.
@app.schedule(Cron(0, '*', '*', '*', '?', '*'))
def snapshot(event):
    get_dog_db().snapshot_current()


# This is in GMT, so 12:30pm -> 4:30am... excpet when daylight savings is doing
# its horrible thing, in which case 12:30pm -> 5:30am. Thankfully I don't wake
# up to the sound of my phone going off.
@app.schedule(Cron(30, 12, '?', '*', 'SAT', '*'))
def remind_me(event):
    text_stats()


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


@cache.cache
def get_socrata_client():
    app_token = _decrypt_env('APP_TOKEN')
    return Socrata(ENDPOINT, app_token)


@cache.cache
def get_twilio_client():
    account_sid = _decrypt_env('TWILIO_ACCOUNT_SID')
    auth_token = _decrypt_env('TWILIO_AUTH_TOKEN')
    return TwilioClient(account_sid, auth_token)


@cache.cache
def _decrypt_env(key):
    blob = base64.b64decode(os.environ[key])
    return kms.decrypt(CiphertextBlob=blob)['Plaintext'].decode('utf-8')


@cache.cache
def get_dog_db():
    return DogDB(get_socrata_client(), os.environ['DOGDB_TABLE_NAME'])


def get_adoptable_dogs():
    return get_dog_db().get_current()


def categorize_dogs(dogs):
    categorized = {}
    for dog in dogs:
        color = categorize_dog(dog)
        if color not in categorized:
            categorized[color] = []
        categorized[color].append(dog)
    return categorized


def categorize_dog(dog):
    description = dog.get('memo', '')
    colors = ['RED', 'GREEN', 'BLUE']
    for color in colors:
        if color in description:
            return color
    return 'NOCOLOR'

