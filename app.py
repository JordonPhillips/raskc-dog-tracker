import os

from chalice import Chalice
from sodapy import Socrata

ANIMAL_DATASET = 'yaai-7frk'

app = Chalice(app_name='dog-tracker')
data_client = Socrata('data.kingcounty.gov', os.environ.get('APP_TOKEN'))


@app.route('/')
def index():
    return get_adoptable_dogs()

def get_adoptable_dogs():
    return data_client.get(
        ANIMAL_DATASET, record_type='ADOPTABLE', animal_type='Dog'
    )

