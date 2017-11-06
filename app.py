import os

from chalice import Chalice
from sodapy import Socrata

ANIMAL_DATASET = 'yaai-7frk'

app = Chalice(app_name='dog-tracker')
data_client = Socrata('data.kingcounty.gov', os.environ.get('APP_TOKEN'))


@app.route('/')
def adoptable_dogs():
    dog_data = get_adoptable_dogs()
    return categorize_dogs(dog_data)


@app.route('/count')
def color_count():
    colors = app.current_request.query_params.get('colors')
    dog_data = get_adoptable_dogs()
    if not colors:
        return len(dog_data)

    colors = [c.upper() for c in colors.split(',') if c]
    dog_data = categorize_dogs(dog_data)
    return sum(len(dogs) for color, dogs in dog_data.items()
               if color in colors)


def get_adoptable_dogs():
    return data_client.get(
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

