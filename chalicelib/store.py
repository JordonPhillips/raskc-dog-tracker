import datetime

import boto3

ANIMAL_DATASET = 'yaai-7frk'


class DogDB(object):
    def __init__(self, socrata_client, table_name):
        self._socrata_client = socrata_client
        self._ddb = boto3.resource('dynamodb')
        self._table = self._ddb.Table(table_name)

    def snapshot_current(self):
        """Takes a snapshot of what the current data is."""
        timestamp = datetime.datetime.utcnow().isoformat()
        # Is this schema stupid? You bet. But I need to account for
        # changes and this is the easy way to do that.
        dog_info = self.get_current()
        self._table.put_item(Item={'timestamp': timestamp, 'data': dog_info})

    def get_current(self):
        return self._socrata_client.get(
            ANIMAL_DATASET, record_type='ADOPTABLE', animal_type='Dog'
        )

