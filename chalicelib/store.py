import datetime
import json

import boto3

ANIMAL_DATASET = 'yaai-7frk'
SNASHOT_PREFIX = 'hourly-snapshots/'


class DogDB(object):
    def __init__(self, socrata_client, bucket_name):
        self._socrata_client = socrata_client
        self._s3 = boto3.client('s3')
        self._bucket_name = bucket_name

    def snapshot_current(self):
        """Takes a snapshot of what the current data is."""
        timestamp = datetime.datetime.utcnow().isoformat()
        dog_info = json.dumps(self.get_current(), indent=2) + '\n'
        key = "%s%s.json" % (SNASHOT_PREFIX, timestamp)
        self._s3.put_object(
            Bucket=self._bucket_name, Key=key, Body=dog_info,
            ContentType='application/json'
        )

    def get_current(self):
        return self._socrata_client.get(
            ANIMAL_DATASET, record_type='ADOPTABLE', animal_type='Dog'
        )

