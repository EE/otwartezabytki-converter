import json
import logging
import os

import pandas

logger = logging.getLogger(__name__)


source_dir = 'relics-json'


def flatten(d, drop_lists=False):
    r = {}
    for k, v in d.items():
        if isinstance(v, dict):
            for k2, v2 in flatten(v).items():
                r[k + '.' + k2] = v2
        elif drop_lists and isinstance(v, list):
            pass
        else:
            r[k] = v
    return r


class RelicDB:
    def __init__(self):
        self.relics = []
        self.events = []
        self.links = []
        self.alerts = []
        self.photos = []
        self.entries = []
        self.documents = []

    def add_relic(self, raw_data):
        raw_data.setdefault('ancestor_id', None)
        for descendant in raw_data.pop('descendants'):
            descendant['ancestor_id'] = raw_data['id']
            self.add_relic(descendant)

        for field, db in [
            ('events', self.events),
            ('links', self.links),
            ('alerts', self.alerts),
            ('photos', self.photos),
            ('entries', self.entries),
            ('documents', self.documents),
        ]:
            for event in raw_data.pop(field):
                event['relic_id'] = raw_data['id']
                db.append(event)

        allowed_lists = [
            'categories',
            'tags',
        ]
        for key, value in raw_data.items():
            if key in allowed_lists:
                pass
            elif isinstance(value, list) and value:
                print(key, value)
                raise Exception('Unexpected list')

        self.relics.append(raw_data)

    def to_excel(self, filename):
        with pandas.ExcelWriter(filename) as writer:

            logger.info('Writing relics')
            relics_df = pandas.DataFrame(
                [flatten(r) for r in self.relics],
            )
            relics_df['ancestor_id'] = relics_df['ancestor_id'].astype('Int64')
            relics_df.to_excel(writer, sheet_name='relics', index=False)

            for field, db in [
                ('events', self.events),
                ('links', self.links),
                ('alerts', self.alerts),
                ('photos', self.photos),
                ('entries', self.entries),
                ('documents', self.documents),
            ]:
                logger.info('Writing %s', field)
                df = pandas.DataFrame(
                    [flatten(r) for r in db],
                )
                df.to_excel(writer, sheet_name=field, index=False)

            logger.info('Saving')


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    db = RelicDB()

    logger.info('Loading data')
    for filename in os.listdir(source_dir):
        with open(os.path.join(source_dir, filename), 'rt') as f:
            data = json.load(f)
        db.add_relic(data)

    logger.info('Dumping data')
    db.to_excel('relics.xlsx')
