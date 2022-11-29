import numbers
import traceback

import psycopg2

from config import DB_CONFIG


class Singleton(type):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]


class ScraperDB(metaclass=Singleton):
    """
    use Singleton to re-use database connection
    """
    COL_NAMES = ["price_source", "datetime", "zone_code", "pickup_address", "pickup_country_code", "pickup_postal_code",
                 "delivery_address", "delivery_country_code", "delivery_postal_code", "ldm", "weight", "distance",
                 "price", "currency", "delivery_type", "courier", "loading_type", "pickup_date", "delivery_date",
                 "delivery_date_2nd", "delivery_time", "with_insurance"]

    def __init__(self):
        db_cf = DB_CONFIG.copy()
        self._TABLE_DATA_SCRAPPED = db_cf['table']
        del db_cf['table']

        self._conn = psycopg2.connect(**db_cf)
        del db_cf

    def _mapping_pickup_delivery_dates(self, row):
        if "pickup_dates" in row:
            if row["pickup_dates"]:
                row["pickup_date"] = row["pickup_dates"][0]
            del row["pickup_dates"]
        if "delivery_dates" in row:
            row["delivery_date"] = row["delivery_dates"][0] if row["delivery_dates"] else None
            row["delivery_date_2nd"] = row["delivery_dates"][1] if len(row["delivery_dates"]) == 2 else None
            del row["delivery_dates"]
        return row

    def insert(self, row: dict):
        """
        insert a data sample into scrapped_data table
        make sure that row.key matches every column.name in targeted table
        :param row: a data sample, output from crawler (e.g: CrawlerBase.run())
        """
        row = self._mapping_pickup_delivery_dates(row)

        for k in list(row.keys()):
            if k not in ScraperDB.COL_NAMES:
                del row[k]

        # deal with float/string value
        remove_single_quote = lambda x: x.replace("'", "''")
        convert = lambda x: f"{x}" if isinstance(x, numbers.Number) else \
            f"'{remove_single_quote(x)}'" if isinstance(x, str) else 'NULL'

        try:
            cur = self._conn.cursor()

            keys = list(row.keys())
            values = [convert(row[k]) for k in keys]
            query = f"INSERT INTO {self._TABLE_DATA_SCRAPPED} ({','.join(keys)}) VALUES ({', '.join(values)})"
            print(query)

            cur.execute(query)
            self._conn.commit()
        except:
            self._conn.rollback()
            print(traceback.format_exc())

    def close_connection(self):
        self._conn.close()

# sample usage
# row = {
#     "price_source": "test",
#     "zone_code": "EU",
#     "pickup_address": "",
#     "pickup_country_code": "IT",
#     "pickup_postal_code": "",
#     "delivery_address": "",
#     "delivery_country_code": "AT",
#     "delivery_postal_code": "",
#     "ldm": 0.5640416666666668,
#     "weight": 953.0,
#     "distance": 646.8278021539612,
#     "price": 616.42,
#     "courier": "test",
#     "currency": "EUR",
#     "type": None,
#     "delivery_type": "ltl"
# }
# ScraperDB().insert(row)
