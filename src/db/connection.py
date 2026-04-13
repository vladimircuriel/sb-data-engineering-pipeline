import os

import psycopg2


def get_conn():
    return psycopg2.connect(os.environ["LANDING_DB_CONN"])
