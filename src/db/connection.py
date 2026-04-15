import os

import psycopg2


def get_conn():
    """Open and return a new psycopg2 connection to the PostgreSQL landing zone.

    Returns:
        psycopg2.extensions.connection: An open database connection.

    Raises:
        KeyError: If the ``LANDING_DB_CONN`` environment variable is not set.
        psycopg2.OperationalError: If the connection attempt fails.
    """
    return psycopg2.connect(os.environ["LANDING_DB_CONN"])
