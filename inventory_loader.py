# data_loader.py
# --------------------------------------------------
# PURPOSE:
# Centralized data loading utilities for CTB pipeline
# --------------------------------------------------

import pandas as pd
from sqlalchemy import create_engine


# ==================================================
# COMMON MYSQL CONNECTION FACTORY
# ==================================================
def get_mysql_engine(host, user, password, database):
    """
    Creates and returns a SQLAlchemy MySQL engine
    """
    return create_engine(
        f"mysql+pymysql://{user}:{password}@{host}/{database}",
        pool_pre_ping=True
    )


# ==================================================
# LOAD INVENTORY FROM jkplanningV1 (O_Production)
# ==================================================
def load_inventory_mysql(
    start_datetime: str = None,
    end_datetime: str = None
):
    """
    Loads inventory (O_Production) data from jkplanningV1
    and returns an item-wise available quantity dictionary.

    Parameters
    ----------
    start_datetime : str (optional)
        Format: 'YYYY-MM-DD HH:MM:SS'
    end_datetime : str (optional)
        Format: 'YYYY-MM-DD HH:MM:SS'

    Returns
    -------
    dict
        { ItemCode : AvailableQuantity }
    """

    # -----------------------
    # DB CONFIG (READ ONLY)
    # -----------------------
    host = "35.208.174.2"
    user = "root"
    password = "Dev112233"
    database = "jkplanningV1"

    engine = get_mysql_engine(host, user, password, database)

    # -----------------------
    # BASE QUERY
    # -----------------------
    query = """
    SELECT
       * from Inventory
    """

    # -----------------------
    # DATE FILTER (OPTIONAL)
    # -----------------------
    if start_datetime and end_datetime:
        query += f"""
        WHERE SyncTime >= '{start_datetime}'
          AND SyncTime <  '{end_datetime}'
        """

    # -----------------------
    # LOAD DATA
    # -----------------------
    df = pd.read_sql(query, engine)


    return df
