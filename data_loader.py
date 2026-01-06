# data_loader.py
# --------------------------------------------------
# PURPOSE:
# Centralized data loading utilities
# Loads:
#   1. O_Production (component-wise, SQL Server)
#   2. I_Material Stage-1 & Stage-2 (SQL Server)
#   3. Inventory (MySQL)
# --------------------------------------------------

import pandas as pd
import pyodbc
from sqlalchemy import create_engine


# ==================================================
# SQL SERVER CONNECTION (MES)
# ==================================================
def get_sqlserver_connection():
    """
    Returns SQL Server connection for MESinterface
    """
    conn_str = (
        "DRIVER={ODBC Driver 17 for SQL Server};"
        "SERVER=10.147.17.61;"
        "DATABASE=MESinterface;"
        "UID=JKBTP_USER;"
        "PWD=JK@$userbtp;"
    )
    return pyodbc.connect(conn_str)


# ==================================================
# MYSQL ENGINE (PLANNING / INVENTORY)
# ==================================================
def get_mysql_engine(host, user, password, database):
    """
    Returns SQLAlchemy MySQL engine
    """
    return create_engine(
        f"mysql+pymysql://{user}:{password}@{host}/{database}",
        pool_pre_ping=True
    )


# ==================================================
# LOAD O_PRODUCTION (COMPONENT + TIME)
# ==================================================
def load_o_production(component, conn, start_time, end_time):
    """
    Loads O_Production data for a given component and time range
    """

    component_map = {
        'TREAD': ['DualEx', 'Duplex', 'QuintuPlex'],
        'INNER LINER': ['trc', 'TRCNew'],
        'BELT': ['WBC', 'WBCNew'],
        'SIDEWALL': ['TRIPLEX', 'Duplex', 'QuintuPlex'],
        'PLY' : ['LTBC','LTBCNew','HTBC'],
        'INSULATED BEAD' :['AllWell','VIPOBeadWinding']
    }

    schemas = component_map.get(component)
    if not schemas:
        raise ValueError(f"Invalid component: {component}")

    df_list = []

    for schema in schemas:
        query = f"""
        SELECT
            [MachineNo],
            [MachineCode],
            [ProductionID],
            [ItemCode],
            [ItemName],
            [TotalQuantity],
            [ScrapQuantity],
            [SyncTime],
            [UserName],
            [MHECode],
            [QualityStatus],
            [LiveQty],
            '{schema}' AS source_schema
        FROM {schema}.O_Production
        WHERE SyncTime BETWEEN ? AND ?
        """

        df = pd.read_sql(query, conn, params=[start_time, end_time])
        df_list.append(df)

    return pd.concat(df_list, ignore_index=True) if df_list else pd.DataFrame()


# ==================================================
# LOAD I_MATERIAL (STAGE-1 + STAGE-2)
# ==================================================
def load_i_material(conn, start_time, end_time):
    """
    Loads I_Material data for Stage-1 and Stage-2
    """

    stages = [
        ("TBMStage1", "STAGE1"),
        ("TBMStage2", "STAGE2")
    ]

    df_list = []

    for schema, stage in stages:
        query = f"""
        SELECT
            [Lot_Id],
            [UOM],
            [Qty],
            [MaterialCode],
            [LiveQty],
            [dtandTime],
            '{stage}' AS stage,
            '{schema}' AS source_schema
        FROM {schema}.I_Material
        WHERE dtandTime BETWEEN ? AND ?
        """

        df = pd.read_sql(query, conn, params=[start_time, end_time])
        df_list.append(df)

    return pd.concat(df_list, ignore_index=True) if df_list else pd.DataFrame()


# ==================================================
# LOAD INVENTORY (MYSQL)
# ==================================================
def load_inventory(start_time, end_time):
    host = "35.208.174.2"
    user = "root"
    password = "Dev112233"
    database = "jkplanningV1"

    engine = get_mysql_engine(host, user, password, database)

    query = """
    SELECT *
    FROM Inventory
    WHERE productionTime BETWEEN %s AND %s
    """

    # ✅ FIX: params must be a TUPLE
    df = pd.read_sql(query, engine, params=(start_time, end_time))

    return df
