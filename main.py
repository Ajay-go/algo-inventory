import pandas as pd
import numpy as np
from data_saver import save_inventory_check_files

components = ['TREAD','INNER LINER','BELT','SIDEWALL','PLY','INSULATED BEAD']

def split_balance_o_production_with_inventory(
    component: str,
    balance_o_prod_df: pd.DataFrame,
    inventory_df: pd.DataFrame
):
    """
    Splits balanced O_Production into:
    1. ProductionIDs FOUND in Inventory (LotNo)
    2. ProductionIDs NOT FOUND in Inventory
    """

    print(f"📦 Splitting balance production for {component} vs Inventory")

    if balance_o_prod_df.empty:
        print("⚠️ Balanced O_Production is empty")
        return pd.DataFrame(), pd.DataFrame()

    if inventory_df.empty:
        print("⚠️ Inventory is empty — all rows treated as NOT FOUND")
        return pd.DataFrame(), balance_o_prod_df.copy()

    # -------------------------------
    # CLEAN IDS
    # -------------------------------
    balance_o_prod_df["ProductionID_clean"] = (
        balance_o_prod_df["ProductionID"]
        .astype(str)
        .str.strip()
    )

    inventory_df["LotNo_clean"] = (
        inventory_df["lotNo"]
        .astype(str)
        .str.strip()
    )

    # -------------------------------
    # INVENTORY LOT SET
    # -------------------------------
    inventory_lots = set(
        inventory_df["LotNo_clean"]
        .dropna()
        .unique()
    )

    # -------------------------------
    # SPLIT DATA
    # -------------------------------
    found_in_inventory_df = balance_o_prod_df[
        balance_o_prod_df["ProductionID_clean"].isin(inventory_lots)
    ].copy()

    not_found_in_inventory_df = balance_o_prod_df[
        ~balance_o_prod_df["ProductionID_clean"].isin(inventory_lots)
    ].copy()

    # -------------------------------
    # CLEANUP
    # -------------------------------
    found_in_inventory_df.drop(columns=["ProductionID_clean"], inplace=True)
    not_found_in_inventory_df.drop(columns=["ProductionID_clean"], inplace=True)

    print(
        f"✅ {component} | "
        f"Found in inventory: {len(found_in_inventory_df)} | "
        f"Not found: {len(not_found_in_inventory_df)}"
    )

    return found_in_inventory_df, not_found_in_inventory_df


from data_loader import (
    get_sqlserver_connection,
    load_o_production,
    load_i_material,
    load_inventory
)

# establish connection
conn = get_sqlserver_connection()

start_time = "2026-01-03 07:00:00" # custom dates
end_time   = "2026-01-04 07:00:00"

print("loading i mat and inventory")

i_mat_df  = load_i_material(conn, start_time, end_time) # get i materials
inv_df    = load_inventory(start_time, end_time)        # get inventory 

print("loaded i mat and inventory")


def get_balanced_o_production_for_component(
    component: str,
    i_material_df: pd.DataFrame,
    start_time: str,
    end_time: str
):
    print(f"✅ getting balance production id for {component} .......")
    """
    Loads O_Production for a given component and time window,
    removes ProductionIDs already present in I_Material (Lot_Id),
    and returns balance O_Production.
    """

    # -------------------------------
    # Load O_Production ONLY
    # -------------------------------

    o_prod_df = load_o_production(
        component=component,
        conn=conn,
        start_time=start_time,
        end_time=end_time
    )

    conn.close()

    if o_prod_df.empty:
        print(f"⚠️ No O_Production data for component {component}")
        return o_prod_df

    if i_material_df.empty:
        print("⚠️ I_Material is empty — returning full O_Production")
        return o_prod_df

    # -------------------------------
    # CLEAN IDS
    # -------------------------------
    o_prod_df["ProductionID_clean"] = (
        o_prod_df["ProductionID"]
        .astype(str)
        .str.strip()
    )

    i_material_df["Lot_Id_clean"] = (
        i_material_df["Lot_Id"]
        .astype(str)
        .str.strip()
    )

    # -------------------------------
    # CONSUMED IDS (FIXED SET)
    # -------------------------------
    consumed_ids = set(
        i_material_df["Lot_Id_clean"]
        .dropna()
        .unique()
    )

    # -------------------------------
    # ANTI-JOIN (BALANCE)
    # -------------------------------
    balance_o_prod_df = o_prod_df[
        ~o_prod_df["ProductionID_clean"].isin(consumed_ids)
    ].copy()

    balance_o_prod_df.drop(
        columns=["ProductionID_clean"],
        inplace=True
    )

    print(
        f"✅ {component} | Balance rows: "
        f"{len(balance_o_prod_df)} / {len(o_prod_df)}"
    )
    print(f"✅ process complete for {component}")
    return balance_o_prod_df




for component in components:

    print(f"\n==============================")
    print(f"🚀 Processing component: {component}")
    print(f"==============================")

    # 1️⃣ Get balanced O_Production for this component
    balance_o_prod_df = get_balanced_o_production_for_component(
        component=component,
        i_material_df=i_mat_df,
        start_time=start_time,
        end_time=end_time
    )

    if balance_o_prod_df.empty:
        print(f"⚠️ No balance data for {component}, skipping...")
        continue

    # 2️⃣ Split vs Inventory
    found_df, not_found_df = split_balance_o_production_with_inventory(
        component=component,
        balance_o_prod_df=balance_o_prod_df,
        inventory_df=inv_df
    )

    # 3️⃣ Save files
    save_inventory_check_files(
        component=component,
        found_df=found_df,
        not_found_df=not_found_df,
        start_time=start_time,
        end_time=end_time,
        output_dir="/Users/ajaygour/Library/CloudStorage/OneDrive-ALGO8AIPRIVATELIMITED/inventory_checking_pipeline"
    )

    print(f"✅ Completed processing for {component}")


