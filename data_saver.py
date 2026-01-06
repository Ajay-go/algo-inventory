from datetime import datetime
import os
import pandas as pd

def save_inventory_check_files(
    component: str,
    found_df: pd.DataFrame,
    not_found_df: pd.DataFrame,
    start_time: str,
    end_time: str,
    output_dir: str = "output"
):
    """
    Saves two files:
    1. <component> found (<time>).csv
    2. <component> not found (<time>).csv
    """

    os.makedirs(output_dir, exist_ok=True)

    # -------------------------------
    # FORMAT TIME FOR FILE NAME
    # -------------------------------
    start_str = datetime.strptime(
        start_time, "%Y-%m-%d %H:%M:%S"
    ).strftime("%Y%m%d_%H%M")

    end_str = datetime.strptime(
        end_time, "%Y-%m-%d %H:%M:%S"
    ).strftime("%Y%m%d_%H%M")

    time_tag = f"{start_str}_to_{end_str}"

    # -------------------------------
    # FILE NAMES
    # -------------------------------
    found_file = (
        f"{component}_found_({time_tag}).csv"
    )

    not_found_file = (
        f"{component}_not_found_({time_tag}).csv"
    )

    # -------------------------------
    # SAVE FILES
    # -------------------------------
    if not found_df.empty:
        found_df.to_csv(
            os.path.join(output_dir, found_file),
            index=False
        )

    if not not_found_df.empty:
        not_found_df.to_csv(
            os.path.join(output_dir, not_found_file),
            index=False
        )

    print(f"📁 Files saved for {component}")
    print(f"   ✔ {found_file}")
    print(f"   ✔ {not_found_file}")
