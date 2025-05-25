from config import logger
from utils.data_manager import DataManager
from utils.db_manager import DatabaseManager
from config.settings import DATA_CONFIG_PATH, CSV_FILES_DIR, DATABASE_URL, DB_CONFIG_PATH

log = logger.setup_logger()

def main():
    # Prepare data manager
    data_manager = DataManager(CSV_FILES_DIR, DATA_CONFIG_PATH)

    # Return ready to upload DataFrames from raw data files
    stm_df, sec_df = data_manager.process_csv_files()
    log.info(f"Processed stm data: {len(stm_df)} records")
    log.info(f"Processed sec data: {len(sec_df)} records")


    # Prepare database manager
    db_manager = DatabaseManager(DATABASE_URL, DB_CONFIG_PATH)

    # Get existing surrogate keys from database
    stm_keys, sec_keys = db_manager.get_existing_surrogate_keys()

    # Get only new records from DataFrames to be uploaded to database
    stm_new = data_manager.get_new_records(stm_df, stm_keys, df_name="stm_df")
    sec_new = data_manager.get_new_records(sec_df, sec_keys, df_name="sec_df")

    # Upload new records to database
    db_manager.upload_new_records(stm_new, sec_new)

if __name__ == "__main__":
    main()
