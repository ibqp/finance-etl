from config import logger
from utils.data_manager import DataManager
from config.settings import DATA_CONFIG_PATH, CSV_FILES_DIR

log = logger.setup_logger()

def main():
    data_manager = DataManager(DATA_CONFIG_PATH, CSV_FILES_DIR)

    stm_df, sec_df = data_manager.process_csv_files()
    log.info(f"Processed stm data: {len(stm_df)} records")
    log.info(f"Processed sec data: {len(sec_df)} records")

if __name__ == "__main__":
    main()
