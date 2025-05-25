import logging
import pandas as pd
from typing import Tuple
from sqlalchemy import create_engine
from contextlib import contextmanager
from sqlalchemy.orm import sessionmaker
from config.settings import config_loader
from sqlalchemy.exc import SQLAlchemyError


class DatabaseManager:
    def __init__(self, connection_string: str, db_config_path: str):
        logging.info("Starting to initialize DatabaseManager...")
        self.config = config_loader(db_config_path)
        self.schema_name = self.config['schema']
        self.stm_table_name = self.config['tables']['stm']['table_name']
        self.sec_table_name = self.config['tables']['sec']['table_name']
        self.engine = create_engine(connection_string)
        self.Session = sessionmaker(bind=self.engine)
        if not self.test_connection():
            raise ConnectionError("Database connection failed")
        logging.info("DatabaseManager initialized!")

    @contextmanager
    def session_scope(self):
        session = self.Session()
        try:
            logging.info("DatabaseManager: Opening database session...")
            yield session
            session.commit()
        except SQLAlchemyError as e:
            session.rollback()
            logging.error(f"DatabaseManager: Database error: {e}", exc_info=True)
            raise
        except Exception as e:
            session.rollback()
            logging.error(f"DatabaseManager:Unexpected error: {e}")
            raise
        finally:
            session.close()
            logging.info("DatabaseManager: Database session closed!")

    def test_connection(self) -> bool:
        try:
            connection = self.engine.connect()
            connection.close()
            logging.info("DatabaseManager: Connection test successful")
            return True
        except SQLAlchemyError as e:
            logging.error(f"DatabaseManager: Connection test failed: {e}")
            return False

    def get_existing_surrogate_keys(self) -> Tuple[pd.DataFrame, pd.DataFrame]:
        logging.info("DatabaseManager: Extracting existing surrogate keys from both tables...")

        stm_keys_query = f'select surrogate_key from {self.schema_name}.{self.stm_table_name};'
        sec_keys_query = f'select surrogate_key from {self.schema_name}.{self.sec_table_name};'

        stm_existing_keys = self._select_data(stm_keys_query)
        sec_existing_keys = self._select_data(sec_keys_query)

        return stm_existing_keys, sec_existing_keys

    def upload_new_records(self, stm_new: pd.DataFrame, sec_new: pd.DataFrame):
        if not stm_new.empty:
            self._insert_data(stm_new, self.stm_table_name, self.schema_name)
        else:
            logging.info("DatabaseManager: No new records to upload for stm")

        if not sec_new.empty:
            self._insert_data(sec_new, self.sec_table_name, self.schema_name)
        else:
            logging.info("DatabaseManager: No new records to upload for sec")

    def _select_data(self, query: str) -> pd.DataFrame:
        try:
            logging.info("DatabaseManager: Running SQL query...")
            df = pd.read_sql_query(query, self.engine)
            logging.info(f"DatabaseManager: Queried {df.shape[0]} rows!")
            return df
        except Exception as e:
            logging.error(f"DatabaseManager: Error selecting data: {e}")
            return pd.DataFrame()

    def _insert_data(self, df: pd.DataFrame, table_name: str, schema: str):
        try:
            logging.info("DatabaseManager: Inserting data to database...")
            df.to_sql(table_name, self.engine, schema=schema, if_exists='append', index=False)
            logging.info(f"DatabaseManager: Data inserted successfully! Rows inserted: {df.shape[0]}")
        except Exception as e:
            logging.error(f"DatabaseManager: Error inserting data: {e}")
