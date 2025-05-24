import os
import re
import pytz
import logging
import hashlib
import pandas as pd
from datetime import datetime
from typing import Dict, Tuple, Optional, Any
from config.settings import config_loader, csv_files_loader

class FileProcessor:
    def __init__(self, csv_file_path:str, csv_file_name:str, bank:str, acc_type:str, mapping_type:str, file_specific_config:Dict[str, Any]):
        self.csv_file_path = csv_file_path
        self.csv_file_name = csv_file_name
        self.bank = bank
        self.acc_type = acc_type
        self.mapping_type = mapping_type
        self.file_specific_config = file_specific_config

    def process_file(self) -> pd.DataFrame:
        # Read CSV file
        df = self._read_csv()
        if df.empty:
            return df

        # Select and rename columns
        df = self._select_and_rename_columns(df)
        if df.empty:
            return df

        # Transform data and return result
        return self._transform_data(df)

    def _read_csv(self) -> pd.DataFrame:
        try:
            df = pd.read_csv(
                self.csv_file_path
                , sep=self.file_specific_config['csv_separator']
                , encoding='utf-8'
            )
            return df
        except Exception as e:
            logging.error(f"Error reading CSV file: {self.csv_file_path}. Error: {e}")
            return pd.DataFrame()

    def _select_and_rename_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        try:
            # Select columns
            columns_to_select = self.file_specific_config['original_fields'].keys()
            df = df.loc[:,columns_to_select]

            # Rename columns
            df = df.rename(columns=self.file_specific_config['original_fields'])
            return df
        except Exception as e:
            logging.error(f"Error selecting and renaming fields in file: {self.csv_file_path}. Error: {e}")
            return pd.DataFrame()

    def _transform_data(self, df: pd.DataFrame) -> pd.DataFrame:
        try:
            # Add surrogate key
            df['concat_key'] = df[self.file_specific_config['surrogate_key_columns']].astype(str).agg('#'.join, axis=1)
            df['surrogate_key'] = df['concat_key'].apply(lambda x: hashlib.md5(x.encode()).hexdigest())

            # Add common fields for any file
            df = df.assign(bank_name=self.bank, acc_type=self.acc_type, file_name=self.csv_file_name, processed_at=datetime.now(pytz.utc))

            # Do file specific transformations
            if self.mapping_type == 'stm':
                df = df.assign(
                    acc_name=lambda x: x['acc_number'].map(self.file_specific_config['accounts'])
                    , dt=lambda x: pd.to_datetime(x['dt'], format=self.file_specific_config['date_format'])
                    , year=lambda x: x['dt'].dt.year
                    , ym=lambda x: x['dt'].dt.strftime('%Y-%m')
                    , sum_tmp=lambda x: pd.to_numeric(x['sum'].astype(str).str.replace(',', '.', regex=False),errors='coerce')
                    , sum=lambda x: x['sum_tmp'] * x['dc'].map(self.file_specific_config['debit_multiplier'])
                )
            elif self.mapping_type == 'sec':
                df = df.assign(
                    send_dt=lambda x: pd.to_datetime(x['send_dt'], format=self.file_specific_config['date_format'])
                    , effect_dt=lambda x: pd.to_datetime(x['effect_dt'], format=self.file_specific_config['date_format'])
                    , effect_year=lambda x: x['effect_dt'].dt.year
                    , effect_ym=lambda x: x['effect_dt'].dt.strftime('%Y-%m')
                )
            else:
                logging.error(f"Unsupported mapping type: {self.mapping_type}")
                return pd.DataFrame()

            # Place columns in desired order
            df = df.loc[:,self.file_specific_config['desired_fields']]

            return df
        except Exception as e:
            logging.error(f"Error transforming data in file: {self.csv_file_path}. Error: {e}")
            return pd.DataFrame()

class DataManager:
    def __init__(self, data_config_path:str, csv_files_dir:str):
        self.data_config = config_loader(data_config_path)
        self.csv_files_paths = csv_files_loader(csv_files_dir)
        self.ready_data: Dict[str, pd.DataFrame] = {'stm': pd.DataFrame(), 'sec': pd.DataFrame()}
        logging.info("DataManager initialized!")

    def process_csv_files(self) -> Tuple[pd.DataFrame, pd.DataFrame]:
        # Get mandatory configs for CSV files transformations.
        # If they are missing, raise an error. Nothing to transform without configs.
        csv_file_pattern = self.data_config.get('file_pattern') # files must have specific naming conventions
        csv_mapping_config = self.data_config.get('mapping') # after we will extract distinct mapping based on file_pattern
        if not csv_file_pattern or not csv_mapping_config:
            raise ValueError("Missing mandatory configuration parameters!")

        # Process each CSV file
        for csv_file_path in self.csv_files_paths:
            # Get filename
            csv_file_name = os.path.basename(csv_file_path)
            logging.info(f"Processing file: {csv_file_name}")

            # Get file metadata parts.
            # If None - continue to the next file (unlucky with this one)
            groups = self._extract_file_metadata_groups(csv_file_pattern, csv_file_name)
            if not groups:
                continue
            bank, acc_type, mapping_type = groups

            # Now using file metadata parts we can extract distinct configuration for this specific file
            # If None - continue to next file (unlucky with this file)
            file_specific_config = self._extract_file_specific_config(csv_mapping_config, mapping_type, bank)
            if not file_specific_config:
                continue

            # Setup a file processor based on file metadata groups and file_specific_config
            processor = FileProcessor(csv_file_path, csv_file_name, bank, acc_type, mapping_type, file_specific_config)
            processed_df = processor.process_file()

            # Only append non-empty results
            if not processed_df.empty:
                self.ready_data[mapping_type] = pd.concat([self.ready_data[mapping_type], processed_df])
                logging.info(f"Added to ready '{mapping_type}' dataframe: {len(processed_df)} records. Total: {len(self.ready_data[mapping_type])} records.")

        return self.ready_data['stm'], self.ready_data['sec']

    def _extract_file_metadata_groups(self, csv_file_pattern: str, csv_file_name: str) -> Optional[Tuple[str, str, str]]:
        # Match file name against pattern
        # Check if file name matches the pattern
        csv_file_metadata = re.match(csv_file_pattern, csv_file_name)
        if not csv_file_metadata:
            logging.error("File does not match expected pattern. File cannot be processed.")
            return None

        # Extract file metadata parts
        # Check if we have exactly 3 groups (we need bank_name, account_type and mapping_type)
        groups = csv_file_metadata.groups()
        if len(groups) != 3:
            logging.error(f"Expected 3 capturing groups in pattern, but got {len(groups)}. File cannot be processed.")
            return None

        # Return file metadata groups
        return groups

    def _extract_file_specific_config(self, csv_mapping_config: Dict[str, Any], mapping_type:str, bank:str) -> Optional[Dict[str, Any]]:
        # Check if extracted mapping_type/bank exists in config
        # Return None if not found
        if mapping_type not in csv_mapping_config or bank not in csv_mapping_config[mapping_type]:
            logging.error(f"mapping_type: {mapping_type} or bank: {bank} does not exist in csv_mapping_config. File cannot be processed.")
            return None

        # Return file specific config based on its maping type and bank
        file_specific_config = csv_mapping_config[mapping_type][bank]
        return file_specific_config
