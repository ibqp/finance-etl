from config import logger
from typing import Any, Dict
from sqlalchemy import Column, inspect
from sqlalchemy.types import TypeEngine
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.schema import CreateSchema
from utils.db_manager import DatabaseManager
from sqlalchemy.types import Integer, String, Date, Numeric, TIMESTAMP
from config.settings import DATABASE_URL, DB_CONFIG_PATH


log = logger.setup_logger()


class Base(DeclarativeBase):
    pass

class TableModelBuilder:
    # Helper class to build SQLAlchemy table models from config.
    TYPE_MAPPING = {
        'Integer': Integer
        , 'String': String
        , 'Date': Date
        , 'Decimal': Numeric
        , 'Timestamp': TIMESTAMP
    }

    @classmethod
    def get_sql_type(cls, field_config: dict) -> TypeEngine:
        """ Function is responsible for converting data type definitions
            from config into actual SQLAlchemy type objects that
            can be used in database column definitions. """

        field_type = field_config['type']

        if field_type == 'String':
            return String(field_config.get('length', 255))
        if field_type == 'Decimal':
            return Numeric(precision=field_config.get('precision', 10), scale=field_config.get('scale', 2))
        if field_type == 'Timestamp':
            return TIMESTAMP(timezone=field_config.get('timezone', False))

        return cls.TYPE_MAPPING[field_type]

    @classmethod
    def build_model(cls, schema:str, table_name: str, table_fields: Dict[str, Any]):
        """ Build SQLAlchemy Model class from table configuration. """

        table = {'__tablename__': table_name, '__table_args__': {'schema': schema}}

        for field_name, field_config in table_fields.items():
            table[field_name] = Column(
                cls.get_sql_type(field_config)
                , primary_key=field_config.get('primary_key', False)
                , nullable=field_config.get('nullable', True)
            )

        return type(table_name, (Base,), table)

class DatabaseInitializer:
    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager
        self.tables = self.db_manager.config['tables']

    def initialize(self):
        """ Initializes database schema and tables.
            NB! It will drop existing tables and recreate them! """

        try:
            log.info("DatabaseManager: Starting database initialization")

            # Create schema if not exists
            inspector = inspect(self.db_manager.engine)
            schema_exists = inspector.has_schema(self.db_manager.schema_name)

            if not schema_exists:
                with self.db_manager.session_scope() as session:
                    session.execute(CreateSchema(self.db_manager.schema_name))
                    log.info(f"DatabaseManager: Created schema: {self.db_manager.schema_name}")
            else:
                log.info(f"DatabaseManager: Schema '{self.db_manager.schema_name}' already exists")

            # Create table models
            for _, table_config in self.tables.items():
                table_name = table_config['table_name']
                table_fields = table_config['fields']
                TableModelBuilder.build_model(self.db_manager.schema_name, table_name, table_fields)
                log.info(f"DatabaseManager: Created model for table {table_name}")

            # Drop existing tables if they exist (in database)
            Base.metadata.drop_all(self.db_manager.engine)
            log.info("DatabaseManager: Dropped existing tables")

            # Create all tables in database (in database)
            Base.metadata.create_all(self.db_manager.engine)
            log.info("DatabaseManager: Successfully created all tables")
        except Exception as e:
            log.error(f"DatabaseManager: Database initialization failed: {e}", exc_info=True)
            raise

def main():
    db_manager = DatabaseManager(DATABASE_URL, DB_CONFIG_PATH)
    initializer = DatabaseInitializer(db_manager)
    initializer.initialize()

if __name__ == "__main__":
    main()
