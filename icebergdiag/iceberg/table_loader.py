import codecs
import json
from typing import Dict, Any

from icebergdiag.iceberg.serializers import Compressor, NOOP_COMPRESSOR
from icebergdiag.metrics.table import Table
from pyiceberg.catalog.glue import GlueCatalog
from pyiceberg.exceptions import NoSuchTableError, NoSuchPropertyException, NoSuchIcebergTableError
from pyiceberg.io import InputStream, load_file_io, InputFile
from pyiceberg.table import Table as IcebergTable
from pyiceberg.table import TableMetadata
from pyiceberg.table.metadata import TableMetadataUtil

Properties = Dict[str, str]
PROP_GLUE_TABLE = "Table"
PROP_GLUE_TABLE_DATABASE_NAME = "DatabaseName"
PROP_GLUE_TABLE_PARAMETERS = "Parameters"
PROP_GLUE_TABLELIST = "TableList"
ICEBERG = "iceberg"
TABLE_TYPE = "table_type"
METADATA_LOCATION = "metadata_location"
PROP_GLUE_TABLE_NAME = "Name"


class TableLoader:
    def __init__(self, catalog: GlueCatalog):
        self.catalog = catalog

    def load_table(self, table: Table) -> IcebergTable:
        """Loads the table's metadata and returns the table instance.

        You can also use this method to check for table existence using 'try catalog.table() except TableNotFoundError'.
        Note: This method doesn't scan data stored in the table.

        Args:
            Table: Table identifier.

        Returns:
            Table: the table instance with its metadata.

        Raises:
            NoSuchTableError: If a table with the name does not exist, or the identifier is invalid.
        """
        database_name, table_name = table.database, table.table_name
        try:
            load_table_response = self.catalog.glue.get_table(DatabaseName=database_name, Name=table_name)
        except self.catalog.glue.exceptions.EntityNotFoundException as e:
            raise NoSuchTableError(f"Table does not exist: {database_name}.{table_name}") from e

        return self._convert_glue_to_iceberg(load_table_response.get(PROP_GLUE_TABLE, {}))

    def _convert_glue_to_iceberg(self, glue_table: Dict[str, Any]) -> IcebergTable:
        properties: Properties = glue_table.get(PROP_GLUE_TABLE_PARAMETERS, {})

        if TABLE_TYPE not in properties:
            raise NoSuchPropertyException(
                f"Property {TABLE_TYPE} missing, could not determine type: "
                f"{glue_table[PROP_GLUE_TABLE_DATABASE_NAME]}.{glue_table[PROP_GLUE_TABLE_NAME]}"
            )
        glue_table_type = properties[TABLE_TYPE]

        if glue_table_type.lower() != ICEBERG:
            raise NoSuchIcebergTableError(
                f"Property table_type is {glue_table_type}, expected {ICEBERG}: "
                f"{glue_table[PROP_GLUE_TABLE_DATABASE_NAME]}.{glue_table[PROP_GLUE_TABLE_NAME]}"
            )

        if METADATA_LOCATION not in properties:
            raise NoSuchPropertyException(
                f"Table property {METADATA_LOCATION} is missing, cannot find metadata for: "
                f"{glue_table[PROP_GLUE_TABLE_DATABASE_NAME]}.{glue_table[PROP_GLUE_TABLE_NAME]}"
            )
        metadata_location = properties[METADATA_LOCATION]

        io = load_file_io(properties=self.catalog.properties, location=metadata_location)
        file = io.new_input(metadata_location)
        metadata = TableLoader.get_table_data(file)
        return IcebergTable(
            identifier=(glue_table[PROP_GLUE_TABLE_DATABASE_NAME], glue_table[PROP_GLUE_TABLE_NAME]),
            metadata=metadata,
            metadata_location=metadata_location,
            io=self.catalog._load_file_io(metadata.properties, metadata_location),
            catalog=self.catalog,
        )

    @staticmethod
    def get_table_data(input_file: InputFile):
        with input_file.open() as input_stream:
            return TableLoader.table_metadata(input_stream, compression=Compressor.get_compressor(location=input_file.location))

    @staticmethod
    def table_metadata(
            byte_stream: InputStream, encoding: str = 'utf-8', compression: Compressor = NOOP_COMPRESSOR
    ) -> TableMetadata:
        """Instantiate a TableMetadata object from a byte stream.

        Args:
            byte_stream: A file-like byte stream object.
            encoding (default "utf-8"): The byte encoder to use for the reader.
            compression: Optional compression method
        """
        with compression.stream_decompressor(byte_stream) as decompressed_stream:
            reader = codecs.getreader(encoding)(decompressed_stream)
            json_content = reader.read()
        data = json.loads(json_content)
        return TableMetadataUtil.parse_obj(data)
