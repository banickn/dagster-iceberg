import os
from dagster import ConfigurableResource
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.io.pyarrow import PyArrowFileIO


class IcebergCatalogResource(ConfigurableResource):
    """Resource for managing Iceberg catalog connections."""
    connection_string: str = os.getenv("AZURE_CONNECTION_STRING")
    account_name: str = os.getenv("AZURE_STORAGE_ACCOUNT_NAME")
    account_key: str = os.getenv("AZURE_STORAGE_ACCOUNT_KEY")
    silver_container: str = os.getenv("AZURE_SILVER_CONTAINER_NAME")

    def get_catalog(self) -> SqlCatalog:
        """Initialize and configure the Iceberg catalog."""
        file_io = PyArrowFileIO(
            properties={
                "adls.connection-string": self.connection_string,
                "adls.account-name": self.account_name,
                "adls.account-key": self.account_key,
            }
        )

        warehouse_path = f"abfs://{self.silver_container}@{self.account_name}.dfs.core.windows.net/"

        return SqlCatalog(
            name="test",
            identifier="default",
            file_io=file_io,
            **{
                "uri": "sqlite:///iceberg_catalog.db",
                "warehouse": warehouse_path,
                "adls.connection-string": self.connection_string,
                "adls.account-name": self.account_name,
                "adls.account-key": self.account_key,
            }
        )
