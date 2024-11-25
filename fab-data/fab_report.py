from pyiceberg.catalog.sql import SqlCatalog
import os
from pyiceberg.io.pyarrow import PyArrowFileIO
from dotenv import load_dotenv
import streamlit as st

load_dotenv()
warehouse_path = f"abfs://{os.getenv("AZURE_GOLD_CONTAINER_NAME")
                           }@{os.getenv("AZURE_STORAGE_ACCOUNT_NAME")}.dfs.core.windows.net/"
file_io = PyArrowFileIO(
    properties={
        "adls.connection-string":  os.getenv("AZURE_CONNECTION_STRING"),
        "adls.account-name":  os.getenv("AZURE_STORAGE_ACCOUNT_NAME"),
        "adls.account-key": os.getenv("AZURE_STORAGE_ACCOUNT_KEY"),
    }
)
catalog = SqlCatalog(
    name="test",
    identifier="default",
    file_io=file_io,
    **{
        "uri": "sqlite:///iceberg-gold.db",
        "warehouse": warehouse_path,
        "adls.connection-string": os.getenv("AZURE_CONNECTION_STRING"),
        "adls.account-name": os.getenv("AZURE_STORAGE_ACCOUNT_NAME"),
        "adls.account-key": os.getenv("AZURE_STORAGE_ACCOUNT_KEY"),
    },
)

table = catalog.load_table("gold.fab_report")

# Can't use .arrow() because it's not possible to pass a schema with not nullable fields
con = table.scan().to_duckdb(table_name="fabreport")
data = con.sql(
    """
        SELECT tool, process_step, avg_temp, min_defects, max_defects
        FROM fabreport order by tool desc
    """
).df()

st.title("Fabrication Report Dashboard")


st.subheader("Average Temperature by Process Step")
avg_temp_chart = data.groupby("process_step")["avg_temp"].mean().sort_values()
st.bar_chart(avg_temp_chart)

st.subheader("Defect Range by Tool")
defect_chart = data.groupby("tool")[["min_defects", "max_defects"]].mean()
st.line_chart(defect_chart)
