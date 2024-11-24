from pyiceberg.schema import Schema
from pyiceberg.types import (
    TimestampType, DoubleType, StringType,
    LongType, NestedField
)
import pyarrow as pa

ICEBERG_SCHEMA = Schema(
    NestedField(1, "timestamp", TimestampType(), required=False),
    NestedField(2, "wafer_id", StringType(), required=True),
    NestedField(3, "process_step", StringType(), required=False),
    NestedField(4, "tool", StringType(), required=False),
    NestedField(5, "temperature_c", DoubleType(), required=False),
    NestedField(6, "pressure_pa", DoubleType(), required=False),
    NestedField(7, "chemical_used", StringType(), required=False),
    NestedField(8, "defects_detected", LongType(), required=False),
    NestedField(9, "operator_id", LongType(), required=False),
    NestedField(10, "batch_id", StringType(), required=False),
    NestedField(11, "source_file", StringType(), required=True),
    NestedField(12, "processed_at", TimestampType(), required=True)
)


def get_pyarrow_schema() -> pa.Schema:
    return pa.schema([
        pa.field('timestamp', pa.timestamp('us'), nullable=True),
        pa.field('wafer_id', pa.string(), nullable=False),
        pa.field('process_step', pa.string(), nullable=True),
        pa.field('tool', pa.string(), nullable=True),
        pa.field('temperature_c', pa.float64(), nullable=True),
        pa.field('pressure_pa', pa.float64(), nullable=True),
        pa.field('chemical_used', pa.string(), nullable=True),
        pa.field('defects_detected', pa.int64(), nullable=True),
        pa.field('operator_id', pa.int64(), nullable=True),
        pa.field('batch_id', pa.string(), nullable=True),
        pa.field('source_file', pa.string(), nullable=False),
        pa.field('processed_at', pa.timestamp('us'), nullable=False)
    ])
