import logging

import pyarrow as pa
import pyarrow.ipc as pa_ipc

logger = logging.getLogger(__name__)

def ipc_to_table(ipc_data: bytes) -> pa.Table:
    """
    Deserialize bytes of Arrow IPC in Arrow Table.
    """
    try:
        reader = pa_ipc.open_stream(pa.BufferReader(ipc_data))
        table = reader.read_all()
        logger.info(f"Deserialized Arrow Table with {table.num_rows} rows and {table.num_columns} columns.")
        return table
    except Exception as e:
        logger.error(f"Failed to parse Arrow IPC data: {e}")
        raise

def table_to_ipc(table: pa.Table) -> bytes:
    """
    Serialize Arrow Table in Arrow IPC (stream).
    """
    try:
        sink = pa.BufferOutputStream()
        with pa_ipc.new_stream(sink, table.schema) as writer:
            writer.write_table(table)
        ipc_bytes = sink.getvalue().to_pybytes()
        logger.info(f"Serialized Arrow Table to IPC format, {len(ipc_bytes)} bytes.")
        return ipc_bytes
    except Exception as e:
        logger.error(f"Failed to serialize Arrow Table to IPC format: {e}")
        raise
