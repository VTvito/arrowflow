import pyarrow as pa
import pyarrow.ipc as pa_ipc


def ipc_to_table(ipc_data: bytes) -> pa.Table:
    """
    Deserialize bytes of Arrow IPC in Arrow Table.
    """
    try:
        reader = pa_ipc.open_stream(pa.BufferReader(ipc_data))
        return reader.read_all()
    except Exception as e:
        raise ValueError(f"Failed to parse Arrow IPC data: {e}") from e

def table_to_ipc(table: pa.Table) -> bytes:
    """
    Serialize Arrow Table in Arrow IPC (stream).
    """
    try:
        sink = pa.BufferOutputStream()
        with pa_ipc.new_stream(sink, table.schema) as writer:
            writer.write_table(table)
        return sink.getvalue().to_pybytes()
    except Exception as e:
        raise ValueError(f"Failed to serialize Arrow Table to IPC: {e}") from e
