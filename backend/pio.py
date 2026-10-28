"""Parquet read/write helpers (real pyarrow in production; the tests/stubs stand-ins only in the offline tests)."""
import io
import os
import tempfile

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

STUB = not hasattr(pq, "read_table")           # the offline test stand-ins have no read_table


def read_bytes(body, columns=None):
    if STUB:
        df = pd.read_pickle(io.BytesIO(body))
    else:
        df = pd.read_parquet(io.BytesIO(body), columns=columns)
    return df[columns] if columns else df


def read_path(path, columns=None):
    with open(path, "rb") as f:
        return read_bytes(f.read(), columns)


def write_path(df, path, row_group_size=5000, schema=None):
    if schema is not None:
        table = pa.Table.from_pandas(df, schema=schema, preserve_index=False)
    else:
        table = pa.Table.from_pandas(df, preserve_index=False)
    pq.write_table(table, path, compression="snappy", row_group_size=row_group_size)


def to_bytes(df, row_group_size=5000):
    fd, p = tempfile.mkstemp(suffix=".parquet")
    os.close(fd)
    try:
        write_path(df, p, row_group_size)
        with open(p, "rb") as f:
            return f.read()
    finally:
        os.remove(p)
