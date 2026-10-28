Minimal stand-ins for pyarrow and boto3, used by tests/test_convert_e2e.py ONLY when the real packages are not installed
(e.g. a sandbox without internet). "Parquet" files written through them are pickled DataFrames. Never put this folder on the path in the workflows.
