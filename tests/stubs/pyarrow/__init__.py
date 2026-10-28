import pandas as pd
class _T:
    def __init__(self, df): self.df = df
class Table:
    @staticmethod
    def from_pandas(df, schema=None, preserve_index=False): return _T(df.reset_index(drop=True))
def schema(fields): return fields
string = lambda: "string"; float64 = lambda: "float64"; int64 = lambda: "int64"
