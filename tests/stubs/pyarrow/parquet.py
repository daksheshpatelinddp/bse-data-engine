import pandas as pd
_GROUPS = {}
def write_table(t, path, compression=None, row_group_size=None):
    t.df.to_pickle(path); _GROUPS[path] = max(1, -(-len(t.df) // (row_group_size or len(t.df) or 1)))
class ParquetFile:
    def __init__(self, path): self.path = path
    @property
    def num_row_groups(self): return _GROUPS.get(self.path, 1)
class ParquetWriter:
    def __init__(self, path, schema, compression=None): self.path, self.parts = path, []
    def __enter__(self): return self
    def __exit__(self, *a): pd.concat(self.parts, ignore_index=True).to_pickle(self.path); _GROUPS[self.path] = len(self.parts)
    def write_table(self, t, row_group_size=None): self.parts.append(t.df)
