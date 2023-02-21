from pathlib import Path
from pyarrow import fs
from pprint import pprint

# local FS
local = fs.LocalFileSystem() # use mmap param set to False by default
f, path = fs.FileSystem.from_uri(Path.cwd()) # from_uri to pass standard window paths

pprint(f"file_obj={local}")
pprint(f"file_obj={f} {path=}")

# Credentials can be passed directly through S3FileSystem or picked up implicitly
# through env vars like AWS_ACCESS_KEY_ID or ~/.aws/config file
s3_explicit = fs.S3FileSystem(region="us-east-1") # explicit create
s3_implicit, path = fs.FileSystem.from_uri("s3://my-bucket/") # implicit

pprint(f"s3_obj={s3_explicit}")
pprint(f"s3_obj={s3_implicit} {path=}")

# Working with CSV files in pyarrow
import pyarrow as pa
import pyarrow.csv # import csv submodule

csv_path = Path.cwd().parent.parent / Path("sample_data/train.csv")
table = pa.csv.read_csv(csv_path)

pprint(table.schema)

pprint(table.column(0).num_chunks)
# Displays all chunks
# pprint(table.column(0).chunks)

# Working with JSON files in pyarrow
import pyarrow.json # import json submodule
import pandas as pd

# Sample data example
data = [
    {"a": max(1, x * 2 - 1), "b": float(x), "c" : x} for x in range(1, 5)
]
df = pd.DataFrame.from_records(data)
json_path = Path("/tmp/tmp.json")

with open(json_path, "w") as f:
    f.write(df.to_json(orient="records", lines=True))

data_table = pa.json.read_json(json_path)
pprint(data_table.to_pydict())

# Using training data as example, but first have training data in json format
with open(json_path, "w") as f:
    f.write(table.to_pandas().to_json(orient="records", lines=True))

table = pa.json.read_json(json_path)
# Very large output if you decide to print out the complete table
# pprint(table.to_pydict())

# Working with ORC files in pyarrow
import pyarrow.orc # import orc submodule

orc_path = Path.cwd().parent.parent / Path("sample_data/train.orc")
of = pa.orc.ORCFile(orc_path)

pprint(of.nrows)
pprint(of.schema)

# columns can be None to read all columns
table = of.read(columns=["vendor_id", "passenger_count", "rate_code_id"])
pprint(table)

# Working with parquet files in pyarrow
import pyarrow.parquet # import parquet submodule

parquet_path = Path.cwd().parent.parent / Path("sample_data/train.parquet")
table = pa.parquet.read_table(parquet_path)

pprint(table)
pprint(table.schema)