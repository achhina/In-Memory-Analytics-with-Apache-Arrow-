import pyarrow as pa
import numpy as np
from pprint import pprint

COMPONENTS = 3
RECORDS = 5

records = [
    {
    "id": np.random.randint(0, RECORDS),
    "cost_components": [
        round(abs(np.random.rand()) * 100, 2) for _ in range(COMPONENTS)
    ],
    }
    for _ in range(RECORDS)
]

for record in records:
    record["cost"] = round(sum(record["cost_components"]), 2)

pprint(records)

# Take a (row-wise) list of objects and convert them to a column-oriented recrod
# batch
pa_type = pa.struct([
    ("id", pa.int64()),
    ("cost", pa.float64()),
    ("cost_components", pa.list_(pa.float64()))
])

pa_records = pa.array(records, type=pa_type)
rb = pa.RecordBatch.from_arrays(pa_records.flatten(), ["id", "cost", "cost_components"])
pprint(rb)

# Convert record back back into row-oriented list
pylist = rb.to_pylist()
pprint(pylist)