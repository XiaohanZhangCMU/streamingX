# Copyright 2023 MosaicML Streaming authors
# SPDX-License-Identifier: Apache-2.0

import json
import os
import shutil
from decimal import Decimal
from tempfile import mkdtemp
from typing import Any, Tuple, List, Dict

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import DecimalType, IntegerType, StringType, StructField, StructType

from streaming import delta
import glob
import time
import pandas as pd
import random

pq = '0000_one_piece/part-00000-c3c4d5fb-726a-4f1e-8d38-8e7b44d71475-c000.snappy.parquet'
#pq = '0000/part-00000-804c197a-4c3a-46b3-a29e-bb458043e861-c000.snappy.parquet'
#pq = 'lz4/spark_generated.lz4.parquet'

#tik = time.time()
#pq = '../resources/'+pq
#result = delta.read_one_v2(0, pq)
#tok = time.time()
#print(f'elapsed time: {tok - tik}')

pq = '../resources/'+pq

df = pd.read_parquet(pq)
print(df.head())
print(df.columns)

col_indx = 2
rg_indx = 1
pg_indx = 1
to_skip = 4

tik = time.time()
result = delta.read_one_v3(rg_indx, col_indx, pg_indx, to_skip, pq)
#result = delta.read_one(1000, pq)
##result = delta.read_one_v2(1000, pq)
tok = time.time()
print(f'Elapsed Python time: {tok - tik}')
print(result)



#tik = time.time()
#pq = '../resources/'+pq
#result = delta.read_one(100000, pq)
#tok = time.time()
#print(f'elapsed time: {tok - tik}')
#

