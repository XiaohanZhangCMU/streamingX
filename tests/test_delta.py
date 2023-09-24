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

n_records = 500


class TestDelta:

    @pytest.fixture
    def test_data(self) -> str:
        yield 'tests/resources/0000_one_piece/'
        #yield 'tests/resources/0000/'
        #yield 'tests/resources/lz4/'
        #yield 'tests/resources/lion_w_page/'

    @pytest.fixture
    def offsets(self, test_data) -> Dict:
        sample_ids = {}
        for pq in glob.glob(test_data + '/*'):
            if not pq.endswith('.parquet'):
                continue
            df = pd.read_parquet(pq)
            l = [random.randint(0, len(df)-1) for _ in range(n_records)]
            # always test with first element and last element included
            l.insert(0,0)
            l.append(len(df)-1)
            sample_ids[pq] = sorted(list(set(l)))

        #print(len(sample_ids), sample_ids)
        yield sample_ids


    @pytest.mark.parametrize('delta_or_pandas', ['delta', ]) # 'pandas'])
    def test_time_single(self, offsets: Dict, delta_or_pandas: str):
        print('test read single record a time - ' + delta_or_pandas)
        tik = time.time()
        ans = []
        for pq, sample_ids in offsets.items():
            for offset in sample_ids:
                if delta_or_pandas == 'delta':
                    result = delta.read_one(offset, pq)
                    ans.append(result[0].to_pydict())
                else:
                    df = pd.read_parquet(pq)
                    result = df.iloc[offset, :]
                    ans.append(result.to_dict())
        tok = time.time()
        df = pd.DataFrame.from_dict(ans)
        print(f'elapsed time: {tok - tik}')
        print(f'num of retrived records: {len(df)}')

    @pytest.mark.parametrize('delta_or_pandas', ['delta', 'pandas'])
    def test_time_rust_batch(self, offsets: Dict, delta_or_pandas: str):
        print('test read batch records a time - ' + delta_or_pandas )
        tik = time.time()
        ans = []
        for pq, sample_ids in offsets.items():
            if delta_or_pandas == 'delta':
                result = delta.read_batch(sample_ids, pq)
                ans.append(pd.DataFrame.from_dict(result[0].to_pydict()))
            else:
                df = pd.read_parquet(pq)
                result = df.iloc[sample_ids, :]
                ans.append(result)
        tok = time.time()
        df = pd.concat(ans)
        print(f'elapsed time: {tok - tik}')
        print(f'num of retrived records: {len(df)}')

