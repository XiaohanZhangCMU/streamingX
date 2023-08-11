# Copyright 2023 MosaicML Streaming authors
# SPDX-License-Identifier: Apache-2.0

"""A utility to convert databricks' tables to MDS."""

import json
import os
import shutil
import urllib.parse
import uuid
from argparse import ArgumentParser, Namespace
from collections.abc import Iterable
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple, Union

import pandas as pd
from pyspark import TaskContext
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.types import StringType, StructField, StructType

from streaming import MDSWriter

default_mds_kwargs = {
    'compression': 'zstd:7',
    'hashes': ['sha1', 'xxh64'],
    'size_limit': 1 << 27,
    'progress_bar': 1,
    'columns': {
        'tokens': 'bytes'
    },
    'keep_local': False
}

default_udf_kwargs = {
    'concat_tokens': 2048,
    'tokenizer': 'EleutherAI/gpt-neox-20b',
    'eos_text': '<|endoftext|>',
    'compression': 'zstd',
    'split': 'train',
    'no_wrap': False,
    'bos_text': '',
    'key': 'content',
}


def is_iterable(obj):
    """Check if obj is iterable."""
    return issubclass(type(obj), Iterable)


def do_merge_index(partitions, mds_out, skip=False):
    if not partitions or skip:
        return

    shards = []

    for row in partitions:
        mds_partition_index = f'{row.mds_path}/index.json'
        mds_partition_basename = os.path.basename(row.mds_path)
        obj = json.load(open(mds_partition_index))
        for i in range(len(obj['shards'])):
            shard = obj['shards'][i]
            for key in ['raw_data', 'zip_data']:
                if shard.get(key):
                    basename = shard[key]['basename']
                    obj['shards'][i][key]['basename'] = os.path.join(mds_partition_basename,
                                                                     basename)
        shards += obj['shards']

    obj = {
        'version': 2,
        'shards': shards,
    }

    mds_index = os.path.join(mds_out, 'index.json')

    with open(mds_index, 'w') as out:
        json.dump(obj, out)


def dataframeToMDS(dataframe: DataFrame,
                   out: Union[str, Tuple[str, str]],
                   columns: Dict[str, str],
                   partition_size: int = -1,
                   merge_index: bool = True,
                   sample_ratio: float = -1.0,
                   keep_local: bool = False,
                   compression: Optional[str] = None,
                   hashes: Optional[List[str]] = None,
                   size_limit: Optional[Union[int, str]] = 1 << 26,
                   udf_iterable: Callable = None,
                   udf_kwargs: Dict = None):
    """Execute a spark dataframe to MDS conversion process.

    This method orchestrates the conversion of a spark dataframe into MDS format by
    processing the input data, applying a user-defined iterable function if
    provided, and writing the results to MDS-compatible format. The converted data is saved to mds_path.

    Args:
        dataframe (pyspark.sql.DataFrame or None): A DataFrame containing Delta Lake data.
        out (str | Tuple[str, str]): Output dataset directory to save shard files.
            1. If ``out`` is a local directory, shard files are saved locally.
            2. If ``out`` is a remote directory, a local temporary directory is created to
               cache the shard files and then the shard files are uploaded to a remote
               location. At the end, the temp directory is deleted once shards are uploaded.
            3. If ``out`` is a tuple of ``(local_dir, remote_dir)``, shard files are saved in the
               `local_dir` and also uploaded to a remote location.
        partition_size (int): The number of partitions to use during conversion. Default is -1, which does not do repartition.
        merge_index (bool): Whether to merge MDS index files. Default is True.
        sample_ratio (float): The fraction of data to randomly sample during conversion.
            Should be in the range (0, 1). Default is -1.0 (no sampling).
        columns (Dict[str, str]): Sample columns.
        keep_local (bool): If the dataset is uploaded, whether to keep the local dataset directory
            or remove it after uploading. Defaults to ``False``.
        compression (str, optional): Optional compression or compression:level. Defaults to
            ``None``.
        hashes (List[str], optional): Optional list of hash algorithms to apply to shard files.
            Defaults to ``None``.
        size_limit (Union[int, str], optional): Optional shard size limit, after which point to start a new
            shard. If ``None``, puts everything in one shard. Can specify bytes
            human-readable format as well, for example ``"100kb"`` for 100 kilobyte
            (100*1024) and so on. Defaults to ``1 << 26``
        udf_iterable (Callable or None): A user-defined function that returns an iterable over the dataframe. ppfn_kwargs is the k-v args for the method. Default is None.
        udf_kwargs (Dict): Additional keyword arguments to pass to the pandas processing
            function if provided. Default is an empty dictionary.

    Returns:
        None

    Raises:
        ValueError: If dataframe is not provided

    Note:
        - The method creates a SparkSession if not already available.
        - If 'sample_ratio' is provided, the input data will be randomly sampled.
        - The 'ppfn_kwargs' dictionaries can be used to pass additional
          keyword arguments to the udf_iterable.
    """

    def write_mds(iterator):

        id = TaskContext.get().taskAttemptId()
        out_file_path = os.path.join(out, f'{id}')
        mds_kwargs = {
            'out': out_file_path,
            'columns': columns,
            'keep_local': keep_local,
            'compression': compression,
            'hashes': hashes,
            'size_limit': size_limit
        }
        for k, v in mds_kwargs.items():
            if v is None:
                del mds_kwargs[k]

        with MDSWriter(**mds_kwargs) as mds_writer:
            for pdf in iterator:
                if udf_iterable is not None:
                    d = udf_iterable(pdf, **udf_kwargs)
                else:
                    d = pdf.to_dict('records')
                assert is_iterable(
                    d), f'pandas_processing_fn needs to return an iterable instead of a {type(d)}'

                for row in d:
                    mds_writer.write(row)
        yield pd.DataFrame(pd.Series([out_file_path], name='mds_path'))

    import pyspark
    spark = pyspark.sql.SparkSession.builder.getOrCreate()

    if not dataframe:
        raise ValueError(f'input dataframe is none!')

    df = dataframe

    if 0 < sample_ratio < 1:
        df = dataframe.sample(sample_ratio)

    if partition_size > 0:
        df = df.repartition(partition_size)

    if urllib.parse.urlparse(out).scheme == '' and os.path.exists(out) and len(
            os.listdir(out)) != 0:
        raise ValueError(
            'Looks like {out} is local folder and it is not empty. MDSwriter needs an empty local folder to proceed.'
        )
        return

    # Prepare partition schema
    result_schema = StructType([StructField('mds_path', StringType(), False)])
    partitions = df.mapInPandas(func=write_mds, schema=result_schema).collect()

    do_merge_index(partitions, out, skip=not merge_index)


if __name__ == '__main__':

    def parse_args():
        """Parse commandline arguments."""
        parser = ArgumentParser(
            description=
            'Convert dataset into MDS format. Running from command line does not support optionally processing functions!'
        )
        parser.add_argument('--delta_table_path', type=str, required=True)
        parser.add_argument('--mds_path', type=str, required=True)
        parser.add_argument('--partition_size', type=int, required=True)
        parser.add_argument('--merge_index', type=bool, required=True)

        parsed = parser.parse_args()
        return parsed

    args = parse_args()

    df = spark.read.table(args.delta_table_path)
    default_mds_kwargs['out'] = args.mds_path
    dataframeToMDS(df,
                   partition_size=args.partition_size,
                   merge_index=args.merge_index,
                   sample_ratio=args.sample_ratio,
                   mds_kwargs=default_mds_kwargs,
                   udf_iterable=None,
                   udf_kwargs=None)
