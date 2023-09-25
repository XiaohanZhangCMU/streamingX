use std::sync::Arc;
use bytes::Bytes;
use parquet::arrow::arrow_reader::{
    ArrowReaderOptions, RowSelection, RowSelector, ParquetRecordBatchReaderBuilder
};
use parquet::arrow::ParquetRecordBatchStreamBuilder;
use parquet::arrow::async_reader::AsyncFileReader;
use parquet::file::footer::parse_metadata;
use parquet::file::metadata::ParquetMetaData;
use futures::future::{BoxFuture, FutureExt};
use futures::TryStreamExt;
use std::ops::Range;
use std::sync::Mutex;
use arrow::error::Result as ArrowResult;
use arrow::record_batch::RecordBatch;
use arrow::pyarrow::ToPyArrow;
use parquet::errors::Result;
use tokio::task;

use pyo3::prelude::*;
use pyo3::prelude::{PyResult, Python};
use std::fs::File;

fn print_type_of<T>(_: &T) {
    println!("{}", std::any::type_name::<T>())
}

#[pyfunction]
fn read_batch(indicies: Vec<usize>, pq_path: String) -> Result<Py<PyAny>, PyErr> { 

    let options = ArrowReaderOptions::new().with_page_index(true);

    let mut differences = Vec::new();
    if indicies[0] > 0 {
        differences.push(RowSelector {row_count: indicies[0], skip: true});
    }
    differences.push(RowSelector {row_count: 1, skip: false});

    for i in 1..indicies.len() {
        let rows_to_skip = indicies[i].checked_sub(indicies[i-1]+1).unwrap();
        if rows_to_skip > 0 {
            differences.push(RowSelector {row_count: rows_to_skip, skip: true});
        }
        if rows_to_skip < 0 { 
            panic!("Value error: difference must be geater or equal 0.");
        }
        differences.push(RowSelector {row_count: 1, skip: false});
    }

    let selection = RowSelection::from(differences);

    let file = File::open(pq_path).unwrap();

    let sync_batches = ParquetRecordBatchReaderBuilder::try_new_with_options(file, options)
        .unwrap()
        .with_batch_size(indicies.len())
        .with_row_selection(selection)
        .build()
        .unwrap()
        .collect::<ArrowResult<Vec<_>>>()
        .unwrap();

    Python::with_gil(|py| {
        sync_batches.to_pyarrow(py) 
    })
}

#[pyfunction]
fn read_one(n: usize, pq_path: String) -> Result<Py<PyAny>, PyErr> { 

    let options = ArrowReaderOptions::new().with_page_index(true);

    // let selection = RowSelection::from(vec![
    //     RowSelector::skip(n),
    //     RowSelector::select(1)
    // ]);

    let file = File::open(pq_path).unwrap();

    let sync_batches = ParquetRecordBatchReaderBuilder::try_new_with_options(file, options)
        .unwrap()
        .with_batch_size(1)
        .with_row_selection((vec![RowSelector::skip(n), RowSelector::select(1)]).into())
        .build()
        .unwrap()
        .collect::<ArrowResult<Vec<_>>>()
        .unwrap();

    Python::with_gil(|py| {
        sync_batches.to_pyarrow(py) 
    })
}

#[pymodule]
fn delta(_py: Python<'_>, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(read_one, m)?)?;
    m.add_function(wrap_pyfunction!(read_batch, m)?)?;
    Ok(())
}

