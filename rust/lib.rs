use std::sync::Arc;
use bytes::Bytes;
use parquet::arrow::arrow_reader::{
    ArrowReaderOptions, RowSelection, RowSelector, ParquetRecordBatchReaderBuilder
};
use parquet::arrow::ParquetRecordBatchStreamBuilder;
use parquet::arrow::arrow_reader::ArrowReaderMetadata;
use parquet::arrow::async_reader::AsyncFileReader;
use parquet::file::footer::parse_metadata;
use parquet::file::metadata::ParquetMetaData;
use parquet::file::serialized_reader::SerializedFileReader;
use parquet::file::serialized_reader::SerializedRowGroupReader;
use parquet::file::properties::ReaderProperties;
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


#[pyfunction]
fn read_one_v2(n: usize, pq_path: String){ // -> Result<Py<PyAny>, PyErr> { 

    let chunk_reader = File::open(pq_path).unwrap();
    let metadata = parse_metadata(&chunk_reader)?;
    let pmetadata = ParquetMetaData::new_with_page_index(
                    metadata.file_metadata().clone(),
                    filtered_row_groups,
                    Some(columns_indexes),
                    Some(offset_indexes),
                );


    //let builder = ReadOptionsBuilder::new();
    ////enable read page index
    //let options = builder.with_page_index().build();
    //let reader_result = SerializedFileReader::new_with_options(test_file, options);

    //let options = ArrowReaderOptions::new().with_page_index(true);


    //let sync_batches = ParquetRecordBatchReaderBuilder::try_new_with_options(file, options)
    //    .unwrap()
    //    .with_batch_size(1)
    //    .with_offset(n) //row_selection((vec![RowSelector::skip(n), RowSelector::select(1)]).into())
    //    .build()
    //    .unwrap()
    //    .collect::<ArrowResult<Vec<_>>>()
    //    .unwrap();

    //Python::with_gil(|py| {
    //    sync_batches.to_pyarrow(py) 
    //})
}


#[pymodule]
fn delta(_py: Python<'_>, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(read_one, m)?)?;
    m.add_function(wrap_pyfunction!(read_one_v2, m)?)?;
    m.add_function(wrap_pyfunction!(read_batch, m)?)?;
    Ok(())
}


///////////////////////////////////////////////////////////////////

//#[pyfunction]
//fn read_one_v2(n: usize, pq_path: String) -> Result<Py<PyAny>, PyErr> { 
//
//    let file = File::open(pq_path).unwrap();
//    let reader_metadata = ArrowReaderMetadata::load(&file, Default::default()).unwrap();
//    let metadata = reader_metadata.metadata();
//    let row_group_metadata = metadata.row_group(0);
//    let props = Arc::new(ReaderProperties::builder().build());
//    let f = Arc::clone(file);
//    let row_group_reader = SerializedRowGroupReader::new(
//            f,
//            row_group_metadata,
//            metadata.offset_index().map(|x| x[0].as_slice()),
//            props,
//        );
//
//    //let row_group = reader.get_row_group(0).unwrap();
//    let page_reader = row_group_reader.get_column_page_reader(0).unwrap();
//
//    let mut iter = page_reader.skip(5);
//
//    let data = iter.next();
//
//    Python::with_gil(|py| {
//        data.to_pyarrow(py) 
//    })
//}
