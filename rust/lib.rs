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
use parquet::file::reader::SerializedPageReader;
use parquet::file::reader::ChunkReader;
use parquet::file::page_index::index_reader::{
    read_columns_indexes, read_pages_locations,
};
use futures::future::{BoxFuture, FutureExt};
use futures::TryStreamExt;
use arrow::error::Result as ArrowResult;
use arrow::record_batch::RecordBatch;
use arrow::pyarrow::ToPyArrow;
use parquet::errors::Result;
use tokio::task;

use pyo3::prelude::*;
use pyo3::prelude::{PyResult, Python};

use std::sync::Arc;
use bytes::Bytes;
use std::fs::File;
use std::time::Instant;
use std::time::Duration;
use std::ops::Range;
use std::sync::Mutex;

//use parquet::arrow::array_reader::ArrayReader;

fn print_type_of<T: ?Sized>(_: &T) {
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

    let tik = Instant::now();

    let sync_batches = ParquetRecordBatchReaderBuilder::try_new_with_options(file, options)
        .unwrap()
        .with_batch_size(1)
        .with_row_selection((vec![RowSelector::skip(n), RowSelector::select(1)]).into())
        .build()
        .unwrap()
        .collect::<ArrowResult<Vec<_>>>()
        .unwrap();

    let tok = Instant::now();
    let elapsed_time = tok.duration_since(tik);
    println!("Elapsed rust fetch time: {} seconds and {} milliseconds", elapsed_time.as_secs(), elapsed_time.subsec_millis());

    Python::with_gil(|py| {
        sync_batches.to_pyarrow(py) 
    })
}


#[pyfunction]
fn read_one_v2(n: usize, pq_path: String) { // -> Result<Py<PyAny>, PyErr> { 


    let chunk_reader = File::open(pq_path).unwrap();
    let metadata = parse_metadata(&chunk_reader).unwrap();
    let rg_meta = metadata.row_group(0);

    //let mut columns_indexes = vec![];
    //let mut offset_indexes = vec![];

    //let column_index = read_columns_indexes(&chunk_reader, rg_meta.columns()).unwrap();
    let offset_index = read_pages_locations(&chunk_reader, rg_meta.columns()).unwrap();

    //let pmetadata = ParquetMetaData::new_with_page_index(
    //                metadata.file_metadata().clone(),
    //                vec![rg_meta.clone()],
    //                Some(columns_indexes),
    //                Some(offset_indexes),
    //            );
    //let page_index = metadata.offset_index().unwrap();
    //let pageloc = &page_index[0][0][0];
    //let mut page_locations = vec![];
    //page_locations.push(pageloc.clone());

    let tik = Instant::now();

    //let page_locations = &offset_index[0];
    let page_location = &offset_index[0][214];
    let page_locations = vec![page_location.clone()];
    println!("{}", {page_location.offset});
    println!("{}", {page_location.compressed_page_size});
    println!("{}", {page_location.first_row_index});
    let column_meta = rg_meta.column(0);
    let total_rows = rg_meta.num_rows() as usize;
    //let page_reader = SerializedPageReader::new(chunk_reader.into(), column_meta, total_rows, Some(page_locations.to_vec())).unwrap();
    //let mut page_reader = SerializedPageReader::new(chunk_reader.into(), column_meta, total_rows, Some(page_locations.clone())).unwrap();
    let selection = RowSelection::from(vec![
        RowSelector::skip(n),
        RowSelector::select(1)
    ]);

    let ranges = selection.scan_ranges(&page_locations.clone());

    //print_type_of(&page_reader);
    print_type_of(&ranges);
    println!("{:?}", ranges);
    let range = ranges.first().unwrap();
    print_type_of(&range);
    let start = range.start;
    let length = range.end-range.start + 1;

    let bytes = chunk_reader.get_bytes(start as u64, length);
    print_type_of(&bytes);
    //println!("{:?}", bytes);
    let tok = Instant::now();

    //if let Some(page_result) = page_reader.next() { 
    //    match page_result { 
    //        Ok(page) => { 
    //            print_type_of(&page);
    //            println!("num vals: {}", page.num_values());
    //            //println!("buffer: {:?}", page.buffer());
    //            print_type_of(page.buffer().data());
    //            let bytes = page.buffer().data().to_vec();
    //            let s_result = std::str::from_utf8(&bytes);

    //            match s_result {
    //                Ok(s) => {
    //                    println!("I am here 1");
    //                    print_type_of(&s);
    //                    //println!("s = {}", s.unwrap());
    //                }
    //                Err(e) => {
    //                    //println!("{:?}", bytes);
    //                    eprintln!("Error convert byte array: {:?}", e);
    //                }
    //            }
    //        }
    //        Err(e) => {
    //            eprintln!("Error reading page: {:?}", e);
    //        }
    //    }
    //} else { 
    //    println!("No more pages to read.");
    //}

//    //let page = page_reader.take(1); 
//    let page = page_reader.next();
//

    let elapsed_time = tok.duration_since(tik);
    println!("Elapsed rust fetch time: {} seconds and {} milliseconds", elapsed_time.as_secs(), elapsed_time.subsec_millis());
    //println!("num vals: {}", page.unwrap().expect("REASON").num_values());
    //println!("buffer: {}", page.unwrap().expect("REASON").buffer());

    //Python::with_gil(|py| {
    //    sync_batches.to_pyarrow(py) 
    //})
}

#[pyfunction]
fn read_one_v3(n: usize, pq_path: String) { // -> Result<Py<PyAny>, PyErr> { 
    let col_indx = 4;
    let rg_indx = 0;
    let pg_indx = 0;


    let chunk_reader = File::open(pq_path).unwrap();
    let metadata = parse_metadata(&chunk_reader).unwrap();
    let rg_meta = metadata.row_group(0);

    let tik = Instant::now();

    let offset_index = read_pages_locations(&chunk_reader, rg_meta.columns()).unwrap();
    let page_location = &offset_index[col_indx][rg_indx];
    let page_locations = vec![page_location.clone()];
    println!("{}", {page_location.offset});
    println!("{}", {page_location.compressed_page_size});
    println!("{}", {page_location.first_row_index});
    let column_meta = rg_meta.column(col_indx);
    let total_rows = rg_meta.num_rows() as usize;
    let mut page_reader = SerializedPageReader::new(chunk_reader.into(), column_meta, total_rows, Some(page_locations)).unwrap();
    let tok = Instant::now();

    if let Some(page_result) = page_reader.next() { 
        match page_result { 
            Ok(page) => { 
                print_type_of(&page);
                println!("num vals: {}", page.num_values());
                //println!("buffer: {:?}", page.buffer());
                print_type_of(page.buffer().data());
                let bufferPtr = page.buffer();
                let bytes = page.buffer().data().to_vec(); //working for 1st element
                //let bytes = vec![page.buffer().data()];
                let s_result = std::str::from_utf8(&bytes);
                println!("{}", page.page_type());
                println!("{}", page.encoding());
                println!("{}", bufferPtr.len());
                println!("{}", bytes.len());

                match s_result {
                    Ok(s) => {
                        println!("I am here 1");
                        print_type_of(&s);
                        println!("s = {}", s);
                    }
                    Err(e) => {
                        //println!("{:?}", bytes);
                        eprintln!("Error convert byte array: {:?}", e);
                    }
                }
            }
            Err(e) => {
                eprintln!("Error reading page: {:?}", e);
            }
        }
    } else { 
        println!("No more pages to read.");
    }

    let elapsed_time = tok.duration_since(tik);
    println!("Elapsed rust fetch time: {} seconds and {} milliseconds", elapsed_time.as_secs(), elapsed_time.subsec_millis());

    //Python::with_gil(|py| {
    //    sync_batches.to_pyarrow(py) 
    //})
}


#[pymodule]
fn delta(_py: Python<'_>, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(read_one, m)?)?;
    m.add_function(wrap_pyfunction!(read_one_v2, m)?)?;
    m.add_function(wrap_pyfunction!(read_one_v3, m)?)?;
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
