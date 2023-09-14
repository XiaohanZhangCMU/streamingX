use pyo3::prelude::*;

#[pymodule]
fn my_factorial(_py: Python, m: &PyModule) -> PyResult<()> {
    #[pyfn(m, "calculate_factorial")]
    fn rust_factorial(n: u64) -> u64 {
        if n <= 1 {
            1
        } else {
            n * rust_factorial(n - 1)
        }
    }

    Ok(())
}

