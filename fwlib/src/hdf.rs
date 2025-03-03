use hdf5::*;

struct HdfFile {
    file: hdf5::File,
}

impl HdfFile {
    fn append_scalar_dataset(dataset: &mut hdf5::Dataset, value: i32) {}
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn can_append_to_1d_dataset() {
        // Create a simple in-memory HDF file
        // Add a 1d dataset

        // write data

        // assert data written
    }
}

// TODO:
// initially just write a numeric scalar, so we can start putting together a walking skeleton.
// different numeric types
// arrays
// start and stop times?
