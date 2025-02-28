use std::usize;

use hdf5::{File, Result};
use ndarray::{arr1, Array};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Number, Value};

#[derive(Debug, Serialize, Deserialize)]
struct F144Settings {
    dtype: String,
    source: String,
    topic: String,
    value_units: Option<String>,
    #[serde(skip)]
    path: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct Ev44Settings {
    source: String,
    topic: String,
    #[serde(skip)]
    path: String,
}

#[derive(Debug)]
enum ModuleSettings {
    F144(F144Settings),
    Ev44(Ev44Settings),
}

fn create_group(parent: &mut hdf5::Group, v: &Value) -> Result<hdf5::Group> {
    let name = v["name"].as_str().unwrap();
    let group = parent.create_group(name).unwrap();
    let attributes = v["attributes"].as_array().unwrap();
    for a in attributes {
        let aname = a["name"].as_str().unwrap();
        let avalue: hdf5::types::VarLenUnicode = a["values"].as_str().unwrap().parse().unwrap();
        let attr = group
            .new_attr::<hdf5::types::VarLenUnicode>()
            .create(aname)
            .unwrap();
        attr.write_scalar(&avalue).unwrap();
    }
    Ok(group)
}

fn write_string_dataset(
    parent: &mut hdf5::Group,
    v: &Map<String, Value>,
) -> Result<String, &'static str> {
    let name = v["name"].as_str().unwrap();
    let values: hdf5::types::VarLenUnicode = v["values"].as_str().unwrap().parse().unwrap();
    let ds = parent
        .new_dataset::<hdf5::types::VarLenUnicode>()
        .create(name)
        .unwrap();
    ds.write_scalar(&values).unwrap();
    Ok(name.to_owned())
}

/// Extracts the shapes and values for a (potentially) nested array.
///
/// The array must be symmetric, meaning the shape is consistent all the way down.
/// E.g. [[1, 2], [3]] is not symmetric.
fn extract_array_details(v: &Value) -> Result<(Vec<usize>, Vec<Number>), &'static str> {
    fn recursion(
        v: &Value,
        shape: &mut Vec<usize>,
        values: &mut Vec<Number>,
        level: usize,
    ) -> std::result::Result<(), &'static str> {
        if v.is_array() {
            let va = v.as_array().unwrap();
            if level == shape.len() {
                shape.push(va.len());
            } else if shape[level] != va.len() {
                return Err("array is not symmetric");
            }
            for x in va {
                if let Err(err) = recursion(x, shape, values, level + 1) {
                    return Err(err);
                }
            }
        } else {
            values.push(v.as_number().unwrap().clone());
        }
        Ok(())
    }
    let mut shape = Vec::new();
    let mut values = Vec::new();
    if let Err(err) = recursion(v, &mut shape, &mut values, 0) {
        Err(err)
    } else {
        Ok((shape, values))
    }
}

/// Writes double values to the dataset (including arrays upto 3 dimensions).
///
/// Note: writes f32 values as f64 because I am lazy.
fn write_dataset_double(
    parent: &mut hdf5::Group,
    v: &Map<String, Value>,
) -> Result<String, &'static str> {
    let name = v["name"].as_str().unwrap();
    if v["values"].is_array() {
        let (shape, values) = extract_array_details(&v["values"]).unwrap();
        let values: Vec<f64> = values.iter().map(|v| v.as_f64().unwrap()).collect();

        match shape.len() {
            1 | 2 | 3 => {
                let data = Array::from_vec(values);
                let data = data.into_shape_with_order(shape).unwrap();
                let builder = parent.new_dataset_builder();
                builder.with_data(&data).create(name).unwrap();
            }
            _ => {
                return Err("Static data is limit to maximum of three dimensions");
            }
        }

        //let values: Vec<f64> = v["values"]
        //    .as_array()
        //    .unwrap()
        //    .iter()
        //    .map(|v| v.as_f64().unwrap())
        //    .collect();
        //let ds = parent
        //    .new_dataset::<T>()
        //    .shape([values.len()])
        //    .create(name)
        //    .unwrap();
        //ds.write_slice(&values, ..).unwrap();
    } else {
        let values = v["values"].as_f64().unwrap();
        let ds = parent.new_dataset::<f64>().create(name).unwrap();
        ds.write_scalar(&values).unwrap();
    }
    Ok(name.to_owned())
}

/// Writes int values to the dataset (including arrays upto 3 dimensions).
///
/// Note: writes i32 values as i64 because I am lazy.
fn write_dataset_int(
    parent: &mut hdf5::Group,
    v: &Map<String, Value>,
) -> Result<String, &'static str> {
    let name = v["name"].as_str().unwrap();
    if v["values"].is_array() {
        let (shape, values) = extract_array_details(&v["values"]).unwrap();
        let values: Vec<i64> = values.iter().map(|v| v.as_i64().unwrap()).collect();

        match shape.len() {
            1 | 2 | 3 => {
                let data = Array::from_vec(values);
                let data = data.into_shape_with_order(shape).unwrap();
                let builder = parent.new_dataset_builder();
                builder.with_data(&data).create(name).unwrap();
            }
            _ => {
                return Err("Static data is limit to maximum of three dimensions");
            }
        }
    } else {
        let values = v["values"].as_i64().unwrap();
        let ds = parent.new_dataset::<i64>().create(name).unwrap();
        ds.write_scalar(&values).unwrap();
    }
    Ok(name.to_owned())
}

fn recurse_json(
    parent: &mut hdf5::Group,
    v: &Value,
    depth: usize,
    path: &mut Vec<String>,
    modules: &mut Vec<ModuleSettings>,
) -> Result<(), &'static str> {
    // TODO: links
    if v["type"].is_string() && v["type"].as_str().unwrap() == "group" {
        let Ok(mut group) = create_group(parent, v) else {
            // TODO: return error
            return Err("could not create group");
        };

        path.push(String::from(group.name()));
        if v["children"].is_array() {
            for c in v["children"].as_array().unwrap() {
                recurse_json(&mut group, c, depth + 1, path, modules);
            }
        }
        path.pop();
    } else if v["module"].is_string() && v["module"].as_str().unwrap() == "dataset" {
        let c = v["config"].as_object().unwrap();
        let dtype = c["dtype"].as_str().unwrap();
        let name = if dtype == "string" {
            write_string_dataset(parent, c)
        } else if dtype == "double" || dtype == "float" {
            write_dataset_double(parent, c)
        } else if dtype == "int32" || dtype == "int64" {
            write_dataset_int(parent, c)
        } else {
            // TODO: handle all possible types!
            println!("unhandled {}", dtype);
            Err("unhandled")
        };

        // TODO: Can datasets even have children?
        if let Ok(name) = name {
            path.push(name);
            if v["children"].is_array() {
                for c in v["children"].as_array().unwrap() {
                    recurse_json(parent, c, depth + 1, path, modules);
                }
            }
            path.pop();
        }
    } else if v["module"].is_string() {
        let path_s = path.join("/");
        // TODO: check config field exists
        let c = serde_json::to_string(v["config"].as_object().unwrap()).unwrap();
        let module = match v["module"].as_str().unwrap() {
            "f144" => {
                let mut module: F144Settings = serde_json::from_str(&c).unwrap();
                module.path = path_s;
                Some(ModuleSettings::F144(module))
            }
            "ev44" => {
                let mut module: Ev44Settings = serde_json::from_str(&c).unwrap();
                module.path = path_s;
                Some(ModuleSettings::Ev44(module))
            }
            _ => None,
        };
        if let Some(module) = module {
            modules.push(module);
        }
    } else {
        println!("{depth} got something unexpected");
    }
    Ok(())
}

fn generate_file_contents(
    json_file: std::fs::File,
    hdf_file: &mut hdf5::File,
) -> Result<Vec<ModuleSettings>> {
    let mut modules = Vec::new();

    let v: Value = serde_json::from_reader(&json_file).unwrap();

    // Top-level should contain one child which is NXentry
    if v["children"].is_array() && v["children"].as_array().unwrap().len() == 1 {
        let toplevel = &v["children"].as_array().unwrap()[0];
        let name = toplevel["name"].as_str().unwrap();
        let ntype = toplevel["type"].as_str().unwrap();
        if ntype != "group" {
            panic!("top-level must be a group");
        }

        let mut group = hdf_file.create_group(name).unwrap();

        // Check attributes contains NXentry?
        let attributes = toplevel["attributes"].as_array().unwrap();
        for a in attributes {
            let aname = a["name"].as_str().unwrap();
            let avalue: hdf5::types::VarLenUnicode = a["values"].as_str().unwrap().parse().unwrap();
            let attr = group
                .new_attr::<hdf5::types::VarLenUnicode>()
                .create(aname)
                .unwrap();
            attr.write_scalar(&avalue).unwrap();
        }

        let mut path = vec![String::from(name)];

        if toplevel["children"].is_array() {
            for c in toplevel["children"].as_array().unwrap() {
                recurse_json(&mut group, c, 0, &mut path, &mut modules);
            }
        }
    } else {
        panic!("top-level must be a single nxentry");
    }
    Ok(modules)
}

fn main() {
    println!("Hello, world!");

    let jfile = std::fs::File::open("/home/matthecl/code/scratch/hdf/nxs.json").unwrap();
    let mut hfile = File::create("example.hf").unwrap();
    //let hfile = hdf5::File::with_options().with_fapl(|p| p.core_filebacked(false)).create(&"in_mem").unwrap();

    let modules = generate_file_contents(jfile, &mut hfile).unwrap();

    dbg!(modules);

    //let builder = group.new_dataset_builder();
    //
    ////let ds = builder.with_data(&arr2(&[
    ////        [1, 2, 3],
    ////        [4, 5, 6],
    ////])).create("arr").unwrap();
    //let ds = builder.with_data(&vec![1, 2, 3]).create("arr").unwrap();
    //
    //let ds = file.new_dataset::<i32>().chunk((1, 5)).shape((1.., 5)).create("chunky").unwrap();
    //ds.write_slice(&vec![1, 2, 3, 4, 5], (0, ..,)).unwrap();
    //
    //ds.resize((2, 5)).unwrap();
    //ds.write_slice(&vec![6, 2, 3, 4, 5], (1, ..,)).unwrap();
    //
    let g = hfile.create_group("group1");
    let gg = g.unwrap().create_group("group2");

    let ds = gg
        .unwrap()
        .new_dataset::<i32>()
        .chunk((1, 5))
        .shape((1.., 5))
        .create("chunky")
        .unwrap();
    ds.write_slice(&vec![1, 2, 3, 4, 5], (0, ..)).unwrap();

    let ds = hfile
        .dataset("entry/instrument/beam_monitor/depends_on")
        .unwrap();
    let ans: hdf5::types::VarLenUnicode = ds.read_scalar().unwrap();
    println!("value read was {}", ans);
    //ds.resize((2, 5)).unwrap();
    //ds.write_slice(&vec![6, 7, 8, 9, 10], (1, ..)).unwrap();

    let gg = hfile.group("group1");
    let ds = gg
        .unwrap()
        .new_dataset::<i32>()
        .shape(1..)
        .create("sinvle")
        .unwrap();
    ds.write_slice(&vec![123], 0..).unwrap();
    ds.resize(2).unwrap();
    ds.write_slice(&vec![245], 1..).unwrap();
    ds.resize(3).unwrap();
    ds.write_slice(&vec![345], 2..).unwrap();
}

#[cfg(test)]
mod tests {
    use ndarray::array;

    use super::*;

    #[test]
    fn test_extract_simple_int_array() {
        let data = r#"
        {
            "a": [1, 2, 3, 4, 5]
        }"#;

        let j: Value = serde_json::from_str::<Value>(data).unwrap();
        let a = &j["a"];

        let (shape, values) = extract_array_details(a).unwrap();
        let values: Vec<i64> = values.iter().map(|v| v.as_i64().unwrap()).collect();

        assert_eq!(shape, vec![5]);
        assert_eq!(values, vec![1, 2, 3, 4, 5]);
    }

    #[test]
    fn test_extract_simple_float_array() {
        let data = r#"
        {
            "a": [1.1, 2.2, 3.3, 4.4, 5.5]
        }"#;

        let j: Value = serde_json::from_str::<Value>(data).unwrap();
        let a = &j["a"];

        let (shape, values) = extract_array_details(a).unwrap();
        let values: Vec<f64> = values.iter().map(|v| v.as_f64().unwrap()).collect();

        assert_eq!(shape, vec![5]);
        assert_eq!(values, vec![1.1, 2.2, 3.3, 4.4, 5.5]);
    }

    #[test]
    fn test_extract_two_level_array_1() {
        let data = r#"
        {
            "a": [[1, 2, 3], [4, 5, 6]]
        }"#;

        let j: Value = serde_json::from_str::<Value>(data).unwrap();
        let a = &j["a"];

        let (shape, values) = extract_array_details(a).unwrap();
        let values: Vec<i64> = values.iter().map(|v| v.as_i64().unwrap()).collect();

        assert_eq!(shape, vec![2, 3]);
        assert_eq!(values, vec![1, 2, 3, 4, 5, 6]);
    }

    #[test]
    fn test_extract_two_level_array_2() {
        let data = r#"
        {
            "a": [[1, 2, 3], [4, 5, 6], [7, 8, 9]]
        }"#;

        let j: Value = serde_json::from_str::<Value>(data).unwrap();
        let a = &j["a"];

        let (shape, values) = extract_array_details(a).unwrap();
        let values: Vec<i64> = values.iter().map(|v| v.as_i64().unwrap()).collect();

        assert_eq!(shape, vec![3, 3]);
        assert_eq!(values, vec![1, 2, 3, 4, 5, 6, 7, 8, 9]);
    }

    #[test]
    fn test_extract_three_level_array_1() {
        let data = r#"
        {
            "a": [
                     [
                         [1, 2, 3]
                     ], 
                     [
                         [4, 5, 6]
                     ]
                 ]
        }"#;

        let j: Value = serde_json::from_str::<Value>(data).unwrap();
        let a = &j["a"];

        let (shape, values) = extract_array_details(a).unwrap();
        let values: Vec<i64> = values.iter().map(|v| v.as_i64().unwrap()).collect();

        assert_eq!(shape, vec![2, 1, 3]);
        assert_eq!(values, vec![1, 2, 3, 4, 5, 6]);
    }

    #[test]
    fn test_extract_three_level_array_2() {
        let data = r#"
        {
            "a": [
                     [
                         [1, 2, 3], [7, 8, 9]
                     ], 
                     [
                         [4, 5, 6], [7, 8, 9]
                     ]
                 ]
        }"#;

        let j: Value = serde_json::from_str::<Value>(data).unwrap();
        let a = &j["a"];

        let (shape, values) = extract_array_details(a).unwrap();
        let values: Vec<i64> = values.iter().map(|v| v.as_i64().unwrap()).collect();

        assert_eq!(shape, vec![2, 2, 3]);
        assert_eq!(values, vec![1, 2, 3, 7, 8, 9, 4, 5, 6, 7, 8, 9]);
    }

    #[test]
    fn test_error_if_not_symmetric_1() {
        let data = r#"
        {
            "a": [[1, 2, 3], [5, 6]]
        }"#;

        let j: Value = serde_json::from_str::<Value>(data).unwrap();
        let a = &j["a"];

        let Err(_) = extract_array_details(a) else {
            assert!(false);
            return;
        };
    }

    #[test]
    fn test_error_if_not_symmetric_2() {
        let data = r#"
        {
            "a": [[1, 2], [5, 6, 7]]
        }"#;

        let j: Value = serde_json::from_str::<Value>(data).unwrap();
        let a = &j["a"];

        let Err(_) = extract_array_details(a) else {
            assert!(false);
            return;
        };
    }

    #[test]
    fn test_error_if_not_symmetric_3() {
        let data = r#"
        {
            "a": [
                     [
                         [1, 2, 3], [7, 8]
                     ], 
                     [
                         [4, 5, 6], [7, 8, 9]
                     ]
                 ]
        }"#;

        let j: Value = serde_json::from_str::<Value>(data).unwrap();
        let a = &j["a"];

        let Err(_) = extract_array_details(a) else {
            assert!(false);
            return;
        };
    }

    #[test]
    fn test_ndarray_does_what_is_expected_1d() {
        let data = r#"
        {
            "a": [1, 2, 3, 4, 5]
        }"#;

        let j: Value = serde_json::from_str::<Value>(data).unwrap();
        let a = &j["a"];

        let (shape, values) = extract_array_details(a).unwrap();
        let values: Vec<i64> = values.iter().map(|v| v.as_i64().unwrap()).collect();

        let data = Array::from_vec(values);
        let data = data.into_shape_with_order(shape).unwrap();

        assert_eq!(data, array![1, 2, 3, 4, 5].into_dyn());
    }

    #[test]
    fn test_ndarray_does_what_is_expected_2d() {
        let data = r#"
        {
            "a": [[1, 2, 3], [3, 4, 5]]
        }"#;

        let j: Value = serde_json::from_str::<Value>(data).unwrap();
        let a = &j["a"];

        let (shape, values) = extract_array_details(a).unwrap();
        let values: Vec<i64> = values.iter().map(|v| v.as_i64().unwrap()).collect();

        let data = Array::from_vec(values);
        let data = data.into_shape_with_order(shape).unwrap();

        assert_eq!(data, array![[1, 2, 3], [3, 4, 5]].into_dyn());
    }

    #[test]
    fn test_ndarray_does_what_is_expected_3d() {
        let data = r#"
        {
            "a": [
                     [
                         [1, 2, 3], [7, 8, 9]
                     ], 
                     [
                         [4, 5, 6], [7, 8, 9]
                     ]
                 ]
        }"#;

        let j: Value = serde_json::from_str::<Value>(data).unwrap();
        let a = &j["a"];

        let (shape, values) = extract_array_details(a).unwrap();
        let values: Vec<i64> = values.iter().map(|v| v.as_i64().unwrap()).collect();

        let data = Array::from_vec(values);
        let data = data.into_shape_with_order(shape).unwrap();

        assert_eq!(
            data,
            array![[[1, 2, 3], [7, 8, 9]], [[4, 5, 6], [7, 8, 9]]].into_dyn()
        );
    }
}
