use hdf5::{File, H5Type, Hyperslab, Result};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

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

fn write_string_dataset(parent: &mut hdf5::Group, v: &Map<String, Value>) -> Result<String> {
    let name = v["name"].as_str().unwrap();
    let values: hdf5::types::VarLenUnicode = v["values"].as_str().unwrap().parse().unwrap();
    let ds = parent
        .new_dataset::<hdf5::types::VarLenUnicode>()
        .create(name)
        .unwrap();
    ds.write_scalar(&values).unwrap();
    Ok(name.to_owned())
}


fn get_shape(v: &Value) -> Result<Vec<usize>> {
    let mut result = Vec::new();
    let mut v = v;
    loop {
        if v.is_array() {
            result.push(v.as_array().unwrap().len());
            v = &v[0];
        } else {
            break;
        }
    }

    Ok(result)
}


fn write_double_dataset<T: hdf5::H5Type>(parent: &mut hdf5::Group, v: &Map<String, Value>) -> Result<String> {
    let name = v["name"].as_str().unwrap();
    if v["values"].is_array() {
        let values: Vec<f64> = v["values"]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v.as_f64().unwrap())
            .collect();
        let ds = parent
            .new_dataset::<T>()
            .shape([values.len()])
            .create(name)
            .unwrap();
        ds.write_slice(&values, ..).unwrap();
    } else {
        let values = v["values"].as_f64().unwrap();
        let ds = parent.new_dataset::<T>().create(name).unwrap();
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
) {
    // TODO: links
    if v["type"].is_string() && v["type"].as_str().unwrap() == "group" {
        let Ok(mut group) = create_group(parent, v) else {
            // TODO: return error
            return;
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
        } else if dtype == "double" {
            write_double_dataset::<f64>(parent, c)
        //} else if dtype == "float" {
        //    write_double_dataset::<f32>(parent, c)
        }
        else {
            // TODO: handle all types!
            println!("unhandled {}", dtype);
            Ok(String::new())
        };

        // TODO: Can datasets even have children?
        path.push(name.unwrap());
        if v["children"].is_array() {
            for c in v["children"].as_array().unwrap() {
                recurse_json(parent, c, depth + 1, path, modules);
            }
        }
        path.pop();
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
        return;
    } else {
        println!("{depth} got something unexpected");
    }
}

fn generate_file_contents(json_file: std::fs::File, hdf_file: &mut hdf5::File) -> Result<Vec<ModuleSettings>> {
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
    //
    let modules = generate_file_contents(jfile, &mut hfile).unwrap();

    dbg!(modules);


    let data = r#"
        {
            "a": [
            [[1, 2]],[[1, 2]]
            ]
        }"#;

    let j: Value = serde_json::from_str(data).unwrap();
    dbg!(&j);
    let j = j.as_object().unwrap();
    let mut a = &j["a"];
    dbg!(&a);
    let shape = get_shape(&a).unwrap();
    dbg!(&shape);
    let g = hfile.create_group("group123");
    let ds = g
        .unwrap()
        .new_dataset::<i32>()
        .shape(shape.as_slice())
        .create("chunky")
        .unwrap();
    ds.write_slice(&vec![1, 2], (0, 0, ..)).unwrap();
    ds.write_slice(&vec![3, 4], (1, 0, ..)).unwrap();

    let data = r#"
        {
            "a": [
            [1, 2],[1, 2]
            ]
        }"#;

    let j: Value = serde_json::from_str(data).unwrap();
    dbg!(&j);
    let j = j.as_object().unwrap();
    let mut a = &j["a"];
    dbg!(&a);
    let shape = get_shape(&a).unwrap();
    dbg!(&shape);
    let g = hfile.create_group("group1234");
    let ds = g
        .unwrap()
        .new_dataset::<i32>()
        .shape(shape.as_slice())
        .create("chunky")
        .unwrap();
    ds.write_slice(&vec![1, 2], (0, ..)).unwrap();
    ds.write_slice(&vec![3, 4], (1, ..)).unwrap();

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
