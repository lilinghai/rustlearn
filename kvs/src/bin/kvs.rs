use std::env;
use std::process::exit;

use clap::Arg;
use clap::Command;
use kvs::KvStore;
use kvs::KvsEngine;
fn main() {
    let c = Command::new("kvs")
        .version(env!("CARGO_PKG_VERSION"))
        .author("lilinghai")
        .about("key value storage")
        .subcommand(Command::new("get").arg(Arg::new("Key").required(true)))
        .subcommand(
            Command::new("set")
                .arg(Arg::new("Key").required(true))
                .arg(Arg::new("Value").required(true)),
        )
        .subcommand(Command::new("rm").arg(Arg::new("Key").required(true)))
        .get_matches();

    let binding = env::current_dir().unwrap();
    let d = binding.as_path();
    let store = KvStore::open(d).unwrap();

    match c.subcommand() {
        Some(("get", sub_m)) => {
            let v: &String = sub_m.get_one("Key").unwrap();
            let r = store.get(v.to_string()).unwrap();
            match r {
                Some(s) => println!("{}", s),
                None => {
                    println!("Key not found");
                }
            }
        }
        Some(("set", sub_m)) => {
            let k: &String = sub_m.get_one("Key").unwrap();
            let v: &String = sub_m.get_one("Value").unwrap();
            store.set(k.to_string(), v.to_string()).unwrap();
        }
        Some(("rm", sub_m)) => {
            let v: &String = sub_m.get_one("Key").unwrap();
            let r = store.remove(v.to_string());
            match r {
                Ok(_) => {}
                Err(e) => {
                    println!("{}", e.to_string());
                    exit(1);
                }
            }
        }
        _ => unreachable!(), // Either no subcommand or one not tested for...
    }
}
