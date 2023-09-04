use std::process::exit;

use clap::Arg;
use clap::Command;
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

    match c.subcommand() {
        Some(("get", _sub_m)) => {
            eprintln!("unimplemented");
            exit(1);
        }
        Some(("set", _sub_m)) => {
            eprintln!("unimplemented");
            exit(1);
        }
        Some(("rm", _sub_m)) => {
            eprintln!("unimplemented");
            exit(1);
        }
        _ => unreachable!(), // Either no subcommand or one not tested for...
    }
}
