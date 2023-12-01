// kvs-client set <KEY> <VALUE> [--addr IP-PORT]

// Set the value of a string key to a string.

// --addr accepts an IP address, either v4 or v6, and a port number, with the format IP:PORT. If --addr is not specified then connect on 127.0.0.1:4000.

// Print an error and return a non-zero exit code on server error, or if IP-PORT does not parse as an address.

// kvs-client get <KEY> [--addr IP-PORT]

// Get the string value of a given string key.

// --addr accepts an IP address, either v4 or v6, and a port number, with the format IP:PORT. If --addr is not specified then connect on 127.0.0.1:4000.

// Print an error and return a non-zero exit code on server error, or if IP-PORT does not parse as an address.

// kvs-client rm <KEY> [--addr IP-PORT]

// Remove a given string key.

// --addr accepts an IP address, either v4 or v6, and a port number, with the format IP:PORT. If --addr is not specified then connect on 127.0.0.1:4000.

// Print an error and return a non-zero exit code on server error, or if IP-PORT does not parse as an address. A "key not found" is also treated as an error in the "rm" command.

// kvs-client -V

// Print the version.

use std::{
    io::{BufRead, BufReader, Write},
    net::{IpAddr, Ipv4Addr, SocketAddr, TcpStream},
    process::exit,
};

use clap::{Parser, Subcommand};
use kvs::Command;

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
    #[arg(long,global=true,default_value_t=SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 4000))]
    addr: SocketAddr,
}

#[derive(Subcommand)]
enum Commands {
    Get { key: String },
    Rm { key: String },
    Set { key: String, value: String },
}

fn main() {
    let cli = Cli::parse();
    let mut stream = TcpStream::connect(cli.addr).unwrap();
    let mut rm_flag = false;
    match cli.command {
        Commands::Get { key } => {
            let s = serde_json::to_string(&Command::Get(key)).unwrap();
            stream.write_all((s + "\n").as_bytes()).unwrap();
        }
        Commands::Rm { key } => {
            let s = serde_json::to_string(&Command::Rm(key)).unwrap();
            stream.write_all((s + "\n").as_bytes()).unwrap();
            rm_flag = true;
        }
        Commands::Set { key, value } => {
            let s = serde_json::to_string(&Command::Set(key, value)).unwrap();
            stream.write_all((s + "\n").as_bytes()).unwrap();
        }
    }
    let mut bf = BufReader::new(stream);
    let mut s = String::new();
    bf.read_line(&mut s).unwrap();
    if s.contains("Key not found") && rm_flag {
        eprintln!("{}", s);
        exit(1);
    }
    println!("{}", s);
}
