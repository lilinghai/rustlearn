// kvs-server [--addr IP-PORT] [--engine ENGINE-NAME]

// Start the server and begin listening for incoming connections. --addr accepts an IP address, either v4 or v6, and a port number, with the format IP:PORT. If --addr is not specified then listen on 127.0.0.1:4000.

// If --engine is specified, then ENGINE-NAME must be either "kvs", in which case the built-in engine is used,
// or "sled", in which case sled is used. If this is the first run (there is no data previously persisted) then the default value is "kvs";
//  if there is previously persisted data then the default is the engine already in use.
//  If data was previously persisted with a different engine than selected, print an error and exit with a non-zero exit code.

// Print an error and return a non-zero exit code on failure to bind a socket, if ENGINE-NAME is invalid, if IP-PORT does not parse as an address.

// kvs-server -V

// Print the version.

use std::{
    env,
    io::{BufRead, BufReader, Write},
    net::{IpAddr, Ipv4Addr, SocketAddr, TcpListener},
    process::exit,
    sync::Arc,
};

use slog::{error, info};
use sloggers::terminal::{Destination, TerminalLoggerBuilder};
use sloggers::types::Severity;
use sloggers::Build;

use clap::{Parser, ValueEnum};

use kvs::{
    thread_pool::{NaiveThreadPool, ThreadPool},
    Command, KvStore, KvsEngine, SledKvsEngine, LOGFILENAM,
};

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Cli {
    #[arg(long, value_enum)]
    engine: Option<Engine>,
    #[arg(long,default_value_t=SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 4000))]
    addr: SocketAddr,
}

#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, ValueEnum, Debug)]
enum Engine {
    Kvs,
    Sled,
}

fn main() {
    let mut builder = TerminalLoggerBuilder::new();
    builder.level(Severity::Debug);
    builder.destination(Destination::Stderr);

    let logger = builder.build().unwrap();
    info!(logger, "Hello World!",);

    let cli = Cli::parse();
    info!(
        logger,
        "address is {}, engine is {:?}, version is {}",
        cli.addr,
        cli.engine,
        env!("CARGO_PKG_VERSION")
    );

    let binding = env::current_dir().unwrap();
    let d = binding.as_path();
    let store: Arc<dyn KvsEngine + Sync + Send>;
    let b = d.join("db");
    let sledkv = b.exists();
    let b2 = d.join(LOGFILENAM);
    let kvskv = b2.exists();

    match cli.engine {
        Some(e) => match e {
            Engine::Kvs => {
                if sledkv {
                    error!(logger, "store engine is wrong");
                    exit(1);
                }
                store = Arc::new(KvStore::open(d).unwrap())
            }
            Engine::Sled => {
                if kvskv {
                    error!(logger, "store engine is wrong");
                    exit(1);
                }
                store = Arc::new(SledKvsEngine::open(d).unwrap())
            }
        },
        None => {
            if sledkv {
                store = Arc::new(SledKvsEngine::open(d).unwrap())
            } else {
                store = Arc::new(KvStore::open(d).unwrap())
            }
        }
    }
    let listener = TcpListener::bind(cli.addr).unwrap();
    info!(logger, "server is started");
    let tp = NaiveThreadPool::new(10).unwrap();
    for income in listener.incoming() {
        match income {
            Ok(mut stream) => {
                // 通过原子引用计数在多线程共享数据
                let store_clone=store.clone();
                tp.spawn(move || {
                    let mut s = String::new();
                    let mut bf = BufReader::new(&stream);
                    bf.read_line(&mut s).unwrap();
                    println!("read data {}", s);
                    // parse s to rm/get/set
                    let command: Command = serde_json::from_str(&s).unwrap();
                    match command {
                        Command::Get(key) => {
                            let r = store_clone.get(key).unwrap();
                            match r {
                                Some(rs) => {
                                    stream.write((rs + "\n").as_bytes()).unwrap();
                                }
                                None => {
                                    stream.write(b"Key not found\n").unwrap();
                                }
                            }
                        }
                        Command::Rm(key) => {
                            let r = store_clone.remove(key);
                            match r {
                                Ok(_) => {
                                    stream.write(b"Success\n").unwrap();
                                }
                                Err(e) => {
                                    stream.write((e.to_string() + "\n").as_bytes()).unwrap();
                                }
                            }
                        }
                        Command::Set(key, value) => {
                                store_clone.set(key, value).unwrap();
                                stream.write(b"Success\n").unwrap();
                        }
                        _ => unreachable!(), // Either no subcommand or one not tested for...
                    }
                });
            }
            Err(_) => todo!(),
        }
    }
}

#[cfg(test)]
mod test {
    use std::path::Path;
    #[test]
    fn pathj() {
        let p = Path::new("/etc");
        let p2 = p.join("data");
        println!("{:?}, {:?}", p, p2);
    }
}
