use std::{
    env, fs,
    io::{self, Read},
    process::exit,
};

fn main() {
    // ARGV
    let args: Vec<String> = env::args().collect();
    println!("ARGS={}", args.join(","));

    // ENV
    println!("ENV_FOO={}", env::var("FOO").unwrap_or_default());

    // STDIN
    let mut buf = String::new();
    io::stdin().read_to_string(&mut buf).unwrap();
    println!("STDIN={}", buf.replace('\n', "\\n"));

    // DATA DIR: read and write
    if let Ok(dat) = fs::read_to_string("infile.txt") {
        println!("DATA_READ={}", dat.trim());
    } else {
        println!("DATA_READ=<missing>");
    }
    fs::write("out.txt", "hello-from-wasi").unwrap();

    // STDERR
    eprintln!("TO_STDERR");

    // EXIT CODE from env
    let code = env::var("EXIT")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);
    exit(code);
}
