use std::io::Read;
use std::net::TcpListener;
use std::net::TcpStream;

mod executor;
mod parser;
mod value;

use executor::Executor;
use parser::Parser;

thread_local! {
    static EXECUTOR: std::cell::RefCell<Executor> = std::cell::RefCell::new(Executor::new());
}

fn handle_stream(mut stream: TcpStream) {
    let mut buffer = [0; 1024];
    let mut parser = Parser::new();
    let mut stop = false;

    while !stop {
        match stream.read(&mut buffer) {
            Ok(0) => {
                stop = true;
            }
            Ok(n) => {
                parser.extend(&buffer[..n]);
            }
            Err(e) => {
                println!("Error reading from client: {}", e);
                break;
            }
        }

        // parse
        parser.parse();

        // execute
        while let Some(arr) = parser.pop() {
            EXECUTOR.with_borrow_mut(|ex| ex.execute(arr, &mut stream));
        }
    }
}

fn main() {
    let listener = TcpListener::bind("127.0.0.1:7379").unwrap();
    for stream in listener.incoming() {
        println!("new connection: {:?}", stream);
        match stream {
            Ok(stream) => {
                println!("New client connected");
                handle_stream(stream);
            }
            Err(e) => {
                println!("Error accepting client: {}", e);
            }
        }
    }
}
