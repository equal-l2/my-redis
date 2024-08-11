use tokio::io::AsyncWriteExt;

mod executor;
mod parser;
mod value;

use executor::Executor;
use parser::Parser;

thread_local! {
    static EXECUTOR: std::cell::RefCell<Executor> = std::cell::RefCell::new(Executor::new());
}

async fn handle_stream(mut stream: tokio::net::TcpStream) {
    let mut buffer = [0; 1024];
    let mut parser = Parser::new();
    let mut stop = false;

    while !stop {
        stream.readable().await.unwrap();
        match stream.try_read(&mut buffer) {
            Ok(0) => {
                stop = true;
            }
            Ok(n) => {
                parser.extend(&buffer[..n]);
            }
            Err(ref e) if e.kind() == tokio::io::ErrorKind::WouldBlock => {
                continue;
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
            let result = EXECUTOR.with_borrow_mut(|ex| ex.execute(arr));
            stream.write_all(&result).await.unwrap();
        }
    }
}

fn main() {
    tokio::runtime::Builder::new_current_thread()
        .enable_io()
        .build()
        .unwrap()
        .block_on(async {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:7379")
                .await
                .unwrap();

            loop {
                let (stream, _) = listener.accept().await.unwrap();
                tokio::spawn(async move { handle_stream(stream).await });
            }
        })
}
