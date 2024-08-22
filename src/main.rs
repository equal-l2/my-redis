use smol::net::TcpListener;
use smol::net::TcpStream;
use smol::prelude::*;

mod executor;
mod parser;
mod value;

use executor::Executor;
use parser::Parser;

thread_local! {
    static INSTANCE: std::cell::OnceCell<Executor> = const { std::cell::OnceCell::new() };
}

async fn handle_stream(mut stream: TcpStream) {
    let handle_opt =
        INSTANCE.with(|inner| inner.get().unwrap().connect(stream.peer_addr().unwrap()));
    let mut handle = if let Some(handle) = handle_opt {
        handle
    } else {
        stream
            .write_all(b"-ERR connection full, try again\r\n")
            .await
            .unwrap();
        stream.shutdown(std::net::Shutdown::Both).unwrap();
        return;
    };
    let mut buffer = [0; 1024];
    let mut parser = Parser::new();
    let mut stop = false;

    while !stop {
        match stream.read(&mut buffer).await {
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
            let result = handle.execute(arr);
            stream.write_all(&result).await.unwrap();
            stream.flush().await.unwrap();
        }
    }
}

fn main() {
    INSTANCE.with(|inner| inner.set(Executor::new(16)).unwrap());
    let executor = smol::LocalExecutor::new();
    smol::block_on(executor.run(async {
        let listener = TcpListener::bind("127.0.0.1:7379").await.unwrap();
        listener
            .incoming()
            .for_each(|stream| {
                executor
                    .spawn(async move {
                        let stream = stream.expect("Stream is None");
                        handle_stream(stream).await;
                    })
                    .detach();
            })
            .await
    }))
}
