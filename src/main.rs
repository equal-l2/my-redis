use executor::InputValue;
use parser::ParsedValue;
use smol::net::TcpListener;
use smol::net::TcpStream;
use smol::prelude::*;

mod bstr;
mod executor;
mod output_value;
mod parser;

use bstr::BStr;
use executor::Executor;
use parser::Parser;

thread_local! {
    static INSTANCE: std::cell::OnceCell<Executor> = const { std::cell::OnceCell::new() };
}

fn remove_non_command_values(value: ParsedValue) -> Result<Vec<InputValue>, &'static [u8]> {
    match value {
        ParsedValue::BulkString(_) => Err(b"ERR Unexpected bare bulk string"),
        ParsedValue::Array(v) => {
            let mut res = Vec::new();
            for item in v {
                match item {
                    ParsedValue::Array(_) => return Err(b"ERR nested arrays are not supported"),
                    ParsedValue::BulkString(s) => res.push(s),
                }
            }
            Ok(res)
        }
    }
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
        let mut values = Vec::new();
        let mut error = Vec::new();
        loop {
            match parser.parse() {
                Some(Ok(v)) => match remove_non_command_values(v) {
                    Ok(v) => values.push(v),
                    Err(e) => error.push(e.to_redis_error()),
                },
                Some(Err(e)) => {
                    error.push(e.to_redis_error());
                    break;
                }
                None => break,
            }
        }

        let results: Vec<_> = values
            .into_iter()
            .map(|v| handle.execute(v))
            .chain(error)
            .collect();

        for v in results.into_iter() {
            stream.write_all(&v).await.unwrap();
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
