use std::{
    io::{Read, Write},
    net::{TcpListener, TcpStream},
    thread::{self},
};

fn handle_connection(mut stream: TcpStream) {
    let mut buf = [0; 512];
    println!("accepted new connection");

    loop {
        let read_count = stream.read(&mut buf).unwrap();
        if read_count == 0 {
            break;
        }
        stream.write_all(b"+PONG\r\n").unwrap();
    }
}

fn main() {
    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                thread::spawn(|| {
                    handle_connection(stream);
                });
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}
