use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

async fn handle_connection(mut stream: TcpStream) {
    let mut buf = [0; 512];
    println!("accepted new connection");

    loop {
        let read_count = stream.read(&mut buf).await.unwrap();
        if read_count == 0 {
            break;
        }
        stream.write_all(b"+PONG\r\n").await.unwrap();
    }
}

#[tokio::main]
async fn main() {
    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();

    loop {
        let (stream, _) = listener.accept().await.unwrap();

        tokio::spawn(async {
            handle_connection(stream).await;
        });
    }
}
