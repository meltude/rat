use tokio::time::Duration;
use tokio::sync::mpsc;
use scrap::{Capturer, Display};
use std::io::ErrorKind::WouldBlock;
use std::thread;
use image::ImageReader;
use std::io::Cursor;

use crate::Data;

pub async fn handle_screenshot(tx: mpsc::Sender<Data>) -> std::io::Result<()> {
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

            let img = ImageReader::new(
                Cursor::new(bitflipped)
            )
            .with_guessed_format()?
            .decode()
            .expect("cannot decode bytes to image");
        
            let mut compressed_bytes = Vec::new();
            img.write_to(&mut Cursor::new(&mut compressed_bytes), image::ImageFormat::Avif).unwrap();

            if tx.blocking_send(Data::Screenshot(compressed_bytes)).is_err() {
                break;
            }

            std::thread::sleep(frame_duration);
        }

        Ok(())
    })
    .await?
}
