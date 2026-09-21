use ratatui_image::{
    thread::{ThreadProtocol, ResizeRequest},
    picker::Picker,
};
use image::{DynamicImage, ImageReader};
use std::io::Cursor;

use tokio::sync::mpsc::{self, UnboundedSender, UnboundedReceiver}; 
use tokio::io;

pub struct Screenshot {
    pub protocol: ThreadProtocol,
    pub bytes: Vec<u8>,
    pub tx: UnboundedSender<ResizeRequest>,
    pub rx: UnboundedReceiver<ResizeRequest>,
}

impl Screenshot {
    pub fn new() -> Self {
        let (tx, rx) = mpsc::unbounded_channel::<ResizeRequest>();
        let tx_clone = tx.clone();
        let protocol = ThreadProtocol::new(tx_clone, None);

        Self {
            protocol,
            bytes: Vec::new(),
            tx,
            rx,
        }
    }

    pub async fn update_screenshot(&mut self) -> io::Result<()> {
        let bytes = self.bytes.clone();
        let screenshot = tokio::task::spawn_blocking(move || {
            ImageReader::new(
                Cursor::new(bytes)
            )
            .with_guessed_format()?
            .decode()
        }).await?
        .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;

        let protocol = Picker::from_query_stdio()
            .map_err(|e| tokio::io::Error::new(tokio::io::ErrorKind::Other, e))?
            .new_resize_protocol(screenshot);

        let tx_clone = self.tx.clone();
        self.protocol = ThreadProtocol::new(tx_clone, Some(protocol));

        Ok(())
    }
}
