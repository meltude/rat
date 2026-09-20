use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::mpsc::{self, UnboundedSender, UnboundedReceiver}; 
use tokio::net::{TcpListener, TcpStream};
use tokio::io::{self, AsyncReadExt, AsyncWriteExt, ReadHalf};

use ratatui_image::{
    thread::{ThreadProtocol, ResizeRequest},
    picker::Picker,
};

use image::{DynamicImage, ImageReader};
use std::io::Cursor;

pub struct Screenshot {
    pub protocol: ThreadProtocol,
    pub bytes: Vec<u8>,
    pub bytes_len: u8,
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
            bytes_len: 0,
            tx,
            rx,
        }
    }

    pub async fn apply_changes(&mut self) -> io::Result<()> {
        let bytes = self.bytes.clone();
        let image = tokio::task::spawn_blocking(move || {
            ImageReader::new(
                Cursor::new(bytes)
            )
            .with_guessed_format()?
            .decode()
        }).await?
        .map_err(|e| tokio::io::Error::new(tokio::io::ErrorKind::Other, e))?;

        let protocol = Picker::from_query_stdio()
            .map_err(|e| tokio::io::Error::new(tokio::io::ErrorKind::Other, e))?
            .new_resize_protocol(image);

        let tx_clone = self.tx.clone();
        self.protocol = ThreadProtocol::new(tx_clone, Some(protocol));

        Ok(())
    }
}

pub enum ClientEvent {
    Connected(std::net::SocketAddr),
    Disconnected,
    Data(Vec<u8>),
}

pub struct ClientHandle {
    pub app_sender: mpsc::Sender<Vec<u8>>,   
    pub app_receiver: mpsc::Receiver<ClientEvent>, 
    pub app_img_receiver: mpsc::Receiver<ClientEvent>,
}

pub async fn spawn_client(addr1: &str, addr2: &str) -> Result<ClientHandle, io::Error> {
    let (app_sender, mut socket_receiver) = mpsc::channel::<Vec<u8>>(32);
    let (socket_sender, app_receiver) = mpsc::channel::<ClientEvent>(32);
    let (socket_img_sender, app_img_receiver) = mpsc::channel::<ClientEvent>(32);

    let listener = TcpListener::bind(addr1).await?;
    let listener_img = TcpListener::bind(addr2).await?;

    socket_txt(listener, socket_sender, socket_receiver).await;
    socket_img(listener_img, socket_img_sender).await;

    Ok(ClientHandle { 
        app_sender, app_receiver, app_img_receiver 
    })
}

async fn socket_txt(
    listener: TcpListener, 
    socket_sender: Sender<ClientEvent>,
    mut socket_receiver: Receiver<Vec<u8>>,
) {
    tokio::spawn(async move {
        loop {
            let (socket, peer_addr) = match listener.accept().await {
                Ok(pair) => pair,
                Err(_) => continue,
            };
            let _ = socket_sender.send(ClientEvent::Connected(peer_addr)).await;

            let (mut rd, mut wr) = io::split(socket);
            let mut buf = [0u8; 1024];

            loop {
                tokio::select! {
                    Some(input) = socket_receiver.recv() => {
                        if wr.write_all(&input).await.is_err() {
                            break;
                        }
                    }
                    read_result = rd.read(&mut buf) => {
                        match read_result {
                            Ok(0) | Err(_) => break,
                            Ok(n) => {
                                let _ = socket_sender.send(ClientEvent::Data(buf[..n].to_vec())).await;
                            }
                        }
                    }
                }
            }
            let _ = socket_sender.send(ClientEvent::Disconnected).await;
        }
    });
}

async fn socket_img(
    listener: TcpListener,
    socket_sender: Sender<ClientEvent>,
) {
    tokio::spawn(async move {
        loop {
            let (socket, peer_addr) = match listener.accept().await {
                Ok(pair) => pair,
                Err(_) => continue,
            };

            if socket_sender.send(ClientEvent::Connected(peer_addr)).await.is_err() {
                break;
            }

            let (mut rd, _) = io::split(socket);

            let mut header_buf = [0u8; 4];

            loop {
                if rd.read_exact(&mut header_buf).await.is_err() {
                    break;
                }

                let body_len = u32::from_be_bytes(header_buf) as usize;
                let mut body_buf = vec![0u8; body_len];

                if rd.read_exact(&mut body_buf).await.is_err() {
                    break;
                }

                if socket_sender.send(ClientEvent::Data(body_buf)).await.is_err() {
                    break;
                }
            }
        }
        Ok::<_, io::Error>(())
    });
}
