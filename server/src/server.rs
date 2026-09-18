use tokio::sync::mpsc::{self, Receiver, Sender};
use tokio::net::{TcpListener, TcpStream};
use tokio::io::{self, AsyncReadExt, AsyncWriteExt, ReadHalf};

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
            let _ = socket_sender.send(ClientEvent::Connected(peer_addr)).await;

            let (mut rd, _) = io::split(socket);

            let mut buf = [0u8; 1024];

            loop {
                let n = rd.read(&mut buf).await?;

                if n == 0 {
                    break;
                }

                let _ = socket_sender.send(ClientEvent::Data(buf[..n].to_vec())).await;
            }
        }
        Ok::<_, io::Error>(())
    });
}
