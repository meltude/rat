use axum::{
    extract::{
        Request,
        ws::{Message, Utf8Bytes, WebSocket, WebSocketUpgrade},
        State,
    },
    middleware::{self, Next},
    response::{Html, IntoResponse},
    routing::get,
    Router,
};

use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::mpsc::{self, UnboundedSender, UnboundedReceiver}; 
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
}

pub async fn spawn_client(addr1: &str, addr2: &str) -> Result<ClientHandle, io::Error> {
    let app = Router::new()
        .route("/", get(index))
        .route("/ws", get(websocket_handler));

    let (app_sender, mut socket_receiver) = mpsc::channel::<Vec<u8>>(32);
    let (socket_sender, app_receiver) = mpsc::channel::<ClientEvent>(32);

    let tcp_listener = TcpListener::bind(addr1).await?;
    let websocket_listener = TcpListener::bind(addr2).await?;

    tokio::spawn(async move {
        axum::serve(websocket_listener, app).await;
    });

    tcp_stream(tcp_listener, socket_sender, socket_receiver).await;

    Ok(ClientHandle { 
        app_sender, app_receiver
    })
}

async fn tcp_stream(
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

async fn websocket_handler(
    ws: WebSocketUpgrade,
) -> impl IntoResponse {
    ws.on_upgrade(|socket| websocket(socket))
}

async fn websocket(mut socket: WebSocket) {
    while let Some(result) = socket.recv().await {
        match result {
            Ok(Message::Binary(bytes)) => {
                println!("received bytes: {:?}", bytes.len());
            }
            Ok(Message::Close(_)) => {
                break;
            }
            Ok(_) => {}
            Err(err) => {
                eprintln!("WebSocket error: {err}");
                break;
            }
        }
    }
}

async fn index() -> Html<&'static str> {
    Html(std::include_str!("../../web/index.html"))
}
