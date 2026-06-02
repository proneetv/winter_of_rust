// Tokio version of the tic-tac-toe server.
//
// tokio version:   async tasks on a small runtime, a tokio Mutex, and a
// `broadcast` channel so a move fans out to every subscriber automatically.

use std::sync::Arc;

use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{broadcast, Mutex};

/// Shared game state. Both player tasks lock this to read/change the board.
struct Game {
    board: [char; 9],
    turn: char,
    over: bool,
}

/// Render the board as a string (with telnet-friendly \r\n line endings).
fn render(board: &[char; 9]) -> String {
    let mut s = String::from("\r\n");
    for row in 0..3 {
        let i = row * 3;
        s.push_str(&format!(
            " {} | {} | {} \r\n",
            board[i],
            board[i + 1],
            board[i + 2]
        ));
        if row < 2 {
            s.push_str("---+---+---\r\n");
        }
    }
    s
}

fn winner(b: &[char; 9]) -> Option<char> {
    let lines = [
        [0, 1, 2],
        [3, 4, 5],
        [6, 7, 8], // rows
        [0, 3, 6],
        [1, 4, 7],
        [2, 5, 8], // cols
        [0, 4, 8],
        [2, 4, 6], // diagonals
    ];
    for l in lines {
        if b[l[0]] != ' ' && b[l[0]] == b[l[1]] && b[l[1]] == b[l[2]] {
            return Some(b[l[0]]);
        }
    }
    None
}

/// Handle one player's connection as an async task.
///
/// `tx` is the broadcast sender: anything we publish to it is delivered to every
/// player's subscriber, so we never have to track sockets ourselves.
async fn handle(
    symbol: char,
    stream: TcpStream,
    game: Arc<Mutex<Game>>,
    tx: broadcast::Sender<String>,
) {
    let mut rx = tx.subscribe();
    let (read_half, mut write_half) = stream.into_split();
    let mut lines = BufReader::new(read_half).lines();

    // Greet the player and show the current board.
    {
        let g = game.lock().await;
        let greeting = format!("Welcome! You are player {symbol}.\r\n{}", render(&g.board));
        let _ = write_half.write_all(greeting.as_bytes()).await;
        let turn_msg = if g.turn == symbol {
            "Your turn — type a cell (1-9).\r\n".to_string()
        } else {
            format!("Waiting for player {} to move...\r\n", g.turn)
        };
        let _ = write_half.write_all(turn_msg.as_bytes()).await;
    }

    loop {
        // Wait for whichever happens first: a broadcast to display, or input to process.
        tokio::select! {
            // A board update was broadcast — write it to this client.
            msg = rx.recv() => {
                if let Ok(m) = msg {
                    if write_half.write_all(m.as_bytes()).await.is_err() {
                        break;
                    }
                }
            }

            // This client sent a line — treat it as a move.
            line = lines.next_line() => {
                let line = match line {
                    Ok(Some(l)) => l,
                    _ => break, // disconnected or error
                };
                let trimmed = line.trim().to_string();

                // Decide what to do while holding the lock, but do no I/O here so
                // the lock is released before we await any writes.
                let (reply, bcast) = {
                    let mut g = game.lock().await;
                    if g.over {
                        (Some("Game over.\r\n".to_string()), None)
                    } else if g.turn != symbol {
                        (Some(format!("Not your turn — waiting for {}.\r\n", g.turn)), None)
                    } else {
                        match trimmed.parse::<usize>() {
                            Ok(n) if (1..=9).contains(&n) && g.board[n - 1] == ' ' => {
                                g.board[n - 1] = symbol;
                                let status = if let Some(w) = winner(&g.board) {
                                    g.over = true;
                                    format!("Player {w} wins!\r\n")
                                } else if g.board.iter().all(|&c| c != ' ') {
                                    g.over = true;
                                    "It's a draw!\r\n".to_string()
                                } else {
                                    g.turn = if symbol == 'X' { 'O' } else { 'X' };
                                    format!("Player {}'s turn.\r\n", g.turn)
                                };
                                (None, Some(format!("{}{}", render(&g.board), status)))
                            }
                            Ok(n) if (1..=9).contains(&n) => {
                                (Some("That cell is taken, try another.\r\n".to_string()), None)
                            }
                            _ => (Some("Please type a number from 1 to 9.\r\n".to_string()), None),
                        }
                    }
                }; // lock dropped here

                // A valid move goes to everyone via the channel (including us, via rx).
                if let Some(m) = bcast {
                    println!("{symbol} played {trimmed}");
                    let _ = tx.send(m);
                }
                // An error reply goes only to this player.
                if let Some(r) = reply {
                    let _ = write_half.write_all(r.as_bytes()).await;
                }
            }
        }
    }
}

#[tokio::main]
async fn main() {
    let game = Arc::new(Mutex::new(Game {
        board: [' '; 9],
        turn: 'X',
        over: false,
    }));
    // Capacity 16 is plenty: a full game is only 9 moves.
    let (tx, _rx) = broadcast::channel::<String>(16);

    println!("Tic-tac-toe server (tokio) running:");
    println!("  Player X: telnet localhost 8000");
    println!("  Player O: telnet localhost 8001");

    let listener_x = TcpListener::bind("127.0.0.1:8000").await.unwrap();
    let listener_o = TcpListener::bind("127.0.0.1:8001").await.unwrap();

    // Accept one X and one O, each on its own task.
    let (gx, txx) = (game.clone(), tx.clone());
    let task_x = tokio::spawn(async move {
        let (sock, addr) = listener_x.accept().await.unwrap();
        println!("Player X connected from {addr}");
        handle('X', sock, gx, txx).await;
    });

    let (go, txo) = (game.clone(), tx.clone());
    let task_o = tokio::spawn(async move {
        let (sock, addr) = listener_o.accept().await.unwrap();
        println!("Player O connected from {addr}");
        handle('O', sock, go, txo).await;
    });

    let _ = tokio::join!(task_x, task_o);
}
