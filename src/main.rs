use std::fs::OpenOptions;
use std::io::Write;
use std::net::UdpSocket;
use std::time::SystemTime;

use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
struct LogRow {
    file_name: String,
    message: String,
}

fn main() -> std::io::Result<()> {
    let socket = UdpSocket::bind("0.0.0.0:9999")?;
    let mut buf = [0; 4096];
    let mut i = 0;
    let mut t0 = SystemTime::now();
    loop {
        let (n, remote_addr) = socket.recv_from(&mut buf)?;
        match serde_json::from_slice::<LogRow>(&buf[..n]) {
            Ok(row) => {
                if n > 0 {
                    {
                        let mut f = OpenOptions::new().create(true).append(true).open(&row.file_name)?;
                        f.write(row.message.as_ref())?;
                    }
                    socket.send_to(b"ok", remote_addr)?;
                    i += 1;
                    if i % 10000 == 1 {
                        let t1 = SystemTime::now();
                        let pps = (10000 as f64 / t1.duration_since(t0).unwrap().as_secs_f64()) as i32;
                        t0 = t1;
                        println!("Count: {}. Remote IP: {} . File: {} . Message: {}. PPS: {}", i, remote_addr, row.file_name, row.message, pps);
                    }
                }
            }
            Err(e) => {
                eprintln!("JSON decode Failed. Error: {}. Content: {}", e, buf[..n])
            }
        }
    }
}
