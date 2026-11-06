use futures::executor::block_on;
use n2x_migration_lab::Lab;
use serde_json::{Value, json};
use std::{
    io::{Read, Write},
    net::{TcpListener, TcpStream},
    path::PathBuf,
};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);
    let mut port = 8427u16;
    let mut open = true;
    let mut data = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("data");
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--port" => port = args.next().ok_or("missing port")?.parse()?,
            "--data" => data = args.next().ok_or("missing data path")?.into(),
            "--no-open" => open = false,
            _ => return Err(format!("unknown argument {arg}").into()),
        }
    }
    let mut lab = Some(Lab::open(data)?);
    let listener = TcpListener::bind(("127.0.0.1", port))?;
    let url = format!("http://127.0.0.1:{port}");
    println!(
        "n2x migration lab: {url}\ndata: {}",
        lab.as_ref().unwrap().root.display()
    );
    if open {
        let _ = std::process::Command::new(if cfg!(target_os = "macos") {
            "open"
        } else {
            "xdg-open"
        })
        .arg(&url)
        .spawn();
    }
    for stream in listener.incoming() {
        match stream {
            Ok(mut stream) => {
                if let Err(error) = serve(&mut stream, &mut lab) {
                    eprintln!("request: {error}");
                }
            }
            Err(error) => eprintln!("accept: {error}"),
        }
    }
    Ok(())
}
fn serve(stream: &mut TcpStream, lab: &mut Option<Lab>) -> Result<(), Box<dyn std::error::Error>> {
    stream.set_read_timeout(Some(std::time::Duration::from_secs(5)))?;
    let mut bytes = Vec::new();
    let header_end = loop {
        let mut buffer = [0; 4096];
        let count = stream.read(&mut buffer)?;
        if count == 0 {
            return Ok(());
        }
        bytes.extend_from_slice(&buffer[..count]);
        if bytes.len() > 65536 {
            return Err("request too large".into());
        }
        if let Some(end) = bytes.windows(4).position(|part| part == b"\r\n\r\n") {
            break end + 4;
        }
    };
    let headers = String::from_utf8_lossy(&bytes[..header_end]).to_string();
    let mut first = headers
        .lines()
        .next()
        .ok_or("empty request")?
        .split_whitespace();
    let method = first.next().unwrap_or("");
    let path = first.next().unwrap_or("");
    let length = headers
        .lines()
        .find_map(|line| {
            line.split_once(':')
                .filter(|(key, _)| key.eq_ignore_ascii_case("content-length"))
                .and_then(|(_, value)| value.trim().parse::<usize>().ok())
        })
        .unwrap_or(0);
    if length > 16384 {
        return Err("body too large".into());
    }
    while bytes.len() < header_end + length {
        let mut buffer = [0; 4096];
        let count = stream.read(&mut buffer)?;
        if count == 0 {
            return Err("truncated body".into());
        }
        bytes.extend_from_slice(&buffer[..count]);
    }
    if method == "GET" && path == "/" {
        return response(
            stream,
            200,
            "text/html; charset=utf-8",
            include_bytes!("../web/index.html"),
        );
    }
    let input: Value = if length > 0 {
        serde_json::from_slice(&bytes[header_end..header_end + length])?
    } else {
        json!({})
    };
    let result = block_on(async {
        if lab.is_none() && !matches!(path, "/api/reopen" | "/api/reset") {
            return Err(n2x_migration_lab::fixtures::err(
                "lab.unavailable",
                "reopen the lab after the previous storage failure",
            ));
        }
        match (method, path) {
            ("GET", "/api/state") => lab.as_mut().unwrap().state().await,
            ("POST", "/api/start") => {
                lab.as_mut()
                    .unwrap()
                    .start(input["target"].as_str().unwrap_or("v3"))
                    .await
            }
            ("POST", "/api/tick") => lab.as_mut().unwrap().tick().await,
            ("POST", "/api/step") => lab.as_mut().unwrap().step().await,
            ("POST", "/api/run") => {
                lab.as_mut()
                    .unwrap()
                    .run_to(input["target"].as_str().unwrap_or("v3"))
                    .await
            }
            ("POST", "/api/resume") => lab.as_mut().unwrap().resume().await,
            ("POST", "/api/fault") => {
                lab.as_ref().unwrap().fault.set(true);
                lab.as_mut().unwrap().state().await
            }
            ("POST", "/api/reopen") => {
                let previous = lab.take().ok_or_else(|| {
                    n2x_migration_lab::fixtures::err(
                        "lab.unavailable",
                        "restart the service to reopen storage",
                    )
                })?;
                *lab = Some(previous.reopen()?);
                lab.as_mut().unwrap().state().await
            }
            ("POST", "/api/reset") => {
                // Only this explicitly invoked test-fixture endpoint removes generated lab data.
                let previous = lab.take().ok_or_else(|| {
                    n2x_migration_lab::fixtures::err(
                        "lab.unavailable",
                        "restart the service to reset storage",
                    )
                })?;
                let root = previous.root.clone();
                drop(previous);
                std::fs::remove_dir_all(&root).map_err(n2x_migration_lab::fixtures::io)?;
                *lab = Some(Lab::open(root)?);
                lab.as_mut().unwrap().state().await
            }
            _ => Err(n2x_migration_lab::fixtures::err(
                "lab.http.not_found",
                "unknown route",
            )),
        }
    });
    let (status, body) = match result {
        Ok(state) => (200, json!({"ok":true,"state":state})),
        Err(error) => (
            409,
            json!({"ok":false,"error":error,"state":lab.as_mut().and_then(|lab| block_on(lab.state()).ok())}),
        ),
    };
    response(
        stream,
        status,
        "application/json; charset=utf-8",
        &serde_json::to_vec(&body)?,
    )
}
fn response(
    stream: &mut TcpStream,
    status: u16,
    kind: &str,
    body: &[u8],
) -> Result<(), Box<dyn std::error::Error>> {
    write!(
        stream,
        "HTTP/1.1 {status} {}\r\nContent-Type: {kind}\r\nContent-Length: {}\r\nCache-Control: no-store\r\nX-Content-Type-Options: nosniff\r\nConnection: close\r\n\r\n",
        if status == 200 { "OK" } else { "Conflict" },
        body.len()
    )?;
    stream.write_all(body)?;
    Ok(())
}
