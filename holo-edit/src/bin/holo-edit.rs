//! holo-edit — the thinnest honest bridge between the NetHack
//! solver's Python store module (nh_store.py) and HoloPhext.
//!
//! nh_store.py speaks phext-edit's HTTP API and spawns its server
//! binary as `<bin> --local <file> --port <p>`. HoloPhext ships no
//! host surface at all (async Rust library only), so this binary
//! serves the same endpoints over `holo_phext::core::Core`:
//!
//!   GET  /api/status                 -> {file, scrolls, bytes, dirty, seq}
//!   GET  /api/scroll/<coord>         -> {coordinate, content, bytes, kind}
//!   POST /api/update {coordinate, content}  -> stage one op
//!   POST /api/save                   -> commit the staged batch as ONE
//!                                       HoloPhext transaction (WAL
//!                                       append + one sync, then visible)
//!   POST /api/checkpoint             -> fold WAL into the base file
//!   GET  /api/tree                   -> {total_scrolls, coordinates[]}
//!   GET  /api/zoom/z                 -> populated Z triples + counts
//!   GET  /api/zoom/y/<z.z.z>         -> populated Y triples under a Z
//!   GET  /api/zoom/x/<z.z.z>/<y.y.y> -> coordinates under a (Z, Y)
//!
//! Semantic differences from phext-edit, stated plainly:
//! * phext-edit applies /api/update to its overlay immediately and
//!   /api/save re-serializes the WHOLE file. Here, updates stage a
//!   transaction and save = one WAL commit: durable (synced) and
//!   atomic, at append cost instead of whole-file cost. The base
//!   file folds only at /api/checkpoint.
//! * An update with empty content stages a delete (the compat
//!   facade's write_scroll semantics: an empty scroll does not
//!   exist).
//! * Reads are snapshot reads over the core's shared state: a
//!   reader never observes a partial transaction.
//!
//! All phext coordinate handling goes through libphext
//! (`phext::to_coordinate`, `Coordinate`'s Display). No phext
//! content is parsed by hand anywhere in this binary; the only
//! hand-rolled parsing is HTTP and the small JSON request bodies,
//! which are not phext.

use holo_phext::core::{Core, Op, Shared, Txn};
use libphext::phext::{self, Coordinate};
use std::collections::BTreeMap;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

struct State {
    core: Mutex<Core>,
    shared: Arc<Shared>,
    staged: Mutex<Vec<(Coordinate, Op)>>,
    file: PathBuf,
}

// ---------------------------------------------------------------------
// minimal JSON (request bodies + response building). Not phext.
// ---------------------------------------------------------------------

fn json_escape(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 16);
    for ch in s.chars() {
        match ch {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if (c as u32) < 0x20 => {
                out.push_str(&format!("\\u{:04x}", c as u32))
            }
            c => out.push(c),
        }
    }
    out
}

/// Extract a string field from a flat JSON object body. The bodies
/// nh_store sends are `{"coordinate": "...", "content": "..."}` —
/// this is a small, strict string-field reader, not a JSON library.
fn json_string_field(body: &str, field: &str) -> Option<String> {
    let needle = format!("\"{}\"", field);
    let at = body.find(&needle)?;
    let rest = &body[at + needle.len()..];
    let rest = rest.trim_start();
    let rest = rest.strip_prefix(':')?.trim_start();
    let rest = rest.strip_prefix('"')?;
    let mut out = String::new();
    let mut chars = rest.chars();
    loop {
        match chars.next()? {
            '"' => return Some(out),
            '\\' => match chars.next()? {
                'n' => out.push('\n'),
                'r' => out.push('\r'),
                't' => out.push('\t'),
                'u' => {
                    let hex: String = chars.by_ref().take(4).collect();
                    let v = u32::from_str_radix(&hex, 16).ok()?;
                    out.push(char::from_u32(v)?);
                }
                other => out.push(other),
            },
            c => out.push(c),
        }
    }
}

// ---------------------------------------------------------------------
// HTTP plumbing
// ---------------------------------------------------------------------

struct Request {
    method: String,
    path: String,
    body: String,
}

fn read_request(reader: &mut BufReader<TcpStream>) -> Option<Request> {
    let mut request_line = String::new();
    if reader.read_line(&mut request_line).ok()? == 0 {
        return None;
    }
    let mut parts = request_line.split_whitespace();
    let method = parts.next()?.to_string();
    let path = parts.next()?.to_string();
    let mut content_length = 0usize;
    loop {
        let mut line = String::new();
        if reader.read_line(&mut line).ok()? == 0 {
            return None;
        }
        let line = line.trim_end();
        if line.is_empty() {
            break;
        }
        if let Some((name, value)) = line.split_once(':') {
            if name.trim().eq_ignore_ascii_case("content-length") {
                content_length = value.trim().parse().unwrap_or(0);
            }
        }
    }
    let mut body = vec![0u8; content_length];
    if content_length > 0 {
        reader.read_exact(&mut body).ok()?;
    }
    Some(Request {
        method,
        path,
        body: String::from_utf8_lossy(&body).into_owned(),
    })
}

fn respond(stream: &mut TcpStream, status: u16, body: &str) -> std::io::Result<()> {
    let reason = if status == 200 { "OK" } else { "Error" };
    // one buffer, one write: two small writes per response trip the
    // Nagle / delayed-ACK pair into a ~40 ms stall per request
    let mut out = format!(
        "HTTP/1.1 {} {}\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n",
        status,
        reason,
        body.len()
    )
    .into_bytes();
    out.extend_from_slice(body.as_bytes());
    stream.write_all(&out)?;
    stream.flush()
}

// ---------------------------------------------------------------------
// routes
// ---------------------------------------------------------------------

fn z_label(c: &Coordinate) -> String {
    format!("{}.{}.{}", c.z.library, c.z.shelf, c.z.series)
}

fn y_label(c: &Coordinate) -> String {
    format!("{}.{}.{}", c.y.collection, c.y.volume, c.y.book)
}

fn route(state: &State, req: &Request) -> (u16, String) {
    let ok = |body: String| (200, body);
    match (req.method.as_str(), req.path.as_str()) {
        ("GET", "/api/status") => {
            let snap = state.shared.snapshot();
            let bytes = std::fs::metadata(&state.file).map(|m| m.len()).unwrap_or(0);
            let dirty = !state.staged.lock().unwrap().is_empty();
            ok(format!(
                "{{\"file\": \"{}\", \"scrolls\": {}, \"bytes\": {}, \"dirty\": {}, \"seq\": {}, \"engine\": \"holo-phext\"}}",
                json_escape(&state.file.to_string_lossy()),
                snap.coordinates().len(),
                bytes,
                dirty,
                snap.seq()
            ))
        }
        ("GET", "/api/tree") => {
            let snap = state.shared.snapshot();
            let coords = snap.coordinates();
            let list: Vec<String> = coords.iter().map(|c| format!("\"{}\"", c)).collect();
            ok(format!(
                "{{\"total_scrolls\": {}, \"coordinates\": [{}]}}",
                coords.len(),
                list.join(", ")
            ))
        }
        ("POST", "/api/update") => {
            let coord_s = match json_string_field(&req.body, "coordinate") {
                Some(s) => s,
                None => return (400, "{\"ok\": false, \"message\": \"no coordinate\"}".into()),
            };
            let content = json_string_field(&req.body, "content").unwrap_or_default();
            if coord_s.len() > 40
                || !coord_s.chars().all(|c| c.is_ascii_digit() || c == '.' || c == '/')
            {
                return (400, "{\"ok\": false, \"message\": \"bad coordinate\"}".into());
            }
            let coord = phext::to_coordinate(&coord_s);
            let op = if content.is_empty() {
                Op::Delete
            } else {
                Op::Set(content)
            };
            state.staged.lock().unwrap().push((coord, op));
            ok("{\"ok\": true}".into())
        }
        ("POST", "/api/save") => {
            let ops: Vec<(Coordinate, Op)> =
                std::mem::take(&mut *state.staged.lock().unwrap());
            if ops.is_empty() {
                return ok(format!(
                    "{{\"ok\": true, \"seq\": {}}}",
                    state.shared.seq()
                ));
            }
            let mut core = state.core.lock().unwrap();
            match core.commit_batch(vec![Txn { ops }]) {
                Ok(seqs) => ok(format!("{{\"ok\": true, \"seq\": {}}}", seqs[0])),
                Err(e) => (500, format!(
                    "{{\"ok\": false, \"message\": \"{}\"}}",
                    json_escape(&e.to_string())
                )),
            }
        }
        ("POST", "/api/checkpoint") => {
            let mut core = state.core.lock().unwrap();
            match core.checkpoint() {
                Ok(()) => ok("{\"ok\": true}".into()),
                Err(e) => (500, format!(
                    "{{\"ok\": false, \"message\": \"{}\"}}",
                    json_escape(&e.to_string())
                )),
            }
        }
        ("GET", "/api/zoom/z") => {
            let snap = state.shared.snapshot();
            let mut counts: BTreeMap<String, usize> = BTreeMap::new();
            for c in snap.coordinates() {
                *counts.entry(z_label(&c)).or_default() += 1;
            }
            let entries: Vec<String> = counts
                .iter()
                .map(|(l, n)| format!("{{\"label\": \"{}\", \"scroll_count\": {}}}", l, n))
                .collect();
            ok(format!("{{\"entries\": [{}]}}", entries.join(", ")))
        }
        _ => {
            if let Some(coord_s) = req.path.strip_prefix("/api/scroll/") {
                if req.method != "GET"
                    || coord_s.len() > 40
                    || !coord_s.chars().all(|c| c.is_ascii_digit() || c == '.' || c == '/')
                {
                    return (400, "{\"ok\": false, \"message\": \"bad scroll request\"}".into());
                }
                let coord = phext::to_coordinate(coord_s);
                let snap = state.shared.snapshot();
                let content = snap.get(&coord).unwrap_or_default();
                return ok(format!(
                    "{{\"coordinate\": \"{}\", \"content\": \"{}\", \"bytes\": {}, \"kind\": \"text\"}}",
                    coord,
                    json_escape(&content),
                    content.len()
                ));
            }
            if let Some(z_s) = req.path.strip_prefix("/api/zoom/y/") {
                if req.method != "GET" {
                    return (400, "{\"ok\": false}".into());
                }
                let snap = state.shared.snapshot();
                let mut counts: BTreeMap<String, usize> = BTreeMap::new();
                for c in snap.coordinates() {
                    if z_label(&c) == z_s {
                        *counts.entry(y_label(&c)).or_default() += 1;
                    }
                }
                let entries: Vec<String> = counts
                    .iter()
                    .map(|(l, n)| {
                        format!("{{\"label\": \"{}\", \"scroll_count\": {}}}", l, n)
                    })
                    .collect();
                return ok(format!("{{\"entries\": [{}]}}", entries.join(", ")));
            }
            if let Some(rest) = req.path.strip_prefix("/api/zoom/x/") {
                if req.method != "GET" {
                    return (400, "{\"ok\": false}".into());
                }
                let (z_s, y_s) = match rest.split_once('/') {
                    Some(pair) => pair,
                    None => return (400, "{\"ok\": false}".into()),
                };
                let snap = state.shared.snapshot();
                let entries: Vec<String> = snap
                    .coordinates()
                    .iter()
                    .filter(|c| z_label(c) == z_s && y_label(c) == y_s)
                    .map(|c| format!("{{\"coordinate\": \"{}\"}}", c))
                    .collect();
                return ok(format!("{{\"entries\": [{}]}}", entries.join(", ")));
            }
            (404, "{\"ok\": false, \"message\": \"no such route\"}".into())
        }
    }
}

fn handle(stream: TcpStream, state: Arc<State>) {
    let _ = stream.set_nodelay(true);
    let mut reader = BufReader::new(stream.try_clone().unwrap());
    let mut out = stream;
    while let Some(req) = read_request(&mut reader) {
        let (status, body) = route(&state, &req);
        if respond(&mut out, status, &body).is_err() {
            return;
        }
    }
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let mut file: Option<PathBuf> = None;
    let mut port: u16 = 8940;
    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--local" => {
                i += 1;
            }
            "--port" => {
                port = args.get(i + 1).and_then(|s| s.parse().ok()).unwrap_or(port);
                i += 2;
            }
            other => {
                if file.is_none() {
                    file = Some(PathBuf::from(other));
                }
                i += 1;
            }
        }
    }
    let raw = file.expect("usage: holo-edit --local <file> --port <p>");
    let file = if raw.is_absolute() {
        raw
    } else {
        std::env::current_dir().unwrap().join(raw)
    };
    if !file.exists() {
        eprintln!("store file must exist: {}", file.display());
        std::process::exit(2);
    }
    let core = Core::open(&file).expect("open holo-phext core");
    let shared = core.shared.clone();
    let state = Arc::new(State {
        core: Mutex::new(core),
        shared,
        staged: Mutex::new(Vec::new()),
        file: file.clone(),
    });
    let listener = TcpListener::bind(("127.0.0.1", port)).expect("bind");
    eprintln!("holo-edit serving {} on 127.0.0.1:{}", file.display(), port);
    for stream in listener.incoming() {
        match stream {
            Ok(s) => {
                let st = state.clone();
                std::thread::spawn(move || handle(s, st));
            }
            Err(_) => continue,
        }
    }
}
