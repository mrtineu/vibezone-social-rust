//! Mini social API: single-threaded epoll HTTP/1.1 server + SQLite (WAL, group commit).
//!
//! Durability: every write request in one event-loop batch runs inside one SQLite
//! transaction; responses for that batch are sent only after COMMIT returns.
//! Reads (`/feed`) are served from an in-memory cache of the newest 20 posts.

mod json;

use json::{esc, num, Field};
use rusqlite::{params, Connection};
use std::collections::VecDeque;
use std::time::{SystemTime, UNIX_EPOCH};

const MAX_BODY: usize = 16 * 1024;
const MAX_HEAD: usize = 8 * 1024;
const FEED_LEN: usize = 20;

// ---------------------------------------------------------------- app state

struct Recent {
    id: i64,
    /// `{"id":..,"author":..,"content":..,"created_at":..,"likes":`
    prefix: Vec<u8>,
}

struct App {
    db: Connection,
    max_id: i64,
    counts: Vec<u32>,
    recent: VecDeque<Recent>,
    feed: Vec<u8>,
    feed_dirty: bool,
    in_txn: bool,
}

fn fatal(what: &str, e: impl std::fmt::Display) -> ! {
    eprintln!("fatal: {what}: {e}");
    std::process::exit(1);
}

impl App {
    fn open(path: &str) -> App {
        let db = Connection::open(path).unwrap_or_else(|e| fatal("open db", e));
        db.busy_timeout(std::time::Duration::from_secs(20)).ok();
        // OFF: commits reach the kernel via write() before any reply is sent, so they
        // survive SIGKILL of the container; no fsync stalls the single event loop.
        let sync = std::env::var("SQLITE_SYNC").unwrap_or_else(|_| "OFF".into());
        let sync = match sync.to_ascii_uppercase().as_str() {
            "NORMAL" => "NORMAL",
            "FULL" => "FULL",
            _ => "OFF",
        };
        // EXCLUSIVE before WAL: the WAL index lives in heap memory, no -shm mmap.
        db.execute_batch(&format!(
            "PRAGMA locking_mode=EXCLUSIVE;
             PRAGMA journal_mode=WAL;
             PRAGMA synchronous={sync};
             PRAGMA temp_store=MEMORY;
             PRAGMA cache_size=-131072;
             CREATE TABLE IF NOT EXISTS posts(
               id INTEGER PRIMARY KEY,
               author TEXT NOT NULL,
               content TEXT NOT NULL,
               created_at TEXT NOT NULL);
             CREATE TABLE IF NOT EXISTS likes(
               post_id INTEGER NOT NULL,
               user TEXT NOT NULL,
               PRIMARY KEY(post_id, user)) WITHOUT ROWID;"
        ))
        .unwrap_or_else(|e| fatal("init db", e));

        let max_id: i64 = db
            .query_row("SELECT COALESCE(MAX(id),0) FROM posts", [], |r| r.get(0))
            .unwrap_or_else(|e| fatal("max id", e));
        let mut counts = vec![0u32; max_id as usize + 1];
        {
            let mut st = db
                .prepare("SELECT post_id, COUNT(*) FROM likes GROUP BY post_id")
                .unwrap_or_else(|e| fatal("count", e));
            let mut rows = st.query([]).unwrap_or_else(|e| fatal("count", e));
            while let Some(r) = rows.next().unwrap_or_else(|e| fatal("count", e)) {
                let id: i64 = r.get(0).unwrap();
                let c: i64 = r.get(1).unwrap();
                if id >= 1 && id <= max_id {
                    counts[id as usize] = c as u32;
                }
            }
        }
        let mut recent = VecDeque::with_capacity(FEED_LEN + 1);
        {
            let mut st = db
                .prepare("SELECT id, author, content, created_at FROM posts WHERE id > ?1 ORDER BY id")
                .unwrap_or_else(|e| fatal("recent", e));
            let mut rows = st.query([max_id - FEED_LEN as i64]).unwrap_or_else(|e| fatal("recent", e));
            while let Some(r) = rows.next().unwrap_or_else(|e| fatal("recent", e)) {
                let id: i64 = r.get(0).unwrap();
                let a: String = r.get(1).unwrap();
                let c: String = r.get(2).unwrap();
                let t: String = r.get(3).unwrap();
                recent.push_back(Recent { id, prefix: post_prefix(id, &a, &c, &t) });
            }
        }
        let mut app = App { db, max_id, counts, recent, feed: Vec::with_capacity(8192), feed_dirty: true, in_txn: false };
        app.build_feed();
        app
    }

    fn begin(&mut self) {
        if !self.in_txn {
            self.db.execute_batch("BEGIN").unwrap_or_else(|e| fatal("begin", e));
            self.in_txn = true;
        }
    }

    fn commit(&mut self) {
        if self.in_txn {
            self.db.execute_batch("COMMIT").unwrap_or_else(|e| fatal("commit", e));
            self.in_txn = false;
        }
    }

    fn build_feed(&mut self) {
        let f = &mut self.feed;
        f.clear();
        f.extend_from_slice(b"{\"posts\":[");
        for (k, r) in self.recent.iter().rev().enumerate() {
            if k > 0 {
                f.push(b',');
            }
            f.extend_from_slice(&r.prefix);
            num(f, self.counts[r.id as usize] as u64);
            f.push(b'}');
        }
        f.extend_from_slice(b"]}");
        self.feed_dirty = false;
    }

    fn feed(&mut self) -> &[u8] {
        if self.feed_dirty {
            self.build_feed();
        }
        &self.feed
    }

    fn create_post(&mut self, author: &str, content: &str, out: &mut Vec<u8>) {
        self.begin();
        let id = self.max_id + 1;
        let ts = now_iso();
        self.db
            .prepare_cached("INSERT INTO posts(id, author, content, created_at) VALUES(?1, ?2, ?3, ?4)")
            .and_then(|mut s| s.execute(params![id, author, content, ts]))
            .unwrap_or_else(|e| fatal("insert post", e));
        self.max_id = id;
        self.counts.push(0);
        debug_assert_eq!(self.counts.len() as i64, id + 1);
        let prefix = post_prefix(id, author, content, &ts);
        out.extend_from_slice(&prefix);
        out.extend_from_slice(b"0}");
        if self.recent.len() == FEED_LEN {
            self.recent.pop_front();
        }
        self.recent.push_back(Recent { id, prefix });
        self.feed_dirty = true;
    }

    /// Returns None when the post does not exist.
    fn like(&mut self, post_id: i64, user: &str) -> Option<u32> {
        if post_id > self.max_id {
            return None;
        }
        self.begin();
        let changed = self
            .db
            .prepare_cached("INSERT OR IGNORE INTO likes(post_id, user) VALUES(?1, ?2)")
            .and_then(|mut s| s.execute(params![post_id, user]))
            .unwrap_or_else(|e| fatal("insert like", e));
        let c = &mut self.counts[post_id as usize];
        if changed > 0 {
            *c += 1;
            if post_id > self.max_id - FEED_LEN as i64 {
                self.feed_dirty = true;
            }
        }
        Some(*c)
    }
}

fn post_prefix(id: i64, author: &str, content: &str, ts: &str) -> Vec<u8> {
    let mut p = Vec::with_capacity(96 + content.len());
    p.extend_from_slice(b"{\"id\":");
    num(&mut p, id as u64);
    p.extend_from_slice(b",\"author\":");
    esc(&mut p, author.as_bytes());
    p.extend_from_slice(b",\"content\":");
    esc(&mut p, content.as_bytes());
    p.extend_from_slice(b",\"created_at\":\"");
    p.extend_from_slice(ts.as_bytes());
    p.extend_from_slice(b"\",\"likes\":");
    p
}

fn now_iso() -> String {
    let ms = SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_millis() as i64).unwrap_or(0);
    let secs = ms.div_euclid(1000);
    let milli = ms.rem_euclid(1000);
    let days = secs.div_euclid(86400);
    let sod = secs.rem_euclid(86400);
    // Howard Hinnant's civil_from_days.
    let z = days + 719468;
    let era = z.div_euclid(146097);
    let doe = z - era * 146097;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = yoe + era * 400 + if m <= 2 { 1 } else { 0 };
    format!(
        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}.{:03}Z",
        y,
        m,
        d,
        sod / 3600,
        sod % 3600 / 60,
        sod % 60,
        milli
    )
}

// ---------------------------------------------------------------- validation

fn valid_name(f: &Field) -> Option<&str> {
    match f {
        Field::Str(s, true)
            if !s.is_empty() && s.len() <= 32 && s.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_') =>
        {
            Some(s)
        }
        _ => None,
    }
}

#[inline]
fn is_ws(c: char) -> bool {
    // Union of Python str.isspace() and JS String.prototype.trim().
    c.is_whitespace() || ('\u{1c}'..='\u{1f}').contains(&c) || c == '\u{feff}'
}

fn valid_content(f: &Field) -> Option<&str> {
    match f {
        Field::Str(s, true) => {
            let n = s.chars().count();
            if (1..=280).contains(&n) && !s.chars().all(is_ws) {
                Some(s)
            } else {
                None
            }
        }
        _ => None,
    }
}

// ---------------------------------------------------------------- HTTP

#[derive(Default)]
struct Conn {
    inbuf: Vec<u8>,
    out: Vec<u8>,
    out_pos: usize,
    close_after: bool,
    /// Half-close and drain input before closing (avoids RST eating an error reply).
    linger: bool,
    draining: bool,
    epollout: bool,
    pending: bool,
    continued: bool,
}

fn respond(out: &mut Vec<u8>, status: u16, body: &[u8], close: bool) {
    out.extend_from_slice(match status {
        200 => b"HTTP/1.1 200 OK\r\n" as &[u8],
        201 => b"HTTP/1.1 201 Created\r\n",
        400 => b"HTTP/1.1 400 Bad Request\r\n",
        404 => b"HTTP/1.1 404 Not Found\r\n",
        405 => b"HTTP/1.1 405 Method Not Allowed\r\n",
        _ => b"HTTP/1.1 500 Internal Server Error\r\n",
    });
    out.extend_from_slice(b"Content-Type: application/json\r\nContent-Length: ");
    num(out, body.len() as u64);
    if close {
        out.extend_from_slice(b"\r\nConnection: close");
    }
    out.extend_from_slice(b"\r\n\r\n");
    out.extend_from_slice(body);
}

fn err_body(msg: &str) -> Vec<u8> {
    let mut v = Vec::with_capacity(msg.len() + 12);
    v.extend_from_slice(b"{\"error\":");
    esc(&mut v, msg.as_bytes());
    v.push(b'}');
    v
}

fn find_head_end(b: &[u8]) -> Option<usize> {
    let mut i = 0;
    while let Some(off) = b[i..].iter().position(|&c| c == b'\n') {
        let j = i + off;
        if j >= 3 && &b[j - 3..=j] == b"\r\n\r\n" {
            return Some(j + 1);
        }
        i = j + 1;
    }
    None
}

#[inline]
fn trim(b: &[u8]) -> &[u8] {
    let s = b.iter().position(|c| !matches!(c, b' ' | b'\t')).unwrap_or(b.len());
    let e = b.iter().rposition(|c| !matches!(c, b' ' | b'\t')).map_or(s, |e| e + 1);
    &b[s..e.max(s)]
}

struct Head<'a> {
    method: &'a [u8],
    path: &'a [u8],
    content_length: usize,
    json: bool,
    keep_alive: bool,
    expect_continue: bool,
}

fn parse_head(h: &[u8]) -> Result<Head<'_>, &'static str> {
    let mut lines = h.split(|&c| c == b'\n').map(|l| l.strip_suffix(b"\r").unwrap_or(l));
    let rl = lines.next().ok_or("bad request line")?;
    let mut parts = rl.split(|&c| c == b' ').filter(|p| !p.is_empty());
    let method = parts.next().ok_or("bad request line")?;
    let target = parts.next().ok_or("bad request line")?;
    let version = parts.next().ok_or("bad request line")?;
    let http11 = match version {
        b"HTTP/1.1" => true,
        b"HTTP/1.0" => false,
        _ => return Err("unsupported HTTP version"),
    };
    let path = match target.iter().position(|&c| c == b'?') {
        Some(q) => &target[..q],
        None => target,
    };
    let mut hd = Head {
        method,
        path,
        content_length: 0,
        json: false,
        keep_alive: http11,
        expect_continue: false,
    };
    let mut seen_cl: Option<usize> = None;
    for line in lines {
        if line.is_empty() {
            continue;
        }
        let colon = line.iter().position(|&c| c == b':').ok_or("bad header")?;
        let name = &line[..colon];
        let value = trim(&line[colon + 1..]);
        if name.eq_ignore_ascii_case(b"content-length") {
            if value.is_empty() || !value.iter().all(u8::is_ascii_digit) || value.len() > 12 {
                return Err("bad content-length");
            }
            let n: usize = std::str::from_utf8(value).unwrap().parse().map_err(|_| "bad content-length")?;
            if seen_cl.is_some_and(|p| p != n) {
                return Err("bad content-length");
            }
            seen_cl = Some(n);
            hd.content_length = n;
        } else if name.eq_ignore_ascii_case(b"content-type") {
            let mime = trim(value.split(|&c| c == b';').next().unwrap_or(b""));
            let lower = mime.to_ascii_lowercase();
            hd.json = lower == b"application/json"
                || (lower.starts_with(b"application/") && lower.ends_with(b"+json"));
        } else if name.eq_ignore_ascii_case(b"connection") {
            let v = value.to_ascii_lowercase();
            if v.windows(5).any(|w| w == b"close") {
                hd.keep_alive = false;
            } else if v.windows(10).any(|w| w == b"keep-alive") {
                hd.keep_alive = true;
            }
        } else if name.eq_ignore_ascii_case(b"transfer-encoding") {
            return Err("transfer-encoding not supported");
        } else if name.eq_ignore_ascii_case(b"expect") {
            hd.expect_continue = value.eq_ignore_ascii_case(b"100-continue");
        }
    }
    Ok(hd)
}

/// Processes complete requests in `buf`; returns bytes consumed.
fn process(app: &mut App, c: &mut Conn, buf: &[u8], allow_keep: bool) -> usize {
    let mut pos = 0;
    while pos < buf.len() && !c.close_after {
        let b = &buf[pos..];
        let he = match find_head_end(b) {
            Some(he) if he <= MAX_HEAD + 4 => he,
            Some(_) => {
                bad_close(c, "request header too large");
                break;
            }
            None => {
                if b.len() > MAX_HEAD {
                    bad_close(c, "request header too large");
                }
                break;
            }
        };
        let hd = match parse_head(&b[..he - 4]) {
            Ok(h) => h,
            Err(m) => {
                bad_close(c, m);
                break;
            }
        };
        if hd.content_length > MAX_BODY {
            bad_close(c, "request body exceeds 16 KiB");
            break;
        }
        let total = he + hd.content_length;
        if b.len() < total {
            if hd.expect_continue && !c.continued {
                c.out.extend_from_slice(b"HTTP/1.1 100 Continue\r\n\r\n");
                c.continued = true;
            }
            break;
        }
        c.continued = false;
        let body = &b[he..total];
        let close = !(hd.keep_alive && allow_keep);
        route(app, c, &hd, body, close);
        pos += total;
        if close {
            c.close_after = true;
        }
    }
    pos
}

fn bad_close(c: &mut Conn, msg: &str) {
    respond(&mut c.out, 400, &err_body(msg), true);
    c.close_after = true;
    c.linger = true;
}

fn route(app: &mut App, c: &mut Conn, hd: &Head, body: &[u8], close: bool) {
    let out = &mut c.out;
    match hd.path {
        b"/feed" => {
            if hd.method == b"GET" {
                let f = app.feed();
                respond(out, 200, f, close);
            } else {
                respond(out, 405, b"{\"error\":\"method not allowed\"}", close);
            }
        }
        b"/posts" => {
            if hd.method != b"POST" {
                return respond(out, 405, b"{\"error\":\"method not allowed\"}", close);
            }
            if !hd.json {
                return respond(out, 400, b"{\"error\":\"Content-Type must be application/json\"}", close);
            }
            let Ok((a, ct)) = json::object2(body, "author", "content") else {
                return respond(out, 400, b"{\"error\":\"Expected a JSON object\"}", close);
            };
            let Some(author) = valid_name(&a) else {
                return respond(out, 400, b"{\"error\":\"author must match [a-zA-Z0-9_]{1,32}\"}", close);
            };
            let Some(content) = valid_content(&ct) else {
                return respond(
                    out,
                    400,
                    b"{\"error\":\"content must contain 1..280 characters and not be blank\"}",
                    close,
                );
            };
            let mut resp = Vec::with_capacity(128 + content.len());
            app.create_post(author, content, &mut resp);
            respond(out, 201, &resp, close);
        }
        b"/like" => {
            if hd.method != b"POST" {
                return respond(out, 405, b"{\"error\":\"method not allowed\"}", close);
            }
            if !hd.json {
                return respond(out, 400, b"{\"error\":\"Content-Type must be application/json\"}", close);
            }
            let Ok((pid, u)) = json::object2(body, "post_id", "user") else {
                return respond(out, 400, b"{\"error\":\"Expected a JSON object\"}", close);
            };
            let (Field::Int(post_id), Some(user)) = (&pid, valid_name(&u)) else {
                return respond(out, 400, b"{\"error\":\"Expected a positive integer post_id and a valid user\"}", close);
            };
            let post_id = *post_id;
            if post_id < 1 {
                return respond(out, 400, b"{\"error\":\"Expected a positive integer post_id and a valid user\"}", close);
            }
            match app.like(post_id, user) {
                None => respond(out, 404, b"{\"error\":\"Post not found\"}", close),
                Some(n) => {
                    let mut r = Vec::with_capacity(64);
                    r.extend_from_slice(b"{\"post_id\":");
                    num(&mut r, post_id as u64);
                    r.extend_from_slice(b",\"likes\":");
                    num(&mut r, n as u64);
                    r.extend_from_slice(b",\"liked\":true}");
                    respond(out, 200, &r, close);
                }
            }
        }
        b"/health" => respond(out, 200, b"{\"status\":\"ok\"}", close),
        _ => respond(out, 404, b"{\"error\":\"not found\"}", close),
    }
}

// ---------------------------------------------------------------- event loop

struct Server {
    ep: i32,
    lfd: i32,
    conns: Vec<Option<Box<Conn>>>,
    nconns: usize,
    hard_limit: usize,
    keep_limit: usize,
    keep_min: usize,
    paused: bool,
    scratch: Vec<u8>,
    pending: Vec<i32>,
    app: App,
}

fn errno() -> i32 {
    std::io::Error::last_os_error().raw_os_error().unwrap_or(0)
}

fn ep_ctl(ep: i32, op: i32, fd: i32, events: u32) {
    let mut ev = libc::epoll_event { events, u64: fd as u64 };
    unsafe { libc::epoll_ctl(ep, op, fd, &mut ev) };
}

impl Server {
    fn set_paused(&mut self, p: bool) {
        if self.paused != p {
            self.paused = p;
            ep_ctl(self.ep, libc::EPOLL_CTL_MOD, self.lfd, if p { 0 } else { libc::EPOLLIN as u32 });
        }
    }

    fn accept(&mut self) {
        for _ in 0..512 {
            if self.nconns >= self.hard_limit {
                self.set_paused(true);
                return;
            }
            let fd = unsafe {
                libc::accept4(self.lfd, std::ptr::null_mut(), std::ptr::null_mut(), libc::SOCK_NONBLOCK | libc::SOCK_CLOEXEC)
            };
            if fd < 0 {
                match errno() {
                    libc::EINTR | libc::ECONNABORTED => continue,
                    libc::EMFILE | libc::ENFILE | libc::ENOBUFS | libc::ENOMEM => {
                        self.set_paused(true);
                        return;
                    }
                    _ => return,
                }
            }
            let one: libc::c_int = 1;
            unsafe {
                libc::setsockopt(
                    fd,
                    libc::IPPROTO_TCP,
                    libc::TCP_NODELAY,
                    &one as *const _ as *const libc::c_void,
                    std::mem::size_of::<libc::c_int>() as u32,
                );
            }
            ep_ctl(self.ep, libc::EPOLL_CTL_ADD, fd, (libc::EPOLLIN | libc::EPOLLRDHUP) as u32);
            let i = fd as usize;
            if i >= self.conns.len() {
                self.conns.resize_with(i + 1, || None);
            }
            self.conns[i] = Some(Box::default());
            self.nconns += 1;
        }
    }

    fn close(&mut self, fd: i32) {
        if let Some(slot) = self.conns.get_mut(fd as usize) {
            if slot.take().is_some() {
                self.nconns -= 1;
            }
        }
        unsafe { libc::close(fd) };
        if self.paused && self.nconns + 16 < self.hard_limit {
            self.set_paused(false);
        }
    }

    fn readable(&mut self, fd: i32) {
        let Some(mut c) = self.conns.get_mut(fd as usize).and_then(|s| s.take()) else { return };
        let mut scratch = std::mem::take(&mut self.scratch);
        let cap = scratch.len();
        let keep = self.close_or_keep(fd, &mut c, &mut scratch, cap);
        self.scratch = scratch;
        if keep {
            if (!c.out.is_empty() || c.close_after) && !c.pending {
                c.pending = true;
                self.pending.push(fd);
            }
            self.conns[fd as usize] = Some(c);
        } else {
            self.conns[fd as usize] = Some(c);
            self.close(fd);
        }
    }

    /// Returns false if the connection must be closed now.
    fn close_or_keep(&mut self, fd: i32, c: &mut Conn, scratch: &mut [u8], cap: usize) -> bool {
        loop {
            let n = unsafe { libc::read(fd, scratch.as_mut_ptr() as *mut libc::c_void, cap) };
            if n == 0 {
                if c.draining {
                    return false;
                }
                c.close_after = true;
                c.linger = false;
                return !c.out.is_empty();
            }
            if n < 0 {
                let e = errno();
                return e == libc::EAGAIN || e == libc::EINTR;
            }
            let n = n as usize;
            if c.draining || c.close_after {
                if n == cap {
                    continue;
                }
                return true;
            }
            // Low concurrency: the server closes first, so one-shot clients (seeders, probes)
            // do not pile up TIME_WAIT sockets and exhaust their ephemeral ports.
            // High concurrency: keep-alive saves a TCP handshake per request, up to the fd budget.
            let allow_keep = self.nconns > self.keep_min && self.nconns <= self.keep_limit;
            if c.inbuf.is_empty() {
                let used = process(&mut self.app, c, &scratch[..n], allow_keep);
                if used < n && !c.close_after {
                    c.inbuf.extend_from_slice(&scratch[used..n]);
                }
            } else {
                c.inbuf.extend_from_slice(&scratch[..n]);
                let buf = std::mem::take(&mut c.inbuf);
                let used = process(&mut self.app, c, &buf, allow_keep);
                c.inbuf = buf;
                if c.close_after {
                    c.inbuf = Vec::new();
                } else {
                    c.inbuf.drain(..used);
                }
            }
            if n < cap {
                return true;
            }
        }
    }

    fn flush(&mut self, fd: i32) {
        enum Act {
            Keep,
            Close,
        }
        let ep = self.ep;
        let act = {
            let Some(c) = self.conns.get_mut(fd as usize).and_then(|s| s.as_mut()) else { return };
            c.pending = false;
            let mut act = None;
            while c.out_pos < c.out.len() {
                let rest = &c.out[c.out_pos..];
                let n = unsafe { libc::write(fd, rest.as_ptr() as *const libc::c_void, rest.len()) };
                if n > 0 {
                    c.out_pos += n as usize;
                    continue;
                }
                let e = errno();
                if n < 0 && e == libc::EINTR {
                    continue;
                }
                if n < 0 && e == libc::EAGAIN {
                    if !c.epollout {
                        c.epollout = true;
                        ep_ctl(ep, libc::EPOLL_CTL_MOD, fd, (libc::EPOLLIN | libc::EPOLLOUT | libc::EPOLLRDHUP) as u32);
                    }
                    act = Some(Act::Keep);
                } else {
                    act = Some(Act::Close);
                }
                break;
            }
            match act {
                Some(a) => a,
                None => {
                    c.out.clear();
                    c.out_pos = 0;
                    if c.out.capacity() > 256 * 1024 {
                        c.out = Vec::new();
                    }
                    if c.epollout {
                        c.epollout = false;
                        ep_ctl(ep, libc::EPOLL_CTL_MOD, fd, (libc::EPOLLIN | libc::EPOLLRDHUP) as u32);
                    }
                    if c.close_after && !c.draining {
                        if c.linger {
                            c.draining = true;
                            unsafe { libc::shutdown(fd, libc::SHUT_WR) };
                            Act::Keep
                        } else {
                            Act::Close
                        }
                    } else {
                        Act::Keep
                    }
                }
            }
        };
        if let Act::Close = act {
            self.close(fd);
        }
    }

    fn run(&mut self) -> ! {
        let mut events = vec![libc::epoll_event { events: 0, u64: 0 }; 1024];
        loop {
            let n = unsafe { libc::epoll_wait(self.ep, events.as_mut_ptr(), events.len() as i32, -1) };
            if n < 0 {
                if errno() == libc::EINTR {
                    continue;
                }
                fatal("epoll_wait", std::io::Error::last_os_error());
            }
            for ev in &events[..n as usize] {
                let fd = ev.u64 as i32;
                let flags = ev.events;
                if fd == self.lfd {
                    self.accept();
                    continue;
                }
                if flags & (libc::EPOLLIN | libc::EPOLLHUP | libc::EPOLLERR | libc::EPOLLRDHUP) as u32 != 0 {
                    self.readable(fd);
                }
                if flags & libc::EPOLLOUT as u32 != 0 {
                    if let Some(c) = self.conns.get_mut(fd as usize).and_then(|s| s.as_mut()) {
                        if !c.pending {
                            c.pending = true;
                            self.pending.push(fd);
                        }
                    }
                }
            }
            // Group commit: durable before any reply of this batch leaves the process.
            self.app.commit();
            let mut pending = std::mem::take(&mut self.pending);
            for &fd in &pending {
                self.flush(fd);
            }
            pending.clear();
            self.pending = pending;
        }
    }
}

extern "C" fn on_term(_: libc::c_int) {
    unsafe { libc::_exit(0) };
}

fn listen(port: u16) -> i32 {
    unsafe {
        let fd = libc::socket(libc::AF_INET, libc::SOCK_STREAM | libc::SOCK_NONBLOCK | libc::SOCK_CLOEXEC, 0);
        if fd < 0 {
            fatal("socket", std::io::Error::last_os_error());
        }
        let one: libc::c_int = 1;
        libc::setsockopt(
            fd,
            libc::SOL_SOCKET,
            libc::SO_REUSEADDR,
            &one as *const _ as *const libc::c_void,
            std::mem::size_of::<libc::c_int>() as u32,
        );
        let addr = libc::sockaddr_in {
            sin_family: libc::AF_INET as libc::sa_family_t,
            sin_port: port.to_be(),
            sin_addr: libc::in_addr { s_addr: 0 },
            sin_zero: [0; 8],
        };
        if libc::bind(fd, &addr as *const _ as *const libc::sockaddr, std::mem::size_of::<libc::sockaddr_in>() as u32) < 0 {
            fatal("bind", std::io::Error::last_os_error());
        }
        if libc::listen(fd, 4096) < 0 {
            fatal("listen", std::io::Error::last_os_error());
        }
        fd
    }
}

fn nofile_limit() -> usize {
    unsafe {
        let mut rl = libc::rlimit { rlim_cur: 0, rlim_max: 0 };
        if libc::getrlimit(libc::RLIMIT_NOFILE, &mut rl) == 0 {
            if rl.rlim_cur < rl.rlim_max {
                let up = libc::rlimit { rlim_cur: rl.rlim_max, rlim_max: rl.rlim_max };
                if libc::setrlimit(libc::RLIMIT_NOFILE, &up) == 0 {
                    rl.rlim_cur = rl.rlim_max;
                }
            }
            return (rl.rlim_cur as usize).min(1 << 20);
        }
    }
    4096
}

fn env_usize(name: &str) -> Option<usize> {
    std::env::var(name).ok().and_then(|v| v.parse().ok())
}

fn main() {
    unsafe {
        libc::signal(libc::SIGTERM, on_term as usize);
        libc::signal(libc::SIGINT, on_term as usize);
        libc::signal(libc::SIGPIPE, libc::SIG_IGN);
    }
    let port: u16 = std::env::var("PORT").ok().and_then(|p| p.parse().ok()).unwrap_or(8080);
    let dir = std::env::var("DATA_DIR").unwrap_or_else(|_| "/data".into());
    let _ = std::fs::create_dir_all(&dir);
    let app = App::open(&format!("{}/social.sqlite3", dir.trim_end_matches('/')));

    let nofile = nofile_limit();
    let hard_limit = env_usize("MAX_CONNS").unwrap_or(nofile.saturating_sub(64).clamp(64, 60_000));
    let keep_limit = env_usize("KEEPALIVE_CONNS").unwrap_or(hard_limit.saturating_sub(512).max(32));
    let keep_min = env_usize("KEEPALIVE_MIN_CONNS").unwrap_or(32);

    let lfd = listen(port);
    let ep = unsafe { libc::epoll_create1(libc::EPOLL_CLOEXEC) };
    if ep < 0 {
        fatal("epoll_create1", std::io::Error::last_os_error());
    }
    ep_ctl(ep, libc::EPOLL_CTL_ADD, lfd, libc::EPOLLIN as u32);
    let mut server = Server {
        ep,
        lfd,
        conns: Vec::with_capacity(hard_limit + 64),
        nconns: 0,
        hard_limit,
        keep_limit,
        keep_min,
        paused: false,
        scratch: vec![0u8; 64 * 1024],
        pending: Vec::with_capacity(1024),
        app,
    };
    server.run();
}
