#[macro_use]
extern crate log;
extern crate env_logger;
extern crate time;
extern crate r2d2;
extern crate r2d2_sqlite;
extern crate rusqlite;
use std::str::from_utf8;
use std::io::BufReader;
use std::io::BufRead;
use std::fs;
use std::path::Path;
use std::os::unix::prelude::OsStrExt;
use std::sync::Arc;
use std::thread::Builder;
use r2d2_sqlite::SqliteConnectionManager;
use rusqlite::SqliteError;


fn on_log(log: &Path, channel_id: i64,  pool: Arc<r2d2::Pool<SqliteConnectionManager>>) {
    let datestr = from_utf8(log.file_stem().unwrap().as_bytes()).unwrap().to_string();
    let date = time::strptime(&datestr, "%Y-%m-%d").unwrap();
    let file = match fs::File::open(log) {
        Ok(f) => f,
        Err(_) => {
            error!("could not open {}; skipping.", log.display());
            return;
        }
    };
    let br = BufReader::new(&file);
    for line in br.lines() {
        let line = match line {
            Ok(l) => l,
            Err(e) => {
                warn!("ignoring error {}", e);
                continue;
            }
        };
        if line.len() < 10 {
            warn!("ignoring line {}", line);
            continue;
        }
        let mut time = match time::strptime(&line[0..8], "%H:%M:%S") {
            Ok(t) => t,
            Err(e) => {
                warn!("Parse error {}; ignoring", e);
                continue;

            }
        };
        time.tm_mday = date.tm_mday;
        time.tm_mon = date.tm_mon;
        time.tm_year = date.tm_year;
        let created_at = time.to_timespec();
        let msg = &line[9..];
        let (user, type_, body) = match &msg[0..1] {
            "!" => ("server", "sysmsg", &msg[1..]),
            "+" => ("server", "join", &msg[1..]),
            "-" => ("server", "part", &msg[1..]),
            "<" => match msg.find('>').map(|e| (&msg[1..e], &msg[e+1..])) {
                Some((user, body)) => (user, "msg", body),
                None => {
                    warn!("cannot parse the entry; skipping");
                    continue;
                }
            },
            _ => ("server", "notice", &msg[1..]),
        };
        let conn = pool.get().unwrap();
        let user_id = match conn.execute("INSERT INTO users (name) VALUES ($1)", &[&user]) {
            // unique constraint failed
            Err(SqliteError{code: 19, message:_}) => conn.query_row("SELECT id FROM users WHERE name = $1", &[&user], |r| r.get(0)).unwrap(),
            Ok(_) => conn.last_insert_rowid(),
            e => {e.unwrap(); return}
        };
        conn.execute("INSERT INTO entries (channel_id, user_id, type, body, created_at) VALUES ($1, $2, $3, $4, $5)", &[&channel_id, &user_id, &type_, &body, &created_at]).unwrap();
    }
}

fn on_channel_dir(path: &Path, pool: Arc<r2d2::Pool<SqliteConnectionManager>>) {
    let dirname = from_utf8(path.file_name().unwrap().as_bytes()).unwrap().to_string();
    let at = match dirname.find('@') {
        Some(i) => i,
        None => return
    };
    let channel = &dirname[..at];
    let server = &dirname[at+1..];
    let conn = pool.get().unwrap();
    let server_id = match conn.execute("INSERT INTO servers (name) VALUES ($1)", &[&server]) {
        // unique constraint failed
        Err(SqliteError{code: 19, message:_}) => conn.query_row("SELECT id FROM servers WHERE name = $1", &[&server], |r| r.get(0)).unwrap(),
        Ok(_) => conn.last_insert_rowid(),
        e => {e.unwrap();return}
    };
    let channel_id = match conn.execute("INSERT INTO channels (name, server_id) VALUES ($1, $2)", &[&channel, &server_id]) {
        // unique constraint failed
        Err(SqliteError{code: 19, message:_}) => conn.query_row("SELECT id FROM channels WHERE name = $1", &[&channel], |r| r.get(0)).unwrap(),
        Ok(_) => conn.last_insert_rowid(),
        e => {e.unwrap(); return}
        
    };
    println!("{} at {}", channel, server);
    let logs = fs::read_dir(path).unwrap();
    for log in logs {
        let log = log.unwrap().path();
        let pathname = log.to_string_lossy().to_string();
        let pool_ = pool.clone();
        let _ = Builder::new().name(pathname).spawn(move|| on_log(&log, channel_id, pool_)).unwrap().join();
    }
}


fn main(){
    env_logger::init().unwrap();
    let manager = SqliteConnectionManager::new("test.db").unwrap();
    let config = r2d2::Config::builder().pool_size(1).build();
    let pool = Arc::new(r2d2::Pool::new(config, manager).unwrap());
    let paths = fs::read_dir("/home/kim/log").unwrap();
    for path in paths {
        let path = path.unwrap().path();
        on_channel_dir(&path, pool.clone());
    }
}
