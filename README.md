# Mini social API: Rust + SQLite

Single static binary (~2.6 MB image, `FROM scratch`), implementing the Vibe Zone API contract.

## Design

- **Server:** a single-threaded HTTP/1.1 server on a plain `epoll` loop (1 vCPU, so no thread contention). It doesn't use io_uring because gVisor/runsc doesn't support it.
- **Storage:** SQLite, which stays the only durable store, in `$DATA_DIR/social.sqlite3`. It uses WAL with `locking_mode=EXCLUSIVE`, so the WAL index lives in heap memory and there is no `-shm` mmap.
  - `posts(id INTEGER PRIMARY KEY, author, content, created_at)`
  - `likes(post_id, user, PRIMARY KEY(post_id,user)) WITHOUT ROWID`: idempotency comes from `INSERT OR IGNORE`.
- **Group commit:** all writes from one event-loop batch run in one transaction. 200/201 replies are written to sockets only after `COMMIT` returns, so every acknowledged write is already in the database file.
- **Reads:** like counts per post and the newest 20 posts are kept in memory. They are rebuilt from SQLite at startup. The feed JSON is cached and rebuilt only when a post is added or a top-20 post gets a like.
- **Connections:**
  - Keep-alive is on at normal load.
  - At very low concurrency (fewer than 32 connections), the server closes first, so one-shot clients don't run out of ephemeral ports.
  - Near the 4096-fd limit, it answers with `Connection: close`.
- **Validation** matches the reference starter: strict JSON, `application/json`, a 16 KiB body limit, Unicode code-point length, and blank checks after trimming.

## Run

```sh
docker build -t vibezone-social:local .
docker run --rm -p 8080:8080 -v social-data:/data vibezone-social:local
```

Optional environment variables:

| Variable | Default | Meaning |
| --- | --- | --- |
| `PORT` | 8080 | Listen port |
| `DATA_DIR` | /data | SQLite directory |
| `SQLITE_SYNC` | NORMAL | `OFF`, `NORMAL` or `FULL` |
| `KEEPALIVE_MIN_CONNS` | 32 | Keep-alive is off below this many open connections |
| `KEEPALIVE_CONNS` | nofile − 576 | Keep-alive is off above this many open connections |
| `MAX_CONNS` | nofile − 64 | Accepting pauses above this many open connections |
