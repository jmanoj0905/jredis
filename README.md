# JRedis

A small in-memory key-value store in the shape of Redis. It takes TCP clients on virtual
threads, keeps data in a `ConcurrentHashMap` with TTL support, and appends every mutation to a
checksummed write-ahead log so the data survives a restart.

Strings only. No RESP, no pub/sub, no replication, no clustering. Three source files.

## Build and run

Needs JDK 21 or newer for virtual threads. Built and tested on Temurin 25.

```
javac *.java
java JRedisServer
```

Options:

```
--port N                              default 6380
--maxclients N                        default 1000
--timeout SECONDS                     idle client timeout, default 300
--wal PATH                            default jredis.wal
--appendfsync always|everysec|no      default everysec
```

The log is read back at startup, so restarting picks up where the last run left off.

## Commands

| Command | Reply |
|---|---|
| `SET key value` | `+OK`. Clears any TTL on the key, same as Redis. |
| `GET key` | the value, or `$-1` if missing or expired |
| `DEL key` | `:1` if it deleted something, `:0` otherwise |
| `EXISTS key` | `:1` / `:0` |
| `INCR key` | `:<new value>`, or an error if the value is not an integer or would overflow |
| `EXPIRE key seconds` | `:1` if the key exists, `:0` if not. Zero or negative deletes it. |
| `TTL key` | `:-2` no such key, `:-1` no expiry set, `:<n>` seconds left, rounded up |

Requests are inline and whitespace-delimited; replies use RESP type prefixes. Redis accepts
inline commands too, so the hybrid is not as odd as it looks, but it does mean values cannot
contain spaces and cannot be empty. Lines are capped at 64KB.

## Try it

```
$ printf 'SET greeting hello\r\nGET greeting\r\nINCR counter\r\nTTL greeting\r\n' | nc -w 1 localhost 6380
+OK
hello
:1
:-1
```

Or interactively with `nc localhost 6380` and type commands at it.

## How it holds together

`KeyValueStore` stores immutable entries and replaces them wholesale rather than mutating in
place - `ConcurrentHashMap` publishes the reference safely but gives you nothing for writes
made to the object afterwards. `INCR` goes through `compute` so the read-modify-write happens
under the key's bin lock instead of racing between two map calls. Expiry is lazy on every read
path plus a sweep every 30 seconds, because lazy alone leaks keys nobody reads again.

`WriteAheadLog` frames records as `<u32 length><u32 crc32><payload>`. Newline-delimited text
would not work: a torn write usually leaves something that still parses, just with the tail of
the value missing, and there is no way to notice. A single platform thread drains a queue and
appends, which both avoids interleaved writes and batches many records into one fsync. It is a
platform thread on purpose - JDK file I/O pins a virtual thread's carrier for the whole fsync,
unlike socket I/O.

Two rewrites happen before anything reaches the log. `EXPIRE key 3600` is written as
`PEXPIREAT key <absolute epoch ms>`, so replaying it after a crash does not hand the key a
fresh hour. `INCR` is written as a `SET` of the resulting value, because `INCR` is not
idempotent and the planned snapshot scheme can replay a record whose effect is already
captured. That leaves three record types in the log, all idempotent.

The sequence number is taken inside the same `compute` call as the mutation. Doing it outside
looks fine and is not: two clients setting the same key can enqueue in one order and apply in
the other, leaving memory disagreeing with the log it would recover from.

On startup the log is replayed record by record. A checksum failure on the last record means a
torn write and the file is truncated there; a checksum failure anywhere else is a hard error
and the server refuses to start rather than silently skipping a mutation.

`DESIGN.md` has the full reasoning, including the parts that were designed but not built
(snapshotting, replication).

## Benchmarks

Apple M1, 8 cores, 8GB, macOS 26.6.2, Temurin JDK 25.0.3, APFS on SSD. Loopback TCP, one
request and one reply per round trip - no pipelining.

Write path, 50 concurrent clients, 200,000 `SET`s:

| appendfsync | ops/sec |
|---|---|
| `no` | 110,299 |
| `everysec` | 109,385 |
| `always` | 6,397 |

That is the number worth having: `always` is 17x slower, and the gap is entirely the disk
round trip. `no` and `everysec` are indistinguishable because neither one makes a client wait
for a sync.

Reads and writes separately, `everysec`:

| | 1 client | 50 clients | 200 clients |
|---|---|---|---|
| `SET` | 21,013 | 109,385 | 110,669 |
| `GET` | 27,550 | 120,068 | 125,778 |

`GET` under `appendfsync=always` still does 105,533 ops/sec at 50 clients, since reads never
touch the log at all.

### Where it actually saturates

Not the WAL. Throughput is flat from 50 clients to 200, and identical between `no` and
`everysec`, which is what it looks like when the bottleneck is somewhere else. Driving the
store and log directly with 8 threads and no sockets in the way:

| | ops/sec |
|---|---|
| `SET`, `everysec` | 2,522,785 |
| `SET`, `no` | 2,455,677 |
| `GET`, `everysec` | 43,776,673 |
| `GET`, `no` | 31,775,059 |

So the storage layer has roughly 20x more headroom than the server ever asks of it, and the
bottleneck over TCP is the per-command socket round trip - one `read` and one `write` syscall
per operation, with no pipelining to amortize them. Request framing is what would have to
change first, not the log.

That in-process run is also where the read/write gap shows up honestly: 17x, because reads
take no lock and writes funnel through the single WAL writer. Over TCP the two look nearly
identical, and quoting only the TCP numbers as evidence that reads scale better would be
claiming something the measurement does not show.

`SET` under `always` in-process is 1,015 ops/sec with 8 threads - slower than the 6,397 the
server manages over TCP with 50 clients, because more concurrent writers means more records
per fsync. That is group commit doing its job.

## Known limitations

- The log grows forever. Snapshotting is designed in `DESIGN.md` §7 but not built.
- Values cannot contain spaces or be empty. Fixing that means length-prefixed framing on the
  wire, i.e. RESP.
- The 30-second sweep is a full scan, which is O(n) and will hurt on a large keyspace. Redis
  samples instead.
- `expiryAt` is wall-clock, so an NTP step backwards can make a key un-expire. `nanoTime` has
  no fixed epoch and is meaningless across the restart the log exists to survive, so wall-clock
  is the right call for a persisted deadline.
