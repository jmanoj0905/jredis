import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * In-memory key-value store with TTL support.
 *
 * Everything here is designed around one rule: the WAL record for a mutation is
 * created in the same critical section as the mutation itself. That's what keeps
 * log order and apply order the same, so replaying the log rebuilds exactly the
 * state that was in memory.
 */
public class KeyValueStore {

    /** Sentinel for "this key never expires". */
    static final long NO_EXPIRY = Long.MAX_VALUE;

    /**
     * Entries are immutable. We never modify one in place, we replace it.
     *
     * If we mutated the fields instead, a reader thread could see a stale value
     * forever - ConcurrentHashMap gives us a happens-before edge for the put/get
     * that publishes the reference, but nothing for writes to the object after
     * that. Making the fields final documents the intent and also stops the long
     * from tearing on a 32-bit VM.
     */
    private static final class Entry {
        final String value;
        final long expiryAt;

        Entry(String value, long expiryAt) {
            this.value = value;
            this.expiryAt = expiryAt;
        }
    }

    private final ConcurrentHashMap<String, Entry> store = new ConcurrentHashMap<>();
    private final ScheduledExecutorService sweeper;

    /** Null while we're replaying the log - replayed records must not be re-logged. */
    private WriteAheadLog wal;

    public KeyValueStore() {
        this.sweeper = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "expiry-sweeper");
            t.setDaemon(true);
            return t;
        });
    }

    /** Called once after replay finishes, before we start accepting clients. */
    public void attachLog(WriteAheadLog wal) {
        this.wal = wal;
    }

    public void startSweeper() {
        sweeper.scheduleAtFixedRate(this::sweep, 30, 30, TimeUnit.SECONDS);
    }

    public void shutdown() {
        sweeper.shutdownNow();
    }

    private static long now() {
        return System.currentTimeMillis();
    }

    private static boolean isExpired(Entry e) {
        return e.expiryAt <= now();
    }

    /**
     * EXPIRE key 9999999999999999 would wrap the multiply and land on a deadline in
     * the past, which reads as "delete immediately" - the exact opposite of what was
     * asked. Saturate instead.
     */
    private static long deadlineFor(long seconds) {
        try {
            return Math.addExact(now(), Math.multiplyExact(seconds, 1000L));
        } catch (ArithmeticException ex) {
            return NO_EXPIRY - 1;
        }
    }

    // ---------------------------------------------------------------- commands

    public String set(String key, String value) {
        long[] seq = new long[1];

        store.compute(key, (k, old) -> {
            // SET always clears any existing TTL, same as Redis.
            seq[0] = log("SET " + key + " " + value);
            return new Entry(value, NO_EXPIRY);
        });

        awaitDurable(seq[0]);
        return "+OK";
    }

    public String get(String key) {
        Entry e = store.get(key);
        if (e == null) {
            return "$-1";
        }
        if (isExpired(e)) {
            dropIfSame(key, e);
            return "$-1";
        }
        return e.value;
    }

    public String exists(String key) {
        Entry e = store.get(key);
        if (e == null) {
            return ":0";
        }
        if (isExpired(e)) {
            dropIfSame(key, e);
            return ":0";
        }
        return ":1";
    }

    public String del(String key) {
        boolean[] deleted = new boolean[1];
        long[] seq = new long[1];

        store.computeIfPresent(key, (k, e) -> {
            if (isExpired(e)) {
                // Already dead. Drop it, but report 0 and don't log - expiry is
                // never written to the log, it gets re-derived from the deadline.
                return null;
            }
            deleted[0] = true;
            seq[0] = log("DEL " + key);
            return null;
        });

        if (!deleted[0]) {
            return ":0";
        }
        awaitDurable(seq[0]);
        return ":1";
    }

    public String incr(String key) {
        long[] seq = new long[1];
        long[] result = new long[1];

        try {
            store.compute(key, (k, old) -> {
                // An expired-but-unswept key counts as absent. This has to gate
                // the expiry too, not just the value - otherwise we'd hand the
                // new entry a deadline that's already in the past and the key
                // would be dead the moment we created it.
                boolean absent = (old == null || isExpired(old));

                long n = absent ? 0 : Long.parseLong(old.value);
                long next = Math.addExact(n, 1);   // throws instead of wrapping
                result[0] = next;

                // Log the resolved value, not "INCR". INCR is deterministic but
                // not idempotent, and the snapshot design can replay a record
                // whose effect is already in the snapshot. A resolved SET can be
                // applied twice safely.
                seq[0] = log("SET " + key + " " + next);

                return new Entry(Long.toString(next), absent ? NO_EXPIRY : old.expiryAt);
            });
        } catch (NumberFormatException | ArithmeticException ex) {
            // Thrown inside the remapping function, so the mapping is untouched
            // and we bailed before the log() call. Nothing was written.
            return "-ERR value is not an integer or out of range";
        }

        awaitDurable(seq[0]);
        return ":" + result[0];
    }

    public String expire(String key, long seconds) {
        boolean[] applied = new boolean[1];
        long[] seq = new long[1];
        long deadline = deadlineFor(seconds);

        store.computeIfPresent(key, (k, e) -> {
            if (isExpired(e)) {
                return null;
            }
            applied[0] = true;

            if (seconds <= 0) {
                // Redis deletes immediately and still answers 1.
                seq[0] = log("DEL " + key);
                return null;
            }

            // Absolute deadline, never the relative offset. If we logged
            // "EXPIRE key 3600" and replayed it two hours after a crash, the key
            // would come back with a fresh hour to live.
            seq[0] = log("PEXPIREAT " + key + " " + deadline);
            return new Entry(e.value, deadline);
        });

        if (!applied[0]) {
            return ":0";
        }
        awaitDurable(seq[0]);
        return ":1";
    }

    public String ttl(String key) {
        Entry e = store.get(key);
        if (e == null) {
            return ":-2";                     // no such key
        }
        if (isExpired(e)) {
            dropIfSame(key, e);
            return ":-2";
        }
        if (e.expiryAt == NO_EXPIRY) {
            return ":-1";                     // exists, but immortal
        }
        long remainingMs = e.expiryAt - now();
        return ":" + ((remainingMs + 999) / 1000);   // round up, like Redis
    }

    // ------------------------------------------------------------ replay hooks

    /** Used by WriteAheadLog during startup. Does not log anything. */
    void applySet(String key, String value) {
        store.put(key, new Entry(value, NO_EXPIRY));
    }

    void applyDel(String key) {
        store.remove(key);
    }

    void applyExpireAt(String key, long deadline) {
        store.computeIfPresent(key, (k, e) -> new Entry(e.value, deadline));
    }

    // ---------------------------------------------------------------- internals

    private long log(String record) {
        if (wal == null) {
            return -1;                        // replaying, or persistence disabled
        }
        return wal.offer(record);             // non-blocking, safe under the bin lock
    }

    private void awaitDurable(long seq) {
        if (wal != null && seq >= 0) {
            wal.awaitDurable(seq);            // only actually blocks under appendfsync=always
        }
    }

    /**
     * Remove only if the entry is still the one we looked at. An unconditional
     * remove(key) would throw away a value written by a SET that landed between
     * our read and our delete.
     */
    private void dropIfSame(String key, Entry seen) {
        store.remove(key, seen);
    }

    /**
     * Periodic cleanup. Lazy expiry alone leaks: a key nobody ever reads again
     * sits in the map forever.
     *
     * This is a full scan, which is O(n) and will hurt on a large keyspace. Redis
     * samples 20 random keys ten times a second and repeats while more than a
     * quarter of the sample is expired. Worth switching to if the keyspace grows.
     */
    private void sweep() {
        // A task from scheduleAtFixedRate is silently cancelled if it throws, and
        // a server that quietly stops expiring keys is worse than one that dies.
        try {
            long cutoff = now();
            Iterator<Map.Entry<String, Entry>> it = store.entrySet().iterator();
            while (it.hasNext()) {
                Map.Entry<String, Entry> e = it.next();
                if (e.getValue().expiryAt <= cutoff) {
                    store.remove(e.getKey(), e.getValue());
                }
            }
        } catch (Exception ex) {
            System.err.println("sweep failed: " + ex);
        }
    }

    public int size() {
        return store.size();
    }
}
