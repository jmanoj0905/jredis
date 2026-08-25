import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.PriorityQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.zip.CRC32;

/**
 * Append-only write-ahead log.
 *
 * Records are framed as <u32 length><u32 crc32><payload>. Newline-delimited text
 * would not do: a torn write usually leaves something that still parses ("SET user al"
 * instead of "SET user alice"), and there is no way to notice.
 *
 * Only three record types ever reach the file - SET, DEL and PEXPIREAT - because the
 * store rewrites everything else into idempotent form before handing it over. Replaying
 * any suffix twice therefore lands on the same state.
 */
public class WriteAheadLog {

    public enum Policy { ALWAYS, EVERYSEC, NO }

    /** Anything bigger than this in the length field means the record is garbage. */
    private static final int MAX_RECORD = 64 * 1024 * 1024;

    private static final Rec POISON = new Rec(-1, new byte[0]);

    private record Rec(long seq, byte[] payload) {}

    private final File file;
    private final Policy policy;

    private final BlockingQueue<Rec> queue = new LinkedBlockingQueue<>();
    private final AtomicLong counter = new AtomicLong();

    private final ReentrantLock lock = new ReentrantLock();
    private final Condition durable = lock.newCondition();
    private long durableSeq;
    private boolean writerDead;

    private Thread writer;
    private DataOutputStream out;
    private FileChannel channel;

    public WriteAheadLog(String path, Policy policy) {
        this.file = new File(path);
        this.policy = policy;
    }

    public Policy policy() {
        return policy;
    }

    // ------------------------------------------------------------------- replay

    /**
     * Rebuild the store from the log. Returns the number of records applied.
     *
     * The last record can be torn - a crash caught the process mid-write. That is
     * survivable: truncate at the last known-good offset and boot. A checksum failure
     * anywhere else means the file is genuinely damaged and we refuse to start, because
     * silently skipping a record in the middle would hand back a state that never existed.
     */
    public int replayInto(KeyValueStore store) throws IOException {
        if (!file.exists() || file.length() == 0) {
            return 0;
        }

        long size = file.length();
        long good = 0;
        int applied = 0;
        boolean torn = false;
        CRC32 crc = new CRC32();

        try (DataInputStream in = new DataInputStream(
                new BufferedInputStream(new FileInputStream(file), 64 * 1024))) {

            while (good < size) {
                if (size - good < 8) {
                    torn = true;
                    break;
                }
                int len = in.readInt();
                int want = in.readInt();

                if (len <= 0 || len > MAX_RECORD || size - good - 8 < len) {
                    torn = true;
                    break;
                }

                byte[] payload = in.readNBytes(len);
                if (payload.length < len) {
                    torn = true;
                    break;
                }

                crc.reset();
                crc.update(payload);
                if ((int) crc.getValue() != want) {
                    if (good + 8 + len == size) {
                        torn = true;
                        break;
                    }
                    throw new IOException("WAL corrupt at offset " + good
                            + ": checksum mismatch on a record that is not the last one");
                }

                apply(store, new String(payload, StandardCharsets.UTF_8));
                good += 8 + len;
                applied++;
            }
        }

        if (torn) {
            System.err.println("wal: torn tail at offset " + good + ", dropping "
                    + (size - good) + " bytes");
            try (RandomAccessFile raf = new RandomAccessFile(file, "rw")) {
                raf.setLength(good);
            }
        }
        return applied;
    }

    private static void apply(KeyValueStore store, String rec) throws IOException {
        String[] p = rec.split(" ", 3);
        switch (p[0]) {
            case "SET" -> {
                if (p.length != 3) throw new IOException("bad SET record: " + rec);
                store.applySet(p[1], p[2]);
            }
            case "DEL" -> {
                if (p.length != 2) throw new IOException("bad DEL record: " + rec);
                store.applyDel(p[1]);
            }
            case "PEXPIREAT" -> {
                if (p.length != 3) throw new IOException("bad PEXPIREAT record: " + rec);
                store.applyExpireAt(p[1], Long.parseLong(p[2]));
            }
            default -> throw new IOException("unknown record type: " + rec);
        }
    }

    // -------------------------------------------------------------- append path

    public void start() throws IOException {
        FileOutputStream fos = new FileOutputStream(file, true);
        this.channel = fos.getChannel();
        this.out = new DataOutputStream(new BufferedOutputStream(fos, 64 * 1024));

        // Deliberately a platform thread. JDK file I/O does not unmount a virtual
        // thread the way socket I/O does, so a virtual writer would pin its carrier
        // for the whole duration of every fsync.
        this.writer = new Thread(this::runWriter, "wal-writer");
        writer.start();
    }

    /**
     * Called from inside the store's compute() remapping function, so it must not
     * block or touch the disk. An offer onto an unbounded queue does neither.
     *
     * The sequence number is taken here, in the same critical section as the mutation,
     * which is the only reason log order can be trusted to match apply order.
     */
    public long offer(String record) {
        long seq = counter.incrementAndGet();
        queue.offer(new Rec(seq, record.getBytes(StandardCharsets.UTF_8)));
        return seq;
    }

    /** Blocks until the record is on the platter. Only does anything under appendfsync=always. */
    public void awaitDurable(long seq) {
        if (policy != Policy.ALWAYS) {
            return;
        }
        lock.lock();
        try {
            while (durableSeq < seq && !writerDead) {
                durable.await();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            lock.unlock();
        }
    }

    public void close() {
        if (writer == null) {
            return;
        }
        queue.offer(POISON);
        try {
            writer.join(5000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void runWriter() {
        // Records can reach the queue out of sequence: two threads holding different bin
        // locks can be interleaved between taking their number and offering. So we buffer
        // by sequence and only write the next one we are owed.
        PriorityQueue<Rec> pending = new PriorityQueue<>((a, b) -> Long.compare(a.seq(), b.seq()));
        List<Rec> batch = new ArrayList<>();
        CRC32 crc = new CRC32();

        long nextSeq = 1;
        long lastSync = System.nanoTime();
        boolean unsynced = false;
        boolean stopping = false;

        try {
            while (true) {
                Rec first = queue.poll(100, TimeUnit.MILLISECONDS);
                if (first != null) {
                    batch.add(first);
                    queue.drainTo(batch);
                    for (Rec r : batch) {
                        if (r.seq() < 0) {
                            stopping = true;
                        } else {
                            pending.add(r);
                        }
                    }
                    batch.clear();
                }

                boolean wrote = false;
                while (!pending.isEmpty() && pending.peek().seq() == nextSeq) {
                    writeRec(crc, pending.poll());
                    nextSeq++;
                    wrote = true;
                }

                if (wrote) {
                    out.flush();      // into the OS page cache, still not durable
                    unsynced = true;
                }

                boolean sync = unsynced && switch (policy) {
                    case ALWAYS -> true;
                    case EVERYSEC -> System.nanoTime() - lastSync >= 1_000_000_000L;
                    case NO -> false;
                };
                if (sync) {
                    // force(true), not force(false): on a growing file the length is
                    // metadata, and without it the appended bytes can be unreachable
                    // after a crash even though they were synced.
                    channel.force(true);
                    lastSync = System.nanoTime();
                    unsynced = false;
                    publish(nextSeq - 1);
                }

                if (stopping && pending.isEmpty()) {
                    break;
                }
                if (stopping) {
                    // Someone took a sequence number and had not offered it yet when the
                    // shutdown pill arrived. Give it a moment; if it never shows, skip the
                    // hole rather than hanging the shutdown.
                    Rec late = queue.poll(50, TimeUnit.MILLISECONDS);
                    if (late != null && late.seq() >= 0) {
                        pending.add(late);
                    } else if (pending.peek().seq() != nextSeq) {
                        System.err.println("wal: no record for seq " + nextSeq + " at shutdown, skipping");
                        nextSeq = pending.peek().seq();
                    }
                }
            }

            out.flush();
            channel.force(true);
            publish(nextSeq - 1);
            out.close();
        } catch (Exception e) {
            System.err.println("wal writer died: " + e);
            lock.lock();
            try {
                writerDead = true;
                durable.signalAll();
            } finally {
                lock.unlock();
            }
        }
    }

    private void writeRec(CRC32 crc, Rec r) throws IOException {
        crc.reset();
        crc.update(r.payload());
        out.writeInt(r.payload().length);
        out.writeInt((int) crc.getValue());
        out.write(r.payload());
    }

    private void publish(long seq) {
        lock.lock();
        try {
            durableSeq = seq;
            durable.signalAll();
        } finally {
            lock.unlock();
        }
    }
}
