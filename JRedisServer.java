import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * TCP front end. One virtual thread per connection.
 *
 * A blocking socket read does not pin the carrier - the JDK routes socket I/O through
 * its non-blocking layer and unmounts the virtual thread - which is what makes
 * thread-per-connection affordable here. File I/O is the exception, and that is why the
 * WAL writer is a platform thread instead.
 */
public class JRedisServer {

    /**
     * Requests are inline and whitespace-delimited, so there is no framing and no way to
     * know where a huge payload ends. Cap the line so a client cannot OOM the server.
     */
    private static final int MAX_LINE = 64 * 1024;

    private static final class LineTooLong extends IOException {
        private static final long serialVersionUID = 1L;

        LineTooLong() {
            super("inline request over " + MAX_LINE + " bytes");
        }
    }

    private final KeyValueStore store;
    private final int port;
    private final int maxClients;
    private final int idleTimeoutMs;
    private final AtomicInteger connections = new AtomicInteger();

    JRedisServer(KeyValueStore store, int port, int maxClients, int idleTimeoutMs) {
        this.store = store;
        this.port = port;
        this.maxClients = maxClients;
        this.idleTimeoutMs = idleTimeoutMs;
    }

    void run() throws IOException {
        try (ServerSocket server = new ServerSocket(port);
             ExecutorService pool = Executors.newVirtualThreadPerTaskExecutor()) {

            System.out.println("jredis listening on " + port + ", maxclients " + maxClients);

            while (true) {
                Socket sock = server.accept();
                // Cheap to hold a virtual thread and an FD; the FD is the scarce one, so
                // the cap is on connections rather than threads. Rejecting explicitly beats
                // letting accept() fail server-wide once the ulimit runs out.
                if (connections.incrementAndGet() > maxClients) {
                    pool.submit(() -> reject(sock));
                    continue;
                }
                pool.submit(() -> handle(sock));
            }
        }
    }

    private void reject(Socket sock) {
        try (sock) {
            sock.getOutputStream().write("-ERR max number of clients reached\r\n"
                    .getBytes(StandardCharsets.UTF_8));
        } catch (IOException ignored) {
            // client is going away either way
        } finally {
            connections.decrementAndGet();
        }
    }

    private void handle(Socket sock) {
        try (sock;
             InputStream in = new BufferedInputStream(sock.getInputStream());
             OutputStream out = new BufferedOutputStream(sock.getOutputStream())) {

            sock.setTcpNoDelay(true);
            while (true) {
                String line;
                try {
                    line = readLine(sock, in);
                } catch (LineTooLong e) {
                    write(out, "-ERR protocol error: too big inline request");
                    break;
                }
                if (line == null) {
                    break;
                }
                if (line.isBlank()) {
                    continue;
                }
                write(out, dispatch(line));
            }
        } catch (SocketTimeoutException e) {
            // idle past the timeout, drop it and give the FD back
        } catch (IOException e) {
            // client vanished mid-command; nothing useful to do
        } finally {
            connections.decrementAndGet();
        }
    }

    private static void write(OutputStream out, String reply) throws IOException {
        out.write(reply.getBytes(StandardCharsets.UTF_8));
        out.write('\r');
        out.write('\n');
        out.flush();
    }

    /**
     * The read timeout is armed only while waiting for the first byte of a command.
     * Leaving it armed mid-line would kill a client that is merely slow at sending a
     * large value, which is a different thing from an idle client.
     */
    private String readLine(Socket sock, InputStream in) throws IOException {
        sock.setSoTimeout(idleTimeoutMs);
        int c = in.read();
        if (c == -1) {
            return null;
        }
        sock.setSoTimeout(0);

        ByteArrayOutputStream buf = new ByteArrayOutputStream(64);
        while (c != -1 && c != '\n') {
            if (c != '\r') {
                if (buf.size() >= MAX_LINE) {
                    throw new LineTooLong();
                }
                buf.write(c);
            }
            c = in.read();
        }
        return buf.toString(StandardCharsets.UTF_8);
    }

    private String dispatch(String line) {
        String[] p = line.trim().split("\\s+");
        String cmd = p[0].toUpperCase();
        int argc = p.length - 1;

        switch (cmd) {
            case "SET":
                if (argc != 2) return wrongArgs("set");
                return store.set(p[1], p[2]);
            case "GET":
                if (argc != 1) return wrongArgs("get");
                return store.get(p[1]);
            case "DEL":
                if (argc != 1) return wrongArgs("del");
                return store.del(p[1]);
            case "EXISTS":
                if (argc != 1) return wrongArgs("exists");
                return store.exists(p[1]);
            case "INCR":
                if (argc != 1) return wrongArgs("incr");
                return store.incr(p[1]);
            case "EXPIRE":
                if (argc != 2) return wrongArgs("expire");
                try {
                    return store.expire(p[1], Long.parseLong(p[2]));
                } catch (NumberFormatException e) {
                    return "-ERR value is not an integer or out of range";
                }
            case "TTL":
                if (argc != 1) return wrongArgs("ttl");
                return store.ttl(p[1]);
            default:
                return "-ERR unknown command '" + p[0] + "'";
        }
    }

    private static String wrongArgs(String cmd) {
        return "-ERR wrong number of arguments for '" + cmd + "' command";
    }

    // -------------------------------------------------------------------- startup

    public static void main(String[] args) throws Exception {
        int port = 6380;
        int maxClients = 1000;
        int idleTimeoutMs = 300_000;
        String walPath = "jredis.wal";
        WriteAheadLog.Policy policy = WriteAheadLog.Policy.EVERYSEC;

        for (int i = 0; i < args.length; i += 2) {
            if (i + 1 >= args.length) {
                System.err.println("missing value for " + args[i]);
                System.exit(2);
            }
            String v = args[i + 1];
            switch (args[i]) {
                case "--port" -> port = Integer.parseInt(v);
                case "--maxclients" -> maxClients = Integer.parseInt(v);
                case "--timeout" -> idleTimeoutMs = Integer.parseInt(v) * 1000;
                case "--wal" -> walPath = v;
                case "--appendfsync" -> policy = WriteAheadLog.Policy.valueOf(v.toUpperCase());
                default -> {
                    System.err.println("unknown option " + args[i]);
                    System.err.println("usage: java JRedisServer [--port N] [--maxclients N] "
                            + "[--timeout SECONDS] [--wal PATH] [--appendfsync always|everysec|no]");
                    System.exit(2);
                }
            }
        }

        KeyValueStore store = new KeyValueStore();
        WriteAheadLog wal = new WriteAheadLog(walPath, policy);

        // Order matters: replay first with no log attached, so replayed records are not
        // written straight back out, then attach and start expiring.
        long t0 = System.currentTimeMillis();
        int replayed = wal.replayInto(store);
        System.out.println("replayed " + replayed + " records in "
                + (System.currentTimeMillis() - t0) + "ms, " + store.size() + " keys");

        wal.start();
        store.attachLog(wal);
        store.startSweeper();
        System.out.println("appendfsync " + policy.name().toLowerCase());

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            store.shutdown();
            wal.close();
        }));

        new JRedisServer(store, port, maxClients, idleTimeoutMs).run();
    }
}
