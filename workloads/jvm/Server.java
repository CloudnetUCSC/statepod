import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.zip.CRC32;

/**
 * JVM workload for the migration evaluation: a small HTTP service whose state is a
 * deterministic in-memory cache, so the JIT-compiled code, class metadata and a sizeable heap
 * are all part of what migrates (the "expected best case for M1": most of a JVM's pages are
 * identical across instances of the same image).
 *
 *   --entries N   number of cache entries created at start (default 20000)
 *   --value-kb K  size of each value in KiB (default 4); heap payload = N x K KiB
 *   --port P      HTTP port (default 8000)
 *
 * Values are a pure function of (key index, generation), so integrity is exact:
 *   GET /health   -> "ok"
 *   GET /count    -> number of entries
 *   GET /checksum -> "<entries> <crc32 of every key and value> <writes>"   (full recompute)
 *   GET /put?i=N  -> rewrites entry N with the next generation (a write; returns the new generation)
 * The server starts listening AFTER the cache is built (readiness = HTTP /health answers).
 */
public class Server {
    static final ConcurrentHashMap<Integer, byte[]> cache = new ConcurrentHashMap<>();
    static final ConcurrentHashMap<Integer, Integer> gens = new ConcurrentHashMap<>();
    static volatile long writes = 0;
    static int valueBytes;

    static byte[] value(int i, int gen) {
        byte[] v = new byte[valueBytes];
        long s = (i * 0x9E3779B97F4A7C15L) ^ ((long) gen << 32) ^ 0xD1B54A32D192ED03L;
        for (int k = 0; k < v.length; k += 8) {
            s ^= s << 13; s ^= s >>> 7; s ^= s << 17;       // xorshift64
            for (int b = 0; b < 8 && k + b < v.length; b++) v[k + b] = (byte) (s >>> (8 * b));
        }
        return v;
    }

    static String checksum() {
        CRC32 crc = new CRC32();
        int n = 0;
        for (int i = 0; i < cache.size() * 2 && n < cache.size(); i++) {
            byte[] v = cache.get(i);
            if (v == null) continue;
            n++;
            crc.update(i);
            crc.update(v, 0, v.length);
            // the stored value must equal its generating function, not just be stable
            byte[] exp = value(i, gens.get(i));
            if (!java.util.Arrays.equals(v, exp)) return n + " MISMATCH@" + i + " " + writes;
        }
        return n + " " + crc.getValue() + " " + writes;
    }

    static void reply(HttpExchange ex, String body) throws IOException {
        byte[] b = body.getBytes(StandardCharsets.UTF_8);
        ex.sendResponseHeaders(200, b.length);
        try (OutputStream os = ex.getResponseBody()) { os.write(b); }
    }

    public static void main(String[] args) throws Exception {
        int entries = 20000, valueKb = 4, port = 8000;
        for (int i = 0; i + 1 < args.length; i += 2) {
            switch (args[i]) {
                case "--entries": entries = Integer.parseInt(args[i + 1]); break;
                case "--value-kb": valueKb = Integer.parseInt(args[i + 1]); break;
                case "--port": port = Integer.parseInt(args[i + 1]); break;
                default: throw new IllegalArgumentException("unknown flag " + args[i]);
            }
        }
        valueBytes = valueKb * 1024;
        for (int i = 0; i < entries; i++) { cache.put(i, value(i, 0)); gens.put(i, 0); }

        HttpServer s = HttpServer.create(new InetSocketAddress(port), 0);
        s.createContext("/", ex -> reply(ex, "ok"));          // the generic HTTP probe asks for /
        s.createContext("/health", ex -> reply(ex, "ok"));
        s.createContext("/count", ex -> reply(ex, Integer.toString(cache.size())));
        s.createContext("/checksum", ex -> reply(ex, checksum()));
        s.createContext("/put", ex -> {
            String q = ex.getRequestURI().getQuery();
            int i = Integer.parseInt(q.substring(q.indexOf('=') + 1)) % cache.size();
            int g;
            synchronized (Server.class) {
                g = gens.get(i) + 1;
                cache.put(i, value(i, g));
                gens.put(i, g);
                writes++;
            }
            reply(ex, Integer.toString(g));
        });
        s.setExecutor(Executors.newFixedThreadPool(4));
        s.start();
        System.err.println("jvm workload ready: " + entries + " entries x " + valueKb + " KiB on :" + port);
    }
}
