package cz.vutbr.fit.diploma.traffic;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.eclipse.microprofile.reactive.messaging.Channel;
import org.jboss.logging.Logger;

import io.quarkus.scheduler.Scheduled;
import io.smallrye.reactive.messaging.MutinyEmitter;
import io.smallrye.reactive.messaging.kafka.Record;
import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import java.io.BufferedInputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.Authenticator;
import java.net.PasswordAuthentication;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPInputStream;

@ApplicationScoped
public class NDICFetcher {

    private static final Logger LOG = Logger.getLogger(NDICFetcher.class);

    @ConfigProperty(name = "ndic.user") String ndicUser;
    @ConfigProperty(name = "ndic.pass") String ndicPass;

    @ConfigProperty(name = "ndic.init.enabled") boolean initEnabled;
    @ConfigProperty(name = "ndic.init.url") String initUrl;
    @ConfigProperty(name = "ndic.regular.url") String regularUrl;
    @ConfigProperty(name = "ndic.init.marker.path") String initMarkerPath;

    @Inject
    @Channel("ndic-out")
    MutinyEmitter<Record<String, String>> emitter;

    @Inject
    ObjectMapper mapper;

    private HttpClient client;
    private volatile String etagInit = "", lastModInit = "";
    private volatile String etagRegular = "", lastModRegular = "";

    @PostConstruct
    void boot() {
        client = HttpClient.newBuilder()
            .version(HttpClient.Version.HTTP_1_1)            // stabler for big downloads
            .connectTimeout(Duration.ofSeconds(30))
            .authenticator(new Authenticator() {
                @Override protected PasswordAuthentication getPasswordAuthentication() {
                    return new PasswordAuthentication(ndicUser, ndicPass.toCharArray());
                }
            })
            .build();


        // INITIAL PULL only once
        if (initEnabled && !Files.exists(Path.of(initMarkerPath))) {
            try {
                int n = fetchFrom(initUrl, true);
                LOG.infof("Initial pull published %d records", n);
                Path p = Path.of(initMarkerPath);
                Files.createDirectories(p.getParent());
                Files.writeString(p, OffsetDateTime.now(ZoneOffset.UTC).toString(), StandardCharsets.UTF_8);
            } catch (Exception e) {
                LOG.info("Initial pull failed (continuing)", e);
            }
        }
    }

    /** Scheduled regular pulls (use config placeholder for a string duration like 30s) */
    @Scheduled(every = "{ndic.poll.seconds}", delayed = "1s")
    void scheduled() {
        try { fetchFrom(regularUrl, false); } catch (Exception e) { LOG.warn("poll error", e); }
    }

    /** STREAMING fetch + parse */
    int fetchFrom(String url, boolean isInit) throws Exception {
        HttpRequest.Builder rb = HttpRequest.newBuilder()
            .uri(URI.create(url))
            .timeout(Duration.ofMinutes(10)) // header timeout; body is streamed
            .header("Accept", "application/xml, text/xml, */*")
            .header("Accept-Encoding", "gzip")
            .GET();

        if (isInit && !etagInit.isBlank())        rb.header("If-None-Match", etagInit);
        if (isInit && !lastModInit.isBlank())     rb.header("If-Modified-Since", lastModInit);
        if (!isInit && !etagRegular.isBlank())    rb.header("If-None-Match", etagRegular);
        if (!isInit && !lastModRegular.isBlank()) rb.header("If-Modified-Since", lastModRegular);

        HttpResponse<InputStream> resp =
            client.send(rb.build(), HttpResponse.BodyHandlers.ofInputStream());

        int sc = resp.statusCode();
        if (sc == 304) return 0;
        if (sc != 200) {
            // Read a small error preview (don’t buffer whole stream)
            String msg = "(no body)";
            try (InputStream err = resp.body()) {
                byte[] buf = err.readNBytes(2048);
                msg = new String(buf, StandardCharsets.UTF_8);
            } catch (IOException ignored) {}
            throw new IOException("HTTP " + sc + ": " + msg);
        }

        String et = resp.headers().firstValue("ETag").orElse("");
        String lm = resp.headers().firstValue("Last-Modified").orElse("");
        if (isInit) { etagInit = et; lastModInit = lm; } else { etagRegular = et; lastModRegular = lm; }

        // Stream + optional progress logs
        String ce = resp.headers().firstValue("Content-Encoding").orElse("");
        String cl = resp.headers().firstValue("Content-Length").orElse("");
        cl = cl.isBlank() ? "unknown" : cl;
        LOG.infof("Downloading DATEX II (%s bytes, encoding=%s)", cl, ce.isBlank() ? "identity" : ce);

        try (InputStream raw = resp.body();
             InputStream decoded = ce.toLowerCase().contains("gzip")
                 ? new GZIPInputStream(raw, 64 * 1024)
                 : raw;
             InputStream in = new ProgressInputStream(new BufferedInputStream(decoded, 128 * 1024))) {

            // --- Parse & emit (kept your current non-streaming DatexParser API) ---
            DatexParser.ParseResult pr = DatexParser.parse(in);

            List<Map<String, Object>> filtered = new ArrayList<>();
            for (Map<String, Object> it : pr.items()) {
                String recType = String.valueOf(it.getOrDefault("recordType", "")).toLowerCase();
                if (recType.contains("maintenanceworks")) filtered.add(it);
            }
            if (filtered.isEmpty()) return 0;

            String now = OffsetDateTime.now(ZoneOffset.UTC).toString();
            for (Map<String, Object> it : filtered) {
                it.put("_source", "ndic");
                it.put("_dataset", "roadworks");
                it.put("_fetchedAt", now);
                if (pr.publicationTime() != null) it.put("publicationTime", pr.publicationTime().toString());

                String key = String.valueOf(it.getOrDefault("situationRecordId", ""));
                if (key.isBlank()) key = "roadworks-" + System.nanoTime();

                String json = mapper.writeValueAsString(it);
                emitter.send(Record.of(key, json)).await().indefinitely();
            }
            LOG.infof("Published %d roadworks", filtered.size());
            return filtered.size();
        }
    }

    /** Simple progress logger (logs every ~5 MiB) */
    static final class ProgressInputStream extends FilterInputStream {
        private long read = 0, nextLog = 5L << 20; // 5 MiB
        ProgressInputStream(InputStream in) { super(in); }
        @Override public int read(byte[] b, int off, int len) throws IOException {
            int n = super.read(b, off, len);
            if (n > 0) {
                read += n;
                if (read >= nextLog) {
                    Logger.getLogger(NDICFetcher.class).infof("Downloaded %,d MiB", read >> 20);
                    nextLog += 5L << 20;
                }
            }
            return n;
        }
        @Override public int read() throws IOException {
            int n = super.read();
            if (n >= 0) {
                read++;
                if (read >= nextLog) {
                    LOG.infof("Downloaded %,d MiB", read >> 20);
                    nextLog += 5L << 20;
                }
            }
            return n;
        }
    }
}