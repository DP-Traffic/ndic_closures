package cz.vutbr.fit.diploma.traffic;

import com.fasterxml.jackson.databind.ObjectMapper;
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
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.eclipse.microprofile.reactive.messaging.Channel;
import org.jboss.logging.Logger;

@ApplicationScoped
public class NDICFetcher {

  private static final Logger LOG = Logger.getLogger(NDICFetcher.class);

  @ConfigProperty(name = "ndic.user")
  String ndicUser;

  @ConfigProperty(name = "ndic.pass")
  String ndicPass;

  // init URL je zároveň ta jediná, která se dotazuje periodicky
  @ConfigProperty(name = "ndic.init.enabled", defaultValue = "true")
  boolean initEnabled;

  @ConfigProperty(name = "ndic.init.url")
  String initUrl;

  @ConfigProperty(name = "ndic.init.marker.path")
  String initMarkerPath;

  @Inject
  @Channel("ndic-out")
  MutinyEmitter<Record<String, String>> emitter;

  @Inject ObjectMapper mapper;

  private HttpClient client;
  private volatile String etag = "", lastMod = "";

  @PostConstruct
  void boot() {
    client =
        HttpClient.newBuilder()
            .version(HttpClient.Version.HTTP_1_1)
            .connectTimeout(Duration.ofSeconds(30))
            .authenticator(
                new Authenticator() {
                  @Override
                  protected PasswordAuthentication getPasswordAuthentication() {
                    return new PasswordAuthentication(ndicUser, ndicPass.toCharArray());
                  }
                })
            .build();

    // Jednorázový init (pouze pokud marker neexistuje)
    if (initEnabled && !Files.exists(Path.of(initMarkerPath))) {
      try {
        int n = fetchFrom(initUrl);
        LOG.infof("Initial pull done, published %d records", n);
      } catch (Exception e) {
        LOG.info("Initial pull failed (continuing)", e);
      }
    }
  }

  /** Každých 5s dotazuje pouze initUrl */
  @Scheduled(every = "5s", delayed = "1s")
  void poll() {
    try {
      int recordsSend = fetchFrom(initUrl);
      if (recordsSend > 0) {
        LOG.infof("Published %d roadworks", recordsSend);
      }
    } catch (Exception e) {
      LOG.warn("poll error", e);
    }
  }

  /** STREAMING fetch + parse; při úspěšném downloadu zapíše marker s UTC časem */
  int fetchFrom(String url) throws Exception {
    HttpRequest.Builder rb =
        HttpRequest.newBuilder()
            .uri(URI.create(url))
            .timeout(Duration.ofMinutes(10)) // header timeout; body je streamován
            .header("Accept", "application/xml, text/xml, */*")
            .header("Accept-Encoding", "gzip")
            .GET();

    if (!etag.isBlank()) {
      rb.header("If-None-Match", etag);
    }
    if (!lastMod.isBlank()) {
      rb.header("If-Modified-Since", lastMod);
    }

    HttpResponse<InputStream> resp =
        client.send(rb.build(), HttpResponse.BodyHandlers.ofInputStream());

    int sc = resp.statusCode();
    if (sc == 304) {
      LOG.info("No changes, marker not updated");
      return 0; // žádná změna, marker neaktualizujeme
    }
    if (sc != 200) {
      String msg = "(no body)";
      try (InputStream err = resp.body()) {
        byte[] buf = err.readNBytes(2048);
        msg = new String(buf, StandardCharsets.UTF_8);
      } catch (IOException ignored) {
      }
      throw new IOException("HTTP " + sc + ": " + msg);
    }

    // 200 OK – uložíme ETag/Last-Modified
    etag = resp.headers().firstValue("ETag").orElse("");

    lastMod = resp.headers().firstValue("Last-Modified").orElse("");

    String ce = resp.headers().firstValue("Content-Encoding").orElse("");

    LOG.infof("Downloading DATEX II (encoding=%s)", ce.isBlank() ? "identity" : ce);

    // úspěšný pars + (potenciálně) publikace
    try (InputStream raw = resp.body();
        InputStream decoded =
            ce.toLowerCase().contains("gzip") ? new GZIPInputStream(raw, 64 * 1024) : raw;
        InputStream in = new ProgressInputStream(new BufferedInputStream(decoded, 128 * 1024))) {

      DatexParser.ParseResult pr = DatexParser.parse(in);

      List<Map<String, Object>> filtered = new ArrayList<>();
      for (Map<String, Object> it : pr.items()) {
        String recType = String.valueOf(it.getOrDefault("recordType", "")).toLowerCase();

        if (recType.contains("maintenanceworks")) {
          filtered.add(it);
        }
      }

      String now = OffsetDateTime.now(ZoneOffset.UTC).toString();

      // i když po odfiltrování nic neposíláme, download proběhl – marker update chceme
      if (!filtered.isEmpty()) {
        for (Map<String, Object> it : filtered) {
          it.put("_source", "ndic");
          it.put("_dataset", "roadworks");
          it.put("_fetchedAt", now);
          if (pr.publicationTime() != null) {
            it.put("publicationTime", pr.publicationTime().toString());
          }

          String key = String.valueOf(it.getOrDefault("situationRecordId", ""));
          if (key.isBlank()) {
            key = "roadworks-" + System.nanoTime();
          }

          String json = mapper.writeValueAsString(it);
          emitter.send(Record.of(key, json)).await().indefinitely();
        }
      }

      // marker: poslední úspěšné STAŽENÍ (HTTP 200 + bez výjimky při parsování)
      writeMarker(now);

      LOG.infof("Parsed %d items, published %d roadworks", pr.items().size(), filtered.size());
      return filtered.size();
    }
  }

  private void writeMarker(String utcIso) {
    try {
      Path p = Path.of(initMarkerPath);
      if (p.getParent() != null) {
        Files.createDirectories(p.getParent());
      }
      Files.writeString(p, utcIso, StandardCharsets.UTF_8);
    } catch (IOException e) {
      LOG.warnf(e, "Failed to write marker to %s", initMarkerPath);
    }
  }

  /** Jednoduchý progress logger (každých ~5 MiB) */
  static final class ProgressInputStream extends FilterInputStream {
    private long read = 0, nextLog = 5L << 20; // 5 MiB

    ProgressInputStream(InputStream in) {
      super(in);
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
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

    @Override
    public int read() throws IOException {
      int n = super.read();
      if (n >= 0) {
        read++;
        if (read >= nextLog) {
          Logger.getLogger(NDICFetcher.class).infof("Downloaded %,d MiB", read >> 20);
          nextLog += 5L << 20;
        }
      }
      return n;
    }
  }
}
