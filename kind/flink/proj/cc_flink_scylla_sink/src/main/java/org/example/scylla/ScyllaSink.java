package org.example.scylla;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * General-purpose abstract Flink sink for writing typed records to ScyllaDB.
 *
 * <p>Extend this class and implement:
 * <ul>
 *   <li>{@link #getInsertCql()} — the CQL INSERT/UPSERT statement (named parameters, e.g. {@code :id})</li>
 *   <li>{@link #bindRecord(PreparedStatement, T)} — bind one record onto the prepared statement</li>
 * </ul>
 *
 * <p>Optionally override {@link #onSessionReady(CqlSession)} to prepare additional
 * statements or initialize resources after the session is established.
 *
 * <p>Write behavior is controlled by {@link ScyllaSinkConfig}:
 * <ul>
 *   <li>{@code batchSize=1} (default) — each record is written immediately on {@code invoke()}</li>
 *   <li>{@code batchSize>1} — records are buffered and flushed when the buffer is full,
 *       when the flush timer fires, or when {@code close()} is called</li>
 * </ul>
 *
 * <p>Both modes provide <b>at-least-once</b> delivery. A failed write throws a
 * {@link RuntimeException}, causing Flink to restart the task.
 *
 * <p>Example subclass:
 * <pre>{@code
 * public class AdEventScyllaSink extends ScyllaSink<AdEvent> {
 *     public AdEventScyllaSink(ScyllaSinkConfig config) { super(config); }
 *
 *     protected String getInsertCql() {
 *         return "INSERT INTO ads.ad_events (event_id, campaign_id) VALUES (:event_id, :campaign_id)";
 *     }
 *
 *     protected BoundStatement bindRecord(PreparedStatement p, AdEvent e) {
 *         return p.bind()
 *             .setUuid("event_id", UUID.fromString(e.event_id))
 *             .setString("campaign_id", e.campaign_id);
 *     }
 * }
 * }</pre>
 *
 * @param <T> the record type to write
 */
public abstract class ScyllaSink<T> extends RichSinkFunction<T> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(ScyllaSink.class);

    private final ScyllaSinkConfig config;

    // transient — not serialized; re-initialized in open()
    private transient CqlSession session;
    private transient PreparedStatement prepared;
    private transient List<T> buffer;
    private transient ScheduledExecutorService flushScheduler;

    protected ScyllaSink(ScyllaSinkConfig config) {
        this.config = config;
    }

    // ─── Lifecycle ────────────────────────────────────────────────────────────

    @Override
    public final void open(Configuration parameters) throws Exception {
        session  = buildSession(config);
        prepared = session.prepare(getInsertCql());
        onSessionReady(session);

        if (config.getBatchSize() > 1) {
            buffer = new ArrayList<>(config.getBatchSize());

            if (config.getFlushIntervalMs() > 0) {
                flushScheduler = Executors.newSingleThreadScheduledExecutor();
                flushScheduler.scheduleAtFixedRate(
                        this::flushBuffer,
                        config.getFlushIntervalMs(),
                        config.getFlushIntervalMs(),
                        TimeUnit.MILLISECONDS
                );
            }
        }
    }

    @Override
    public final void invoke(T record, Context context) {
        if (config.getBatchSize() == 1) {
            executeOne(record);
        } else {
            synchronized (this) {
                buffer.add(record);
                if (buffer.size() >= config.getBatchSize()) {
                    flushBuffer();
                }
            }
        }
    }

    @Override
    public final void close() throws Exception {
        if (flushScheduler != null) {
            flushScheduler.shutdown();
            flushScheduler.awaitTermination(5, TimeUnit.SECONDS);
        }
        if (buffer != null) {
            synchronized (this) {
                flushBuffer();
            }
        }
        if (session != null) {
            session.close();
        }
    }

    // ─── Extension points ─────────────────────────────────────────────────────

    /**
     * Returns the CQL INSERT/UPSERT statement to prepare. Called once during {@code open()}.
     * Use named parameters (e.g. {@code :event_id}) for clarity.
     */
    protected abstract String getInsertCql();

    /**
     * Binds one record onto the prepared statement and returns a ready-to-execute
     * {@link BoundStatement}.
     *
     * @param prepared the statement prepared from {@link #getInsertCql()}
     * @param record   the record to write
     */
    protected abstract BoundStatement bindRecord(PreparedStatement prepared, T record);

    /**
     * Called once after the {@link CqlSession} is created and the CQL statement is prepared,
     * before the sink starts accepting records. Override to prepare additional statements
     * or initialize subclass state that requires a live session.
     *
     * <p>Default implementation does nothing.
     *
     * @param session the live session (do not close it)
     */
    protected void onSessionReady(CqlSession session) {
        // hook for subclasses — no-op by default
    }

    /**
     * Builds the {@link CqlSession} from the provided config.
     *
     * <p>Override in tests to inject a mock session without a real network connection.
     */
    protected CqlSession buildSession(ScyllaSinkConfig config) {
        CqlSessionBuilder builder = CqlSession.builder()
                .addContactPoint(new InetSocketAddress(config.getContactPoints(), config.getPort()))
                .withLocalDatacenter(config.getLocalDatacenter());

        if (config.getKeyspace() != null) {
            builder.withKeyspace(config.getKeyspace());
        }
        if (config.getUsername() != null) {
            builder.withAuthCredentials(config.getUsername(), config.getPassword());
        }
        return builder.build();
    }

    // ─── Internal ─────────────────────────────────────────────────────────────

    private synchronized void flushBuffer() {
        if (buffer == null || buffer.isEmpty()) {
            return;
        }
        for (T record : buffer) {
            executeOne(record);
        }
        buffer.clear();
    }

    private void executeOne(T record) {
        try {
            BoundStatement stmt = bindRecord(prepared, record);
            session.execute(stmt);
        } catch (Exception e) {
            LOG.error("Failed to write record to ScyllaDB: {}", record, e);
            throw new RuntimeException("ScyllaSink write failed", e);
        }
    }
}
