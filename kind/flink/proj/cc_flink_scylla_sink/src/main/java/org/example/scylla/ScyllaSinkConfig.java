package org.example.scylla;

import java.io.Serializable;

/**
 * Immutable configuration for {@link ScyllaSink}.
 *
 * <p>Build via {@link ScyllaSinkConfig#builder()}:
 * <pre>{@code
 * ScyllaSinkConfig config = ScyllaSinkConfig.builder()
 *     .contactPoints("scylla-host", 9042)
 *     .localDatacenter("datacenter1")
 *     .keyspace("ads")
 *     .batchSize(100)
 *     .flushIntervalMs(500)
 *     .build();
 * }</pre>
 *
 * <p>Setting {@code batchSize=1} (the default) disables buffering entirely —
 * each record is written immediately with no background thread.
 */
public class ScyllaSinkConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String contactPoints;
    private final int port;
    private final String localDatacenter;
    private final String keyspace;
    private final String username;
    private final String password;
    private final int batchSize;
    private final long flushIntervalMs;

    private ScyllaSinkConfig(Builder builder) {
        this.contactPoints    = builder.contactPoints;
        this.port             = builder.port;
        this.localDatacenter  = builder.localDatacenter;
        this.keyspace         = builder.keyspace;
        this.username         = builder.username;
        this.password         = builder.password;
        this.batchSize        = builder.batchSize;
        this.flushIntervalMs  = builder.flushIntervalMs;
    }

    public String getContactPoints()   { return contactPoints; }
    public int    getPort()            { return port; }
    public String getLocalDatacenter() { return localDatacenter; }
    public String getKeyspace()        { return keyspace; }
    public String getUsername()        { return username; }
    public String getPassword()        { return password; }
    public int    getBatchSize()       { return batchSize; }
    public long   getFlushIntervalMs() { return flushIntervalMs; }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {
        private String contactPoints;
        private int    port            = 9042;
        private String localDatacenter;
        private String keyspace;
        private String username;
        private String password;
        private int    batchSize       = 1;
        private long   flushIntervalMs = 0L;

        /** ScyllaDB host with default port 9042. */
        public Builder contactPoints(String contactPoints) {
            this.contactPoints = contactPoints;
            return this;
        }

        /** ScyllaDB host and explicit port. */
        public Builder contactPoints(String contactPoints, int port) {
            this.contactPoints = contactPoints;
            this.port = port;
            return this;
        }

        /** Required. The local datacenter name for DC-aware load balancing. */
        public Builder localDatacenter(String localDatacenter) {
            this.localDatacenter = localDatacenter;
            return this;
        }

        /** Optional. Sets the default keyspace on the session. */
        public Builder keyspace(String keyspace) {
            this.keyspace = keyspace;
            return this;
        }

        /** Optional. Credentials for plain-text authentication. */
        public Builder username(String username) {
            this.username = username;
            return this;
        }

        public Builder password(String password) {
            this.password = password;
            return this;
        }

        /**
         * Number of records to buffer before flushing. Default is 1 (immediate mode).
         * Must be >= 1.
         */
        public Builder batchSize(int batchSize) {
            this.batchSize = batchSize;
            return this;
        }

        /**
         * Flush the buffer at most every {@code flushIntervalMs} milliseconds,
         * regardless of batch size. Default is 0 (disabled). Has no effect when
         * {@code batchSize=1}.
         */
        public Builder flushIntervalMs(long flushIntervalMs) {
            this.flushIntervalMs = flushIntervalMs;
            return this;
        }

        public ScyllaSinkConfig build() {
            if (contactPoints == null || contactPoints.isEmpty()) {
                throw new IllegalArgumentException("contactPoints is required");
            }
            if (localDatacenter == null || localDatacenter.isEmpty()) {
                throw new IllegalArgumentException("localDatacenter is required");
            }
            if (batchSize < 1) {
                throw new IllegalArgumentException("batchSize must be >= 1");
            }
            return new ScyllaSinkConfig(this);
        }
    }
}
