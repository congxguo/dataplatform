# ScyllaSink Shared Library Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a reusable Flink sink library (`cc_flink_scylla_sink`) for writing typed records to ScyllaDB, with config-driven immediate or batched write modes.

**Architecture:** An abstract `RichSinkFunction<T>` (`ScyllaSink`) manages the ScyllaDB session lifecycle, a synchronized record buffer, and an optional timer-based flush. Subclasses implement only `getInsertCql()` and `bindRecord()`. `ScyllaSinkConfig` drives all runtime behavior — batch size of 1 collapses to immediate mode with no buffer or background thread.

**Tech Stack:** Java 8, Flink 1.17.1 (provided scope), ScyllaDB Java Driver `com.scylladb:java-driver-core:4.18.0.0`, JUnit 4, Mockito 4.8.1

---

## File Map

| File | Action | Responsibility |
|------|--------|---------------|
| `kind/flink/proj/cc_flink_scylla_sink/pom.xml` | Create | Maven project definition, dependencies |
| `kind/flink/proj/cc_flink_scylla_sink/src/main/java/org/example/scylla/ScyllaSinkConfig.java` | Create | Immutable config with builder |
| `kind/flink/proj/cc_flink_scylla_sink/src/main/java/org/example/scylla/ScyllaSink.java` | Create | Abstract `RichSinkFunction<T>` base class |
| `kind/flink/proj/cc_flink_scylla_sink/src/main/java/org/example/scylla/example/AdEventScyllaSink.java` | Create | Concrete example writing `AdEvent` to `ads.ad_events` |
| `kind/flink/proj/cc_flink_scylla_sink/src/test/java/org/example/scylla/ScyllaSinkConfigTest.java` | Create | Unit tests for config builder validation |
| `kind/flink/proj/cc_flink_scylla_sink/src/test/java/org/example/scylla/ScyllaSinkTest.java` | Create | Unit tests for batching and flush behavior (mock session) |

---

## Task 1: Maven project scaffold

**Files:**
- Create: `kind/flink/proj/cc_flink_scylla_sink/pom.xml`

- [ ] **Step 1: Create the pom.xml**

```xml
<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements. See the NOTICE file
distributed with this work for additional information
regarding copyright ownership. The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License. You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0
-->
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0
         http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>

    <groupId>org.example</groupId>
    <artifactId>cc-flink-scylla-sink</artifactId>
    <version>1.0-SNAPSHOT</version>
    <packaging>jar</packaging>

    <name>Flink ScyllaDB Sink Library</name>

    <properties>
        <project.build.sourceEncoding>UTF-8</project.build.sourceEncoding>
        <flink.version>1.17.1</flink.version>
        <target.java.version>1.8</target.java.version>
        <maven.compiler.source>${target.java.version}</maven.compiler.source>
        <maven.compiler.target>${target.java.version}</maven.compiler.target>
    </properties>

    <dependencies>
        <!-- Flink: provided — consumers shade this into their job JARs -->
        <dependency>
            <groupId>org.apache.flink</groupId>
            <artifactId>flink-streaming-java</artifactId>
            <version>${flink.version}</version>
            <scope>provided</scope>
        </dependency>

        <!-- ScyllaDB Java driver (shard-aware fork of DataStax driver 4.x) -->
        <dependency>
            <groupId>com.scylladb</groupId>
            <artifactId>java-driver-core</artifactId>
            <version>4.18.0.0</version>
        </dependency>

        <dependency>
            <groupId>com.scylladb</groupId>
            <artifactId>java-driver-query-builder</artifactId>
            <version>4.18.0.0</version>
        </dependency>

        <!-- Test dependencies -->
        <dependency>
            <groupId>junit</groupId>
            <artifactId>junit</artifactId>
            <version>4.13.2</version>
            <scope>test</scope>
        </dependency>
        <dependency>
            <groupId>org.mockito</groupId>
            <artifactId>mockito-core</artifactId>
            <version>4.8.1</version>
            <scope>test</scope>
        </dependency>
    </dependencies>

    <build>
        <plugins>
            <plugin>
                <groupId>org.apache.maven.plugins</groupId>
                <artifactId>maven-compiler-plugin</artifactId>
                <version>3.1</version>
                <configuration>
                    <source>${target.java.version}</source>
                    <target>${target.java.version}</target>
                </configuration>
            </plugin>
        </plugins>
    </build>
</project>
```

- [ ] **Step 2: Verify Maven can resolve dependencies**

```bash
cd kind/flink/proj/cc_flink_scylla_sink
mvn dependency:resolve -q
```

Expected: `BUILD SUCCESS` with no errors. If `com.scylladb:java-driver-core:4.18.0.0` is not found, check the ScyllaDB Maven repository — you may need to add it:

```xml
<repositories>
    <repository>
        <id>scylladb</id>
        <url>https://repo1.maven.org/maven2/</url>
    </repository>
</repositories>
```

- [ ] **Step 3: Commit**

```bash
git add kind/flink/proj/cc_flink_scylla_sink/pom.xml
git commit -m "feat: [scylla-sink] scaffold Maven project"
```

---

## Task 2: `ScyllaSinkConfig`

**Files:**
- Create: `kind/flink/proj/cc_flink_scylla_sink/src/main/java/org/example/scylla/ScyllaSinkConfig.java`
- Create: `kind/flink/proj/cc_flink_scylla_sink/src/test/java/org/example/scylla/ScyllaSinkConfigTest.java`

- [ ] **Step 1: Write the failing tests**

Create `src/test/java/org/example/scylla/ScyllaSinkConfigTest.java`:

```java
package org.example.scylla;

import org.junit.Test;
import static org.junit.Assert.*;

public class ScyllaSinkConfigTest {

    @Test
    public void buildsWithRequiredFields() {
        ScyllaSinkConfig config = ScyllaSinkConfig.builder()
                .contactPoints("localhost")
                .localDatacenter("datacenter1")
                .build();

        assertEquals("localhost", config.getContactPoints());
        assertEquals(9042, config.getPort());
        assertEquals("datacenter1", config.getLocalDatacenter());
        assertNull(config.getKeyspace());
        assertNull(config.getUsername());
        assertNull(config.getPassword());
        assertEquals(1, config.getBatchSize());
        assertEquals(0L, config.getFlushIntervalMs());
    }

    @Test
    public void buildsWithAllFields() {
        ScyllaSinkConfig config = ScyllaSinkConfig.builder()
                .contactPoints("scylla-host", 9043)
                .localDatacenter("dc1")
                .keyspace("ads")
                .username("user")
                .password("pass")
                .batchSize(100)
                .flushIntervalMs(500)
                .build();

        assertEquals("scylla-host", config.getContactPoints());
        assertEquals(9043, config.getPort());
        assertEquals("dc1", config.getLocalDatacenter());
        assertEquals("ads", config.getKeyspace());
        assertEquals("user", config.getUsername());
        assertEquals("pass", config.getPassword());
        assertEquals(100, config.getBatchSize());
        assertEquals(500L, config.getFlushIntervalMs());
    }

    @Test(expected = IllegalArgumentException.class)
    public void throwsWhenContactPointsMissing() {
        ScyllaSinkConfig.builder()
                .localDatacenter("datacenter1")
                .build();
    }

    @Test(expected = IllegalArgumentException.class)
    public void throwsWhenLocalDatacenterMissing() {
        ScyllaSinkConfig.builder()
                .contactPoints("localhost")
                .build();
    }

    @Test(expected = IllegalArgumentException.class)
    public void throwsWhenBatchSizeIsZero() {
        ScyllaSinkConfig.builder()
                .contactPoints("localhost")
                .localDatacenter("datacenter1")
                .batchSize(0)
                .build();
    }

    @Test(expected = IllegalArgumentException.class)
    public void throwsWhenBatchSizeIsNegative() {
        ScyllaSinkConfig.builder()
                .contactPoints("localhost")
                .localDatacenter("datacenter1")
                .batchSize(-1)
                .build();
    }
}
```

- [ ] **Step 2: Run tests to confirm they fail**

```bash
cd kind/flink/proj/cc_flink_scylla_sink
mvn test -Dtest=ScyllaSinkConfigTest -q
```

Expected: `BUILD FAILURE` — `ScyllaSinkConfig` does not exist yet.

- [ ] **Step 3: Implement `ScyllaSinkConfig`**

Create `src/main/java/org/example/scylla/ScyllaSinkConfig.java`:

```java
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
        private int    port           = 9042;
        private String localDatacenter;
        private String keyspace;
        private String username;
        private String password;
        private int    batchSize      = 1;
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
```

- [ ] **Step 4: Run tests to confirm they pass**

```bash
cd kind/flink/proj/cc_flink_scylla_sink
mvn test -Dtest=ScyllaSinkConfigTest
```

Expected:
```
Tests run: 6, Failures: 0, Errors: 0, Skipped: 0
BUILD SUCCESS
```

- [ ] **Step 5: Commit**

```bash
git add kind/flink/proj/cc_flink_scylla_sink/src/
git commit -m "feat: [scylla-sink] add ScyllaSinkConfig with builder"
```

---

## Task 3: `ScyllaSink<T>` abstract base class

**Files:**
- Create: `kind/flink/proj/cc_flink_scylla_sink/src/main/java/org/example/scylla/ScyllaSink.java`
- Create: `kind/flink/proj/cc_flink_scylla_sink/src/test/java/org/example/scylla/ScyllaSinkTest.java`

- [ ] **Step 1: Write the failing tests**

The tests use Mockito to mock `CqlSession` and `PreparedStatement`, and a concrete inner subclass `TestSink` that overrides `buildSession()` to avoid a real network connection.

Create `src/test/java/org/example/scylla/ScyllaSinkTest.java`:

```java
package org.example.scylla;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.BoundStatementBuilder;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import org.apache.flink.configuration.Configuration;
import org.junit.Before;
import org.junit.Test;

import static org.mockito.Mockito.*;

public class ScyllaSinkTest {

    private CqlSession mockSession;
    private PreparedStatement mockPrepared;
    private BoundStatement mockBound;
    private ScyllaSinkConfig immediateConfig;
    private ScyllaSinkConfig batchConfig;

    @Before
    public void setUp() {
        mockSession  = mock(CqlSession.class);
        mockPrepared = mock(PreparedStatement.class);
        mockBound    = mock(BoundStatement.class);

        when(mockSession.prepare(anyString())).thenReturn(mockPrepared);

        immediateConfig = ScyllaSinkConfig.builder()
                .contactPoints("localhost")
                .localDatacenter("datacenter1")
                .batchSize(1)
                .build();

        batchConfig = ScyllaSinkConfig.builder()
                .contactPoints("localhost")
                .localDatacenter("datacenter1")
                .batchSize(3)
                .build();
    }

    /** Minimal concrete subclass — injects mock session, binds a string value. */
    private class TestSink extends ScyllaSink<String> {
        TestSink(ScyllaSinkConfig config) {
            super(config);
        }

        @Override
        protected CqlSession buildSession(ScyllaSinkConfig config) {
            return mockSession;
        }

        @Override
        protected String getInsertCql() {
            return "INSERT INTO test.table (id) VALUES (:id)";
        }

        @Override
        protected BoundStatement bindRecord(PreparedStatement prepared, String record) {
            return mockBound;
        }
    }

    @Test
    public void immediateModeWritesEachRecordOnInvoke() throws Exception {
        TestSink sink = new TestSink(immediateConfig);
        sink.open(new Configuration());

        sink.invoke("record-1", null);
        sink.invoke("record-2", null);

        verify(mockSession, times(2)).execute(mockBound);
        sink.close();
    }

    @Test
    public void batchModeBuffersUntilBatchSizeReached() throws Exception {
        TestSink sink = new TestSink(batchConfig);
        sink.open(new Configuration());

        sink.invoke("r1", null);
        sink.invoke("r2", null);
        // batchSize=3 — nothing flushed yet
        verify(mockSession, times(0)).execute(any(BoundStatement.class));

        sink.invoke("r3", null);
        // batch full — flush
        verify(mockSession, times(3)).execute(mockBound);

        sink.close();
    }

    @Test
    public void batchModeFlushesRemainingRecordsOnClose() throws Exception {
        TestSink sink = new TestSink(batchConfig);
        sink.open(new Configuration());

        sink.invoke("r1", null);
        sink.invoke("r2", null);
        // only 2 records — below batchSize of 3, not flushed yet
        verify(mockSession, times(0)).execute(any(BoundStatement.class));

        sink.close();
        // close() must flush the remaining 2
        verify(mockSession, times(2)).execute(mockBound);
    }

    @Test
    public void sessionIsPreparedOnceOnOpen() throws Exception {
        TestSink sink = new TestSink(immediateConfig);
        sink.open(new Configuration());

        sink.invoke("r1", null);
        sink.invoke("r2", null);

        // prepare() called exactly once, regardless of record count
        verify(mockSession, times(1)).prepare("INSERT INTO test.table (id) VALUES (:id)");
        sink.close();
    }

    @Test
    public void sessionIsClosedOnClose() throws Exception {
        TestSink sink = new TestSink(immediateConfig);
        sink.open(new Configuration());
        sink.close();

        verify(mockSession, times(1)).close();
    }
}
```

- [ ] **Step 2: Run tests to confirm they fail**

```bash
cd kind/flink/proj/cc_flink_scylla_sink
mvn test -Dtest=ScyllaSinkTest -q
```

Expected: `BUILD FAILURE` — `ScyllaSink` does not exist yet.

- [ ] **Step 3: Implement `ScyllaSink`**

Create `src/main/java/org/example/scylla/ScyllaSink.java`:

```java
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
 *   <li>{@link #getInsertCql()} — the CQL INSERT statement (named parameters, e.g. {@code :id})</li>
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
     * Returns the CQL INSERT statement to prepare. Called once during {@code open()}.
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
     * Builds the {@link CqlSession} from {@code config}.
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
```

- [ ] **Step 4: Run tests to confirm they pass**

```bash
cd kind/flink/proj/cc_flink_scylla_sink
mvn test -Dtest=ScyllaSinkTest
```

Expected:
```
Tests run: 5, Failures: 0, Errors: 0, Skipped: 0
BUILD SUCCESS
```

- [ ] **Step 5: Run all tests**

```bash
mvn test
```

Expected:
```
Tests run: 11, Failures: 0, Errors: 0, Skipped: 0
BUILD SUCCESS
```

- [ ] **Step 6: Commit**

```bash
git add kind/flink/proj/cc_flink_scylla_sink/src/
git commit -m "feat: [scylla-sink] add ScyllaSink abstract base class with batching and timer flush"
```

---

## Task 4: `AdEventScyllaSink` example

**Files:**
- Create: `kind/flink/proj/cc_flink_scylla_sink/src/main/java/org/example/scylla/example/AdEventScyllaSink.java`

This class lives in `example/` to signal it is reference code, not a framework class. It depends on the `AdEvent` class from `cc_flink_event_window`. Since we do not want a compile-time Maven dependency on another app module, copy only the fields needed and note the dependency in a comment.

- [ ] **Step 1: Create `AdEventScyllaSink`**

```java
package org.example.scylla.example;

import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import org.example.scylla.ScyllaSink;
import org.example.scylla.ScyllaSinkConfig;

import java.time.Instant;
import java.util.UUID;

/**
 * Example ScyllaSink that writes ad-event records to the {@code ads.ad_events} table.
 *
 * <p>Depends on the {@code AdEvent} POJO from {@code cc_flink_event_window}. When using
 * this sink in a Flink job, add {@code cc-flink-scylla-sink} as a compile-scope dependency
 * in that project's pom.xml and include it in the shaded JAR.
 *
 * <p>Table schema (from {@code scylla_schema.cql}):
 * <pre>
 * CREATE TABLE ads.ad_events (
 *     event_id      UUID PRIMARY KEY,
 *     event_type    TEXT,
 *     event_ts      TIMESTAMP,
 *     campaign_id   TEXT,
 *     user_id       TEXT,
 *     revenue       DOUBLE
 * );
 * </pre>
 *
 * <p>Usage:
 * <pre>{@code
 * ScyllaSinkConfig config = ScyllaSinkConfig.builder()
 *     .contactPoints("scylla-svc.scylla.svc.cluster.local", 9042)
 *     .localDatacenter("datacenter1")
 *     .keyspace("ads")
 *     .batchSize(100)
 *     .flushIntervalMs(500)
 *     .build();
 *
 * stream.addSink(new AdEventScyllaSink(config));
 * }</pre>
 */
public class AdEventScyllaSink extends ScyllaSink<AdEvent> {

    public AdEventScyllaSink(ScyllaSinkConfig config) {
        super(config);
    }

    @Override
    protected String getInsertCql() {
        return "INSERT INTO ads.ad_events "
                + "(event_id, event_type, event_ts, campaign_id, user_id, revenue) "
                + "VALUES (:event_id, :event_type, :event_ts, :campaign_id, :user_id, :revenue)";
    }

    @Override
    protected BoundStatement bindRecord(PreparedStatement prepared, AdEvent event) {
        return prepared.bind()
                .setUuid("event_id",      UUID.fromString(event.event_id))
                .setString("event_type",  event.event_type)
                .setInstant("event_ts",   Instant.ofEpochMilli(event.event_time))
                .setString("campaign_id", event.campaign_id)
                .setString("user_id",     event.user_id)
                .setDouble("revenue",     event.revenue);
    }
}
```

Note: `AdEvent` above refers to `org.example.AdEvent` from the `cc_flink_event_window` project. The import statement should be `import org.example.AdEvent;`. Add it at the top of the file.

- [ ] **Step 2: Add the missing import to the file**

The complete import block at the top of `AdEventScyllaSink.java`:

```java
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import org.example.AdEvent;
import org.example.scylla.ScyllaSink;
import org.example.scylla.ScyllaSinkConfig;

import java.time.Instant;
import java.util.UUID;
```

- [ ] **Step 3: Add `cc_flink_event_window` as a provided dependency in `pom.xml` so `AdEvent` compiles**

In `kind/flink/proj/cc_flink_scylla_sink/pom.xml`, add inside `<dependencies>`:

```xml
<!-- Only needed to compile the AdEventScyllaSink example.
     The consuming job JAR already contains AdEvent — do not shade this in. -->
<dependency>
    <groupId>org.example</groupId>
    <artifactId>CcFlinkEventWindow</artifactId>
    <version>1.0-SNAPSHOT</version>
    <scope>provided</scope>
</dependency>
```

- [ ] **Step 4: Install `cc_flink_event_window` to local Maven repo so the dependency resolves**

```bash
cd kind/flink/proj/cc_flink_event_window
mvn install -DskipTests -q
```

Expected: `BUILD SUCCESS`

- [ ] **Step 5: Build the library**

```bash
cd kind/flink/proj/cc_flink_scylla_sink
mvn package -DskipTests -q
```

Expected: `BUILD SUCCESS`, produces `target/cc-flink-scylla-sink-1.0-SNAPSHOT.jar`

- [ ] **Step 6: Run all tests**

```bash
mvn test
```

Expected:
```
Tests run: 11, Failures: 0, Errors: 0, Skipped: 0
BUILD SUCCESS
```

- [ ] **Step 7: Commit**

```bash
git add kind/flink/proj/cc_flink_scylla_sink/
git commit -m "feat: [scylla-sink] add AdEventScyllaSink example"
```

---

## Task 5: Wire into `cc_flink_event_window`

**Files:**
- Modify: `kind/flink/proj/cc_flink_event_window/pom.xml`
- Modify: `kind/flink/proj/cc_flink_event_window/src/main/java/org/example/KafkaEventTimeSlidingWindowJob.java`

This task shows how a consuming job wires in the shared library. It replaces the existing `result.print()` with `AdEventScyllaSink`.

- [ ] **Step 1: Add the library dependency to `cc_flink_event_window/pom.xml`**

Inside `<dependencies>` in `kind/flink/proj/cc_flink_event_window/pom.xml`:

```xml
<dependency>
    <groupId>org.example</groupId>
    <artifactId>cc-flink-scylla-sink</artifactId>
    <version>1.0-SNAPSHOT</version>
</dependency>
```

- [ ] **Step 2: Install the library to the local Maven repo**

```bash
cd kind/flink/proj/cc_flink_scylla_sink
mvn install -DskipTests -q
```

Expected: `BUILD SUCCESS`

- [ ] **Step 3: Update `KafkaEventTimeSlidingWindowJob` to sink into ScyllaDB**

In `KafkaEventTimeSlidingWindowJob.java`, replace the section after Step 3 (the window `.process(...)`) through the end of `main()`. The processed stream currently emits `String` summaries. Since `AdEventScyllaSink` writes `AdEvent` records, wire the sink on the raw `withWm` stream (before windowing) so raw events are persisted to ScyllaDB:

```java
// ─────────────────────────────────────────────
// 4. ScyllaDB Sink — persist raw events
// ─────────────────────────────────────────────
ScyllaSinkConfig scyllaConfig = ScyllaSinkConfig.builder()
        .contactPoints("scylla-client.scylla.svc.cluster.local", 9042)
        .localDatacenter("datacenter1")
        .keyspace("ads")
        .batchSize(100)
        .flushIntervalMs(500)
        .build();

withWm.addSink(new AdEventScyllaSink(scyllaConfig));

// ─────────────────────────────────────────────
// 5. Output — printed to TaskManager stdout/logs
// ─────────────────────────────────────────────
result.print();

env.execute("Kafka Event Time Sliding Window Job");
```

Add the necessary imports at the top of the file:

```java
import org.example.scylla.ScyllaSinkConfig;
import org.example.scylla.example.AdEventScyllaSink;
```

- [ ] **Step 4: Build the job JAR**

```bash
cd kind/flink/proj/cc_flink_event_window
mvn package -DskipTests -q
```

Expected: `BUILD SUCCESS`, produces `target/flink-kafka-sliding-window-job.jar`

- [ ] **Step 5: Commit**

```bash
git add kind/flink/proj/cc_flink_event_window/pom.xml \
        kind/flink/proj/cc_flink_event_window/src/main/java/org/example/KafkaEventTimeSlidingWindowJob.java
git commit -m "feat: [event-window] wire AdEventScyllaSink into Kafka sliding window job"
```
