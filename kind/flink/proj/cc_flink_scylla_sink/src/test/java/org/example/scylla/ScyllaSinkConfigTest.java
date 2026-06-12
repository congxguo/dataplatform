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
