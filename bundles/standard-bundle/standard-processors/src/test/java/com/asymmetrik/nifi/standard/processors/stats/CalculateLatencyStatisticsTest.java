package com.asymmetrik.nifi.standard.processors.stats;

import java.util.Map;

import com.google.common.collect.ImmutableMap;

import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class CalculateLatencyStatisticsTest {

    private final byte[] data = new byte[]{'x'};
    private final String attribute = "time";
    private final Map<String, String> attributes = ImmutableMap.of(attribute, Long.toString(System.currentTimeMillis()));

    private TestRunner runner;

    @Before
    public void setup() {
        runner = TestRunners.newTestRunner(CalculateLatencyStatistics.class);
        runner.setProperty(CalculateLatencyStatistics.ATTR_NAME, attribute);
        runner.setProperty(AbstractStatsProcessor.REPORTING_INTERVAL, "1 s");
        runner.setProperty(AbstractStatsProcessor.BATCH_SIZE, "20");
        runner.assertValid();
    }

    @Test
    public void testMissingAttributeName() {
        TestRunner runner = TestRunners.newTestRunner(CalculateLatencyStatistics.class);
        runner.assertNotValid();
    }

    @Test
    public void testValidInput() throws Exception {
        // run once to begin the reporting interval timer
        runner.run();

        // sleep for the remainder of the reporting interval
        Thread.sleep(1000);

        // send 20 files through
        for (int i = 0; i < 20; i++) {
            runner.enqueue(data, attributes);
        }
        runner.run();

        // all 20 originals emitted
        runner.assertTransferCount(AbstractStatsProcessor.REL_ORIGINAL, 20);
        for (MockFlowFile f : runner.getFlowFilesForRelationship(AbstractStatsProcessor.REL_ORIGINAL)) {
            f.assertContentEquals(data);
        }

        // 1 stats report emitted
        runner.assertTransferCount(AbstractStatsProcessor.REL_STATS, 1);
        MockFlowFile flowFile = runner.getFlowFilesForRelationship(AbstractStatsProcessor.REL_STATS).get(0);
        assertEquals(0, flowFile.getSize());
        assertStatAttributesPresent(flowFile);
    }

    @Test
    public void testNoInput() throws Exception {
        // run once to begin the reporting interval timer
        runner.run();

        // sleep for the remainder of the reporting interval
        Thread.sleep(1000);

        // run again after the reporting interval
        runner.run();

        // no input, so nothing emitted to ORIGINAL
        runner.assertTransferCount(AbstractStatsProcessor.REL_ORIGINAL, 0);

        // processor does not emit stats if there is no data
        runner.assertTransferCount(AbstractStatsProcessor.REL_STATS, 0);
    }

    @Test
    public void testInputFollowedByNone() throws Exception {
        // run once to begin the reporting interval timer
        runner.run();

        // sleep for the remainder of the reporting interval
        Thread.sleep(1100);

        // send 20 files through
        for (int i = 0; i < 20; i++) {
            runner.enqueue(data, attributes);
        }
        runner.run();

        // all 20 originals emitted, 1 stats file
        runner.assertTransferCount(AbstractStatsProcessor.REL_ORIGINAL, 20);
        runner.assertTransferCount(AbstractStatsProcessor.REL_STATS, 1);

        // reset state
        runner.clearTransferState();

        // sleep for another reporting interval
        Thread.sleep(1000);

        // run again after the reporting interval
        runner.run();

        // no new input, so nothing emitted to ORIGINAL
        runner.assertTransferCount(AbstractStatsProcessor.REL_ORIGINAL, 0);

        // processor does not emit stats if there is no data
        runner.assertTransferCount(AbstractStatsProcessor.REL_STATS, 0);
    }

    @Test
    public void testInputMissingAttributeName() throws Exception {
        runner.setProperty(CalculateLatencyStatistics.ATTR_NAME, "DOES_NOT_EXIST");
        runner.assertValid();

        for (int i = 0; i < 20; i++) {
            runner.enqueue(data, attributes);
        }
        runner.run();

        // nothing was aggregated, so no stats emitted
        runner.assertTransferCount(AbstractStatsProcessor.REL_STATS, 0);

        // nothing was aggregated, so originals are emitted without additional stat attributes
        runner.assertTransferCount(AbstractStatsProcessor.REL_ORIGINAL, 20);
        for (MockFlowFile f : runner.getFlowFilesForRelationship(AbstractStatsProcessor.REL_ORIGINAL)) {
            f.assertContentEquals(data);
            assertStatAttributesNotPresent(f);
        }
    }

    private void assertStatAttributesPresent(MockFlowFile f) {
        assertNotNull(Integer.parseInt(f.getAttribute("latency_reporter.count")));
        assertNotNull(Double.parseDouble(f.getAttribute("latency_reporter.sum")));
        assertNotNull(Double.parseDouble(f.getAttribute("latency_reporter.min")));
        assertNotNull(Double.parseDouble(f.getAttribute("latency_reporter.max")));
        assertNotNull(Double.parseDouble(f.getAttribute("latency_reporter.avg")));
        assertNotNull(Double.parseDouble(f.getAttribute("latency_reporter.stdev")));
        assertNotNull(Long.parseLong(f.getAttribute("latency_reporter.timestamp")));
        assertEquals("Seconds", f.getAttribute("latency_reporter.units"));
    }

    private void assertStatAttributesNotPresent(MockFlowFile f) {
        assertNull(f.getAttribute("latency_reporter.count"));
        assertNull(f.getAttribute("latency_reporter.sum"));
        assertNull(f.getAttribute("latency_reporter.min"));
        assertNull(f.getAttribute("latency_reporter.max"));
        assertNull(f.getAttribute("latency_reporter.avg"));
        assertNull(f.getAttribute("latency_reporter.stdev"));
        assertNull(f.getAttribute("latency_reporter.timestamp"));
        assertNull(f.getAttribute("latency_reporter.units"));
    }
}