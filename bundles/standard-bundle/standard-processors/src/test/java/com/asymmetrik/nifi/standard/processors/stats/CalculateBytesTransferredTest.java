package com.asymmetrik.nifi.standard.processors.stats;

import java.util.HashMap;
import java.util.Map;

import com.asymmetrik.nifi.standard.processors.util.MomentAggregator;

import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class CalculateBytesTransferredTest {

    private TestRunner runner;

    @Before
    public void setup() {
        runner = TestRunners.newTestRunner(CalculateBytesTransferred.class);
        runner.setProperty(CalculateBytesTransferred.CORRELATION_ATTR, "flowId");
        runner.setProperty(CalculateBytesTransferred.REPORTING_INTERVAL, "1 s");
        runner.setProperty(CalculateBytesTransferred.BATCH_SIZE, "20");
        runner.assertValid();
    }

    @Test
    public void testAggregation() {
        CalculateBytesTransferred calculateBytesTransferred = new CalculateBytesTransferred();
        MomentAggregator momentAggregator = new MomentAggregator();
        long currentTimeMillis = System.currentTimeMillis();
        ProcessSession session = runner.getProcessSessionFactory().createSession();
        int n = 50;
        for (int i = 0; i < n; i++) {
            FlowFile flowFile = session.create();
            flowFile = session.putAttribute(flowFile, "fileSize", String.valueOf(i));
            calculateBytesTransferred.updateStats(flowFile, momentAggregator, currentTimeMillis);
        }
        double expected = n * (n - 1) / 2;
        assertEquals(expected, momentAggregator.getSum(), 1e-6);
        assertEquals(n, momentAggregator.getN());
        assertEquals(n - 1, momentAggregator.getMax(), 1e-6);
        assertEquals(0, momentAggregator.getMin(), 1e-6);
        assertEquals((n - 1) / 2.0, momentAggregator.getMean(), 1e-6);
    }

    @Test
    public void testInvalidData() {
        String data = "a";
        int n = 20;
        for (int i = 0; i < n; i++) {
            Map<String, String> attributes = new HashMap<>();
            attributes.put("fileSize", i % 2 == 0 ? "a" : String.valueOf(i));
            attributes.put("flowId", "foobar");
            runner.enqueue(data, attributes);
        }
        runner.run();

        // all 20 originals emitted, 1 stats file
        runner.assertTransferCount(AbstractStatsProcessor.REL_STATS, 1);
        MockFlowFile flowFile = runner.getFlowFilesForRelationship(AbstractStatsProcessor.REL_STATS).get(0);

        int count = n/2;
        assertEquals(count, Integer.parseInt(flowFile.getAttribute("CalculateBytesTransferred.count")));
        assertEquals(count*count, Double.parseDouble(flowFile.getAttribute("CalculateBytesTransferred.sum")), 1e-6);
        assertEquals(1, Double.parseDouble(flowFile.getAttribute("CalculateBytesTransferred.min")), 1e-6);
        assertEquals(19, Double.parseDouble(flowFile.getAttribute("CalculateBytesTransferred.max")), 1e-6);
        assertEquals(count, Double.parseDouble(flowFile.getAttribute("CalculateBytesTransferred.avg")), 1e-6);
    }
}