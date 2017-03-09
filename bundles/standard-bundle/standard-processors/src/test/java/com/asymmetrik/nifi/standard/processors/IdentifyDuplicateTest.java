package com.asymmetrik.nifi.standard.processors;

import java.util.Map;

import com.asymmetrik.nifi.standard.services.cache.SetCacheService;
import com.asymmetrik.nifi.standard.services.cache.local.LocalCache;
import com.google.common.collect.ImmutableMap;

import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;

import static com.asymmetrik.nifi.standard.processors.IdentifyDuplicate.BATCH_SIZE;
import static com.asymmetrik.nifi.standard.processors.IdentifyDuplicate.CACHE_ENTRY_IDENTIFIER;
import static com.asymmetrik.nifi.standard.processors.IdentifyDuplicate.CACHE_SERVICE;
import static com.asymmetrik.nifi.standard.processors.IdentifyDuplicate.REL_DUPLICATE;
import static com.asymmetrik.nifi.standard.processors.IdentifyDuplicate.REL_FAILURE;
import static com.asymmetrik.nifi.standard.processors.IdentifyDuplicate.REL_NON_DUPLICATE;

public class IdentifyDuplicateTest {

    TestRunner runner;

    @Before
    public void setup() throws InitializationException {
        runner = TestRunners.newTestRunner(IdentifyDuplicate.class);

        runner.setProperty(CACHE_SERVICE, "cache");
        runner.setProperty(CACHE_ENTRY_IDENTIFIER, "${id}");
        runner.setProperty(BATCH_SIZE, "10");

        LocalCache cache = new LocalCache();
        Map<String, String> cacheProperties = ImmutableMap.of(
                SetCacheService.MAX_SIZE.getName(), "10",
                SetCacheService.AGE_OFF_DURATION.getName(), "10 min");
        runner.addControllerService("cache", cache, cacheProperties);
        runner.enableControllerService(cache);
    }

    @Test
    public void testDuplicate() {
        Map<String, String> props = ImmutableMap.of("id", "xxx");

        runner.enqueue(new byte[]{}, props);
        runner.run();
        runner.assertAllFlowFilesTransferred(REL_NON_DUPLICATE, 1);
        runner.clearTransferState();

        runner.enqueue(new byte[]{}, props);
        runner.run();
        runner.assertAllFlowFilesTransferred(REL_DUPLICATE, 1);
        runner.assertTransferCount(REL_NON_DUPLICATE, 0);
        runner.assertTransferCount(REL_FAILURE, 0);
    }

    @Test
    public void testNonDuplicate() {
        Map<String, String> props1 = ImmutableMap.of("id", "xxx");
        Map<String, String> props2 = ImmutableMap.of("id", "yyy");

        runner.enqueue(new byte[]{}, props1);
        runner.run();
        runner.assertAllFlowFilesTransferred(REL_NON_DUPLICATE, 1);
        runner.clearTransferState();

        runner.enqueue(new byte[]{}, props2);
        runner.run();
        runner.assertAllFlowFilesTransferred(REL_NON_DUPLICATE, 1);
        runner.assertTransferCount(REL_DUPLICATE, 0);
        runner.assertTransferCount(REL_FAILURE, 0);
    }

    @Test
    public void testFailure() {
        Map<String, String> props = ImmutableMap.of("xxx", "xxx");

        runner.enqueue(new byte[]{}, props);
        runner.run();
        runner.assertAllFlowFilesTransferred(REL_FAILURE, 1);
        runner.assertTransferCount(REL_NON_DUPLICATE, 0);
        runner.assertTransferCount(REL_DUPLICATE, 0);
    }

    @Test
    public void testMultipleFiles() {
        Map<String, String> props1 = ImmutableMap.of("id", "xxx");
        Map<String, String> props2 = ImmutableMap.of("id", "yyy");
        Map<String, String> props3 = ImmutableMap.of("xxx", "xxx");

        runner.enqueue(new byte[]{}, props1);
        runner.enqueue(new byte[]{}, props2);
        runner.enqueue(new byte[]{}, props3); // failure
        runner.enqueue(new byte[]{}, props1); // duplicate
        runner.run();

        runner.assertTransferCount(REL_NON_DUPLICATE, 2);
        runner.assertTransferCount(REL_DUPLICATE, 1);
        runner.assertTransferCount(REL_FAILURE, 1);
    }
}
