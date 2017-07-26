package com.asymmetrik.nifi.standard.processors.stats;

import java.util.Map;
import java.util.Optional;

import com.asymmetrik.nifi.standard.processors.util.MomentAggregator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.TriggerWhenEmpty;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessorInitializationContext;

@TriggerWhenEmpty
@InputRequirement(InputRequirement.Requirement.INPUT_ALLOWED)
@SupportsBatching
@Tags({"asymmetrik", "bytes", "monitoring", "statistics"})
@CapabilityDescription("Calculates latency statistics for a flow.")
@WritesAttributes({
        @WritesAttribute(attribute = "CalculateBytesTransferred.count"),
        @WritesAttribute(attribute = "CalculateBytesTransferred.sum"),
        @WritesAttribute(attribute = "CalculateBytesTransferred.min"),
        @WritesAttribute(attribute = "CalculateBytesTransferred.max"),
        @WritesAttribute(attribute = "CalculateBytesTransferred.avg"),
        @WritesAttribute(attribute = "CalculateBytesTransferred.stdev"),
        @WritesAttribute(attribute = "CalculateBytesTransferred.timestamp")
})
public class CalculateBytesTransferred extends AbstractStatsProcessor {

    @Override
    protected void init(ProcessorInitializationContext context) {
        properties = ImmutableList.of(CORRELATION_ATTR, REPORTING_INTERVAL, BATCH_SIZE);
    }

    @OnScheduled
    @Override
    public void onScheduled(final ProcessContext context) {
        super.onScheduled(context);
    }

    @Override
    protected void updateStats(FlowFile flowFile, MomentAggregator aggregator, long currentTimestamp) {
        String fileSize = flowFile.getAttribute("fileSize");
        Long bytes = Long.valueOf(fileSize);
        aggregator.addValue(bytes.doubleValue());
    }

    @Override
    protected Optional<Map<String, String>> buildStatAttributes(long currentTimestamp, MomentAggregator aggregator) {

        // emit stats only if there is data
        if (aggregator.getN() > 0) {
            int n = aggregator.getN();
            double sum = aggregator.getSum();
            double min = aggregator.getMin();
            double max = aggregator.getMax();
            double mean = aggregator.getMean();
            double stdev = aggregator.getStandardDeviation();

            Map<String, String> attributes = new ImmutableMap.Builder<String, String>()
                    .put("CalculateBytesTransferred.count", Integer.toString(n))
                    .put("CalculateBytesTransferred.sum", Double.toString(sum))
                    .put("CalculateBytesTransferred.min", Double.toString(min))
                    .put("CalculateBytesTransferred.max", Double.toString(max))
                    .put("CalculateBytesTransferred.avg", Double.toString(mean))
                    .put("CalculateBytesTransferred.stdev", Double.toString(stdev))
                    .put("CalculateBytesTransferred.timestamp", Long.toString(currentTimestamp))
                    .put("CalculateBytesTransferred.units", "Seconds")
                    .build();
            return Optional.of(attributes);

        } else {
            return Optional.empty();
        }
    }
}
