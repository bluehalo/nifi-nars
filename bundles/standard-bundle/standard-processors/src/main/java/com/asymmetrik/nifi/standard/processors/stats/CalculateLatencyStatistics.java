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
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.util.StandardValidators;

@TriggerWhenEmpty
@InputRequirement(InputRequirement.Requirement.INPUT_ALLOWED)
@SupportsBatching
@Tags({"asymmetrik", "latency", "monitoring", "statistics"})
@CapabilityDescription("Calculates latency statistics for a flow.")
@WritesAttributes({
        @WritesAttribute(attribute = "latency_reporter.count"),
        @WritesAttribute(attribute = "latency_reporter.sum"),
        @WritesAttribute(attribute = "latency_reporter.min"),
        @WritesAttribute(attribute = "latency_reporter.max"),
        @WritesAttribute(attribute = "latency_reporter.avg"),
        @WritesAttribute(attribute = "latency_reporter.stdev"),
        @WritesAttribute(attribute = "latency_reporter.timestamp")
})
public class CalculateLatencyStatistics extends AbstractStatsProcessor {

    /**
     * Property Descriptors
     */
    static final PropertyDescriptor ATTR_NAME = new PropertyDescriptor.Builder()
            .name("timestamp attribute")
            .description("The attribute name holding the timestamp value. This attribute is assumed to be a Java long that represents the milliseconds after epoch.")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    private volatile String keyName;


    @Override
    protected void init(ProcessorInitializationContext context) {
        properties = ImmutableList.of(ATTR_NAME, CORRELATION_ATTR, REPORTING_INTERVAL, BATCH_SIZE);
    }

    @OnScheduled
    @Override
    public void onScheduled(final ProcessContext context) {
        super.onScheduled(context);
        keyName = context.getProperty(ATTR_NAME).getValue();
    }

    @Override
    protected void updateStats(FlowFile flowFile, MomentAggregator aggregator, long currentTimestamp) {

        String eventTimeAttribute = flowFile.getAttribute(keyName);
        try {
            // Extract timestamp from original flowfile
            long eventTime = Long.parseLong(eventTimeAttribute);

            // calculate latency and add to aggregator
            Long latency = currentTimestamp - eventTime;
            aggregator.addValue(latency.doubleValue() / 1000.0);
        } catch (NumberFormatException nfe) {
            getLogger().warn("Unable to convert {} to a long", new Object[]{eventTimeAttribute}, nfe);
        }
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
                    .put("latency_reporter.count", Integer.toString(n))
                    .put("latency_reporter.sum", Double.toString(sum))
                    .put("latency_reporter.min", Double.toString(min))
                    .put("latency_reporter.max", Double.toString(max))
                    .put("latency_reporter.avg", Double.toString(mean))
                    .put("latency_reporter.stdev", Double.toString(stdev))
                    .put("latency_reporter.timestamp", Long.toString(currentTimestamp))
                    .put("latency_reporter.units", "Seconds")
                    .build();
            return Optional.of(attributes);

        } else {
            return Optional.empty();
        }
    }
}
