package com.asymmetrik.nifi.standard.processors.stats;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import com.asymmetrik.nifi.standard.processors.util.MomentAggregator;
import com.google.common.collect.ImmutableSet;

import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;

public abstract class AbstractStatsProcessor extends AbstractProcessor {

    /**
     * Relationship Descriptors
     */
    static final Relationship REL_ORIGINAL = new Relationship.Builder()
            .name("original")
            .description("All input flowfiles are written to this relationship, with statistics as additional flowfile attributes")
            .build();
    static final Relationship REL_STATS = new Relationship.Builder()
            .name("statistics")
            .description("Empty flowfiles with statistics as flowfiles attributes are written to this relationship")
            .build();

    static final PropertyDescriptor BATCH_SIZE = new PropertyDescriptor.Builder()
            .name("Batch Size")
            .description("The maximum number of flowfiles to take from the incoming work queue.")
            .defaultValue("1")
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .required(true)
            .build();
    /**
     * Property Descriptors
     */
    static final PropertyDescriptor REPORTING_INTERVAL = new PropertyDescriptor.Builder()
            .name("Reporting Interval")
            .description("Indicates how often this processor should report statistics.")
            .required(true)
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .defaultValue("1 m")
            .build();

    private final Set<Relationship> relationships = ImmutableSet.of(REL_ORIGINAL, REL_STATS);
    protected List<PropertyDescriptor> properties;
    protected volatile long reportingIntervalMillis;
    protected volatile long lastReportTime = 0L;
    protected volatile MomentAggregator aggregator = new MomentAggregator();

    protected Optional<Map<String, String>> latestStats = Optional.empty();
    protected int batchSize = 1;

    @Override
    protected final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @Override
    public final Set<Relationship> getRelationships() {
        return relationships;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        reportingIntervalMillis = context.getProperty(REPORTING_INTERVAL).asTimePeriod(TimeUnit.MILLISECONDS);
        aggregator.reset();
        batchSize = context.getProperty(BATCH_SIZE).asInteger();
    }

    @Override
    public final void onTrigger(ProcessContext context, ProcessSession session) {
        List<FlowFile> incoming = session.get(batchSize);
        if (incoming.isEmpty()) {
            return;
        }

        final long currentTimestamp = System.currentTimeMillis();

        List<FlowFile> outgoing = new ArrayList<>();

        for (FlowFile flowFile : incoming) {
            updateStats(flowFile, currentTimestamp);

            if (latestStats.isPresent()) {
                flowFile = session.putAllAttributes(flowFile, latestStats.get());
            }
            outgoing.add(flowFile);
        }

        if (!outgoing.isEmpty()) {
            session.transfer(outgoing, REL_ORIGINAL);
        }

        sendStatsIfPresent(session, currentTimestamp);
    }

    /**
     * Send a flowfile with the stats as attributes: IF the report time is exceeded AND there are
     * stats to send
     */
    protected void sendStatsIfPresent(ProcessSession session, long currentTimestamp) {
        if (currentTimestamp >= lastReportTime + reportingIntervalMillis) {
            latestStats = buildStatAttributes(currentTimestamp);
            if (latestStats.isPresent()) {
                FlowFile statsFlowFile = session.create();
                statsFlowFile = session.putAllAttributes(statsFlowFile, latestStats.get());
                session.transfer(statsFlowFile, REL_STATS);
            }

            lastReportTime = currentTimestamp;
            aggregator.reset();
        }
    }

    protected abstract void updateStats(FlowFile flowFile, long currentTimestamp);

    /**
     * Build stat attributes if the aggregator contains data. Note that CloudWatch does not accept
     * metrics with a SampleCount of zero.
     */
    protected abstract Optional<Map<String, String>> buildStatAttributes(long currentTimestamp);
}
