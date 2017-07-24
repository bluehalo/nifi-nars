package com.asymmetrik.nifi.standard.processors.stats;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import com.asymmetrik.nifi.standard.processors.util.MomentAggregator;
import com.google.common.collect.ImmutableSet;

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;

public abstract class AbstractStatsProcessor extends AbstractProcessor {

    static final String DEFAULT_MOMENT_AGGREGATOR_KEY = "";
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
    private volatile long reportingIntervalMillis;
    private volatile long lastReportTime = 0L;
    // protected volatile MomentAggregator aggregator = new MomentAggregator();
    protected ConcurrentHashMap<String, MomentAggregator> momentsMap;

    private Map<String, Optional<Map<String, String>>> latestStats;
    private int batchSize = 1;
    private String correlationKey;

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
        PropertyValue correlationAttrProp = context.getProperty(CORRELATION_ATTR);
        correlationKey = correlationAttrProp.isSet() ? correlationAttrProp.getValue() : DEFAULT_MOMENT_AGGREGATOR_KEY;

        momentsMap = new ConcurrentHashMap<>();
        latestStats = new ConcurrentHashMap<>();
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
            attributes = flowFile.getAttributes();
            String key = StringUtils.isEmpty(correlationKey) ? DEFAULT_MOMENT_AGGREGATOR_KEY : correlationKey;
            String correlationAttr = attributes.getOrDefault(key, DEFAULT_MOMENT_AGGREGATOR_KEY);
            MomentAggregator aggregator = momentsMap.get(correlationAttr);
            if (null == aggregator) {
                aggregator = new MomentAggregator();
                momentsMap.put(correlationAttr, aggregator);
            }
            updateStats(flowFile, aggregator, currentTimestamp);

            Optional<Map<String, String>> stats = latestStats.get(correlationAttr);
            if (null == stats) {
                stats = Optional.of(new ConcurrentHashMap<>());
                latestStats.put(correlationAttr, stats);
            }

            if (stats.isPresent()) {
                flowFile = session.putAllAttributes(flowFile, stats.get());
                session.getProvenanceReporter().modifyAttributes(flowFile);
            }
            outgoing.add(flowFile);
        }

        if (!outgoing.isEmpty()) {
            session.transfer(outgoing, REL_ORIGINAL);
        }

        sendStatsIfPresent(session, new HashMap<>(attributes), currentTimestamp);
    }

    /**
     * Send a flowfile with the stats as attributes: IF the report time is exceeded AND there are
     * stats to send
     */
    private void sendStatsIfPresent(ProcessSession session, Map<String, String> attributes, long currentTimestamp) {
        if (currentTimestamp < lastReportTime + reportingIntervalMillis) {
            return;
        }

        for (Map.Entry<String, Optional<Map<String, String>>> statsMap : latestStats.entrySet()) {

            String statsMapKey = statsMap.getKey();
            Optional<Map<String, String>> result = buildStatAttributes(currentTimestamp, momentsMap.get(statsMapKey));
            if (result.isPresent()) {
                Map<String, String> attrs = new HashMap<>(result.get());
                attrs.put("AbstractStatsProcessor.correlationKey", statsMapKey);
                attributes.putAll(attrs);

                FlowFile statsFlowFile = session.create();
                statsFlowFile = session.putAllAttributes(statsFlowFile, latestStats.get());
                session.getProvenanceReporter().create(statsFlowFile);
                session.transfer(statsFlowFile, REL_STATS);
            }
        }
        lastReportTime = currentTimestamp;
        momentsMap.values().forEach(MomentAggregator::reset);
    }

    protected abstract void updateStats(FlowFile flowFile, long currentTimestamp);

    /**
     * Build stat attributes if the aggregator contains data. Note that CloudWatch does not accept
     * metrics with a SampleCount of zero.
     */
    protected abstract Optional<Map<String, String>> buildStatAttributes(long currentTimestamp, MomentAggregator aggregator);
}
