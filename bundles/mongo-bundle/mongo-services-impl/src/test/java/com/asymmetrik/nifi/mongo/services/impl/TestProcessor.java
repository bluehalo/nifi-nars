package com.asymmetrik.nifi.mongo.services.impl;

import java.util.ArrayList;
import java.util.List;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;

public class TestProcessor extends AbstractProcessor {

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        List<PropertyDescriptor> propDescs = new ArrayList<>();
        propDescs.add(new PropertyDescriptor.Builder()
                .name("StandardMongoClientService test processor")
                .description("StandardMongoClientService test processor")
                .identifiesControllerService(StandardMongoClientService.class)
                .required(true)
                .build());
        return propDescs;
    }
}
