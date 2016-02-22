package com.noctarius.discovery.eurekast;

import com.hazelcast.config.properties.PropertyDefinition;
import com.hazelcast.config.properties.PropertyTypeConverter;
import com.hazelcast.config.properties.SimplePropertyDefinition;
import com.hazelcast.config.properties.ValueValidator;

public final class EurekaProperties {

    public static final PropertyDefinition APPLICATION_NAME = property("application-name", PropertyTypeConverter.STRING);

    // Prevent instantiation
    private EurekaProperties() {
    }

    private static PropertyDefinition property(String key, PropertyTypeConverter typeConverter) {
        return property(key, typeConverter, null);
    }

    private static PropertyDefinition property(String key, PropertyTypeConverter typeConverter, ValueValidator valueValidator) {
        return new SimplePropertyDefinition(key, true, typeConverter, valueValidator);
    }

}
