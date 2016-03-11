package org.hazelcast.discovery.eurekast;

import com.hazelcast.config.properties.PropertyDefinition;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.discovery.DiscoveryNode;
import com.hazelcast.spi.discovery.DiscoveryStrategy;
import com.hazelcast.spi.discovery.DiscoveryStrategyFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class EurekaDiscoveryStrategyFactory
        implements DiscoveryStrategyFactory {

    private static final Collection<PropertyDefinition> PROPERTY_DEFINITIONS;

    static {
        List<PropertyDefinition> propertyDefinitions = new ArrayList<>();
        propertyDefinitions.add(EurekaProperties.APPLICATION_NAME);
        propertyDefinitions.add(EurekaProperties.APPLICATION_GROUP);
        propertyDefinitions.add(EurekaProperties.RESOLVER_DNS);
        propertyDefinitions.add(EurekaProperties.RESOLVER_FILE);
        propertyDefinitions.add(EurekaProperties.RESOLVER_HOSTNAME);
        propertyDefinitions.add(EurekaProperties.RESOLVER_WRITER_PORT);
        propertyDefinitions.add(EurekaProperties.RESOLVER_READER_PORT);
        PROPERTY_DEFINITIONS = Collections.unmodifiableCollection(propertyDefinitions);
    }

    @Override
    public Class<? extends DiscoveryStrategy> getDiscoveryStrategyType() {
        return EurekaDiscoveryStrategy.class;
    }

    @Override
    public DiscoveryStrategy newDiscoveryStrategy(DiscoveryNode discoveryNode, ILogger logger, Map<String, Comparable> map) {
        return new EurekaDiscoveryStrategy(discoveryNode, logger, map);
    }

    @Override
    public Collection<PropertyDefinition> getConfigurationProperties() {
        return PROPERTY_DEFINITIONS;
    }
}
