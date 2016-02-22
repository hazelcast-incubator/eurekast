package com.noctarius.discovery.eurekast;

import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.spi.discovery.AbstractDiscoveryStrategy;
import com.hazelcast.spi.discovery.DiscoveryNode;
import com.hazelcast.spi.discovery.SimpleDiscoveryNode;
import com.netflix.appinfo.InstanceInfo;
import com.netflix.discovery.DiscoveryManager;
import com.netflix.discovery.EurekaClient;
import com.netflix.discovery.shared.Application;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.noctarius.discovery.eurekast.EurekaProperties.APPLICATION_NAME;

class EurekaDiscoveryStrategy
        extends AbstractDiscoveryStrategy {

    private final ILogger logger;

    private final String applicationName;

    private final EurekaClient eurekaClient;

    EurekaDiscoveryStrategy(ILogger logger, Map<String, Comparable> properties) {
        super(logger, properties);

        this.logger = logger;
        this.applicationName = getOrNull(APPLICATION_NAME);

        if (applicationName == null) {
            throw new NullPointerException("application-name cannot be null");
        }

        eurekaClient = DiscoveryManager.getInstance().getEurekaClient();
    }

    @Override
    public Iterable<DiscoveryNode> discoverNodes() {
        List<DiscoveryNode> discoveryNodes = new ArrayList<>();

        List<InstanceInfo> instanceInfos = getInstanceInfos();
        for (InstanceInfo instanceInfo : instanceInfos) {
            InetAddress ipAddress = mapAddress(instanceInfo.getIPAddr());
            int port = instanceInfo.getPort();

            Map<String, Object> metadata = mapMetadata(instanceInfo);
            Address address = new Address(ipAddress, port);

            discoveryNodes.add(new SimpleDiscoveryNode(address, metadata));
        }

        return discoveryNodes;
    }

    @Override
    public void destroy() {
        eurekaClient.shutdown();
    }

    private Map<String, Object> mapMetadata(InstanceInfo instanceInfo) {
        Map metadata = instanceInfo.getMetadata();
        return (Map<String, Object>) metadata;
    }

    private InetAddress mapAddress(String address) {
        if (address == null) {
            return null;
        }
        try {
            return InetAddress.getByName(address);
        } catch (UnknownHostException e) {
            logger.warning("Address '" + address + "' could not be resolved");
        }
        return null;
    }

    private List<InstanceInfo> getInstanceInfos() {
        Application application = eurekaClient.getApplication(applicationName);
        return application.getInstances();
    }
}
