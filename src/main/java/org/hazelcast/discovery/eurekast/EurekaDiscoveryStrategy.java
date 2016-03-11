package org.hazelcast.discovery.eurekast;

import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.spi.discovery.AbstractDiscoveryStrategy;
import com.hazelcast.spi.discovery.DiscoveryNode;
import com.hazelcast.spi.discovery.SimpleDiscoveryNode;
import com.netflix.eureka2.client.EurekaInterestClient;
import com.netflix.eureka2.client.EurekaInterestClientBuilder;
import com.netflix.eureka2.client.EurekaRegistrationClient;
import com.netflix.eureka2.client.EurekaRegistrationClientBuilder;
import com.netflix.eureka2.client.Eurekas;
import com.netflix.eureka2.client.resolver.ServerResolver;
import com.netflix.eureka2.client.resolver.ServerResolvers;
import com.netflix.eureka2.interests.ChangeNotification;
import com.netflix.eureka2.interests.Interest;
import com.netflix.eureka2.interests.Interests;
import com.netflix.eureka2.registry.datacenter.BasicDataCenterInfo;
import com.netflix.eureka2.registry.datacenter.DataCenterInfo;
import com.netflix.eureka2.registry.instance.InstanceInfo;
import com.netflix.eureka2.registry.instance.NetworkAddress;
import com.netflix.eureka2.registry.instance.ServicePort;
import rx.Subscription;
import rx.functions.Action1;
import rx.subjects.BehaviorSubject;

import java.io.File;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static com.netflix.eureka2.interests.Interests.forApplications;
import static com.netflix.eureka2.registry.instance.NetworkAddress.PRIVATE_ADDRESS;
import static com.netflix.eureka2.registry.instance.NetworkAddress.PUBLIC_ADDRESS;
import static com.netflix.eureka2.registry.instance.NetworkAddress.ProtocolType;
import static com.netflix.eureka2.registry.instance.NetworkAddress.ProtocolType.IPv4;
import static com.netflix.eureka2.registry.instance.NetworkAddress.ProtocolType.IPv6;
import static com.netflix.eureka2.registry.instance.ServicePort.ServicePortBuilder.aServicePort;
import static org.hazelcast.discovery.eurekast.EurekaProperties.APPLICATION_GROUP;
import static org.hazelcast.discovery.eurekast.EurekaProperties.APPLICATION_NAME;
import static org.hazelcast.discovery.eurekast.EurekaProperties.RESOLVER_DNS;
import static org.hazelcast.discovery.eurekast.EurekaProperties.RESOLVER_FILE;
import static org.hazelcast.discovery.eurekast.EurekaProperties.RESOLVER_HOSTNAME;
import static org.hazelcast.discovery.eurekast.EurekaProperties.RESOLVER_READER_PORT;
import static org.hazelcast.discovery.eurekast.EurekaProperties.RESOLVER_WRITER_PORT;

class EurekaDiscoveryStrategy
        extends AbstractDiscoveryStrategy {

    private final ILogger logger;

    private final String applicationName;
    private final String applicationGroup;

    private final InstanceInfo localInstanceInfo;

    private final BehaviorSubject<InstanceInfo> infoSubject = BehaviorSubject.create();
    private final EurekaRegistrationClient registrationClient;
    private final Subscription registrationSubscription;

    private final EurekaInterestClient interestClient;

    EurekaDiscoveryStrategy(DiscoveryNode localNode, ILogger logger, Map<String, Comparable> properties) {
        super(logger, properties);

        this.logger = logger;
        this.applicationName = getOrNull("eurekast", APPLICATION_NAME);
        this.applicationGroup = getOrNull("eurekast", APPLICATION_GROUP);
        this.localInstanceInfo = buildInstanceInfo(localNode, applicationName);

        if (applicationName == null) {
            throw new NullPointerException("application.name must be specified");
        } else if (applicationGroup == null) {
            throw new NullPointerException("application.group must be specified");
        }

        String hostname = getOrNull("eurekast", RESOLVER_HOSTNAME);
        String dns = getOrNull("eurekast", RESOLVER_DNS);
        File file = getOrNull("eurekast", RESOLVER_FILE);
        int writerPort = getOrDefault("eurekast", RESOLVER_WRITER_PORT, -1);
        int readerPort = getOrDefault("eurekast", RESOLVER_READER_PORT, -1);

        if (writerPort == -1) {
            throw new IllegalArgumentException("resolver.writer.port must be specified");
        } else if (readerPort == -1) {
            throw new IllegalArgumentException("resolver.reader.port must be specified");
        }

        ServerResolver serverResolver = buildServerResolver(hostname, dns, file, writerPort);

        // Registration configuration if node
        registrationClient = buildRegistrationClient(localNode, serverResolver);
        registrationSubscription = subscribeForRegistration(registrationClient);

        // Discovery configuration
        ServerResolver clientResolver = buildClientResolver(serverResolver);
        EurekaInterestClientBuilder interestClientBuilder = Eurekas.newInterestClientBuilder();
        interestClient = interestClientBuilder.withServerResolver(clientResolver).build();
        interestClient.forInterest(Interests.forApplications(applicationName)).subscribe();
    }

    @Override
    public Iterable<DiscoveryNode> discoverNodes() {
        List<DiscoveryNode> discoveryNodes = new ArrayList<>();

        Interest<InstanceInfo> infoInterest = forApplications(applicationName);

        interestClient.forInterest(infoInterest).forEach(new Action1<ChangeNotification<InstanceInfo>>() {
            @Override
            public void call(ChangeNotification<InstanceInfo> notification) {
                System.out.println("Notification: " + notification.getData());

                InstanceInfo instanceInfo = notification.getData();

                if (instanceInfo == null || instanceInfo.getStatus() != InstanceInfo.Status.UP) {
                    return;
                }

                Iterator<InstanceInfo.ServiceEndpoint> iterator = instanceInfo.serviceEndpoints();
                while (iterator.hasNext()) {
                    InstanceInfo.ServiceEndpoint serviceEndpoint = iterator.next();

                    InetAddress ipAddress = mapAddress(serviceEndpoint);
                    int port = serviceEndpoint.getServicePort().getPort();

                    Map<String, Object> metadata = mapMetadata(instanceInfo);
                    Address address = new Address(ipAddress, port);

                    discoveryNodes.add(new SimpleDiscoveryNode(address, metadata));
                }
            }
        });

        return discoveryNodes;
    }

    @Override
    public void start() {
        if (registrationClient != null) {
            InstanceInfo.Builder builder = new InstanceInfo.Builder();
            InstanceInfo up = builder.withInstanceInfo(localInstanceInfo).withStatus(InstanceInfo.Status.UP).build();
            infoSubject.onNext(up);
        }
    }

    @Override
    public void destroy() {
        if (registrationClient != null) {
            // Unregister first
            InstanceInfo.Builder builder = new InstanceInfo.Builder();
            InstanceInfo down = builder.withInstanceInfo(localInstanceInfo).withStatus(InstanceInfo.Status.DOWN).build();
            infoSubject.onNext(down);
            registrationSubscription.unsubscribe();

            // Shutdown
            registrationClient.shutdown();
        }

        interestClient.shutdown();
    }

    private Map<String, Object> mapMetadata(InstanceInfo instanceInfo) {
        Map metadata = instanceInfo.getMetaData();
        return (Map<String, Object>) metadata;
    }

    private InetAddress mapAddress(InstanceInfo.ServiceEndpoint serviceEndpoint) {
        if (serviceEndpoint == null) {
            return null;
        }
        try {
            return InetAddress.getByName(serviceEndpoint.getAddress().getIpAddress());
        } catch (UnknownHostException e) {
            logger.warning("ServiceEndpoint '" + serviceEndpoint + "' could not be resolved");
        }
        return null;
    }

    private NetworkAddress mapAddress(Address address, boolean publicAddress) {
        try {
            InetAddress inetAddress = address.getInetAddress();

            String label = publicAddress ? PUBLIC_ADDRESS : PRIVATE_ADDRESS;
            ProtocolType protocolType = inetAddress instanceof Inet6Address ? IPv6 : IPv4;

            NetworkAddress.NetworkAddressBuilder builder = NetworkAddress.NetworkAddressBuilder.aNetworkAddress();
            return builder.withIpAddress(inetAddress.getHostAddress()).withLabel(label).withProtocolType(protocolType).build();

        } catch (UnknownHostException e) {
            throw new IllegalStateException("Cannot register node with Eureka server", e);
        }
    }

    private EurekaRegistrationClient buildRegistrationClient(DiscoveryNode localNode, ServerResolver serverResolver) {
        if (localNode == null) {
            return null;
        }
        EurekaRegistrationClientBuilder registrationClientBuilder = Eurekas.newRegistrationClientBuilder();
        return registrationClientBuilder.withServerResolver(serverResolver).build();
    }

    private InstanceInfo buildInstanceInfo(DiscoveryNode localNode, String applicationName) {
        String uuid = UUID.randomUUID().toString();

        Address privateAddress = localNode.getPrivateAddress();
        Address publicAddress = localNode.getPublicAddress();

        List<NetworkAddress> addresses = new ArrayList<>();
        if (privateAddress != publicAddress) {
            addresses.add(mapAddress(privateAddress, false));
        }
        addresses.add(mapAddress(publicAddress, true));

        DataCenterInfo dataCenterInfo = new BasicDataCenterInfo(uuid, addresses);

        ServicePort port = aServicePort().withSecure(false).withPort(localNode.getPublicAddress().getPort()).build();

        InstanceInfo.Status status = InstanceInfo.Status.STARTING;
        InstanceInfo.Builder builder = new InstanceInfo.Builder().withId(uuid).withApp(applicationName).withPorts(port);
        return builder.withAppGroup(applicationGroup).withDataCenterInfo(dataCenterInfo).withStatus(status).build();
    }

    private Subscription subscribeForRegistration(EurekaRegistrationClient registrationClient) {
        if (registrationClient == null) {
            return null;
        }
        try {
            Subscription subscription = registrationClient.register(infoSubject).initialRegistrationResult().subscribe();
            infoSubject.onNext(localInstanceInfo);
            return subscription;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private ServerResolver buildServerResolver(String hostname, String dns, File file, int port) {
        ServerResolver serverResolver;
        if (hostname != null) {
            serverResolver = ServerResolvers.fromHostname(hostname).withPort(port);
        } else if (dns != null) {
            serverResolver = ServerResolvers.fromDnsName(dns).withPort(port);
        } else if (file != null) {
            serverResolver = ServerResolvers.fromFile(file);
        } else {
            throw new IllegalArgumentException("One of resolver.{hostname|dns|file} needs to be defined");
        }
        return serverResolver;
    }

    private ServerResolver buildClientResolver(ServerResolver serverResolver) {
        return ServerResolvers.fromEureka(serverResolver).forInterest(Interests.forFullRegistry());
    }
}
