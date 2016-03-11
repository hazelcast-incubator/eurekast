package org.hazelcast.discovery.eurekast;

import com.hazelcast.config.Config;
import com.hazelcast.config.DiscoveryConfig;
import com.hazelcast.config.DiscoveryStrategyConfig;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.config.XmlConfigBuilder;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastTestSupport;
import com.netflix.eureka2.testkit.junit.resources.ReadServerResource;
import com.netflix.eureka2.testkit.junit.resources.WriteServerResource;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import static org.junit.Assert.assertEquals;

public class DiscoveryTestCase
        extends HazelcastTestSupport {

    private final WriteServerResource writeServerResource = new WriteServerResource();
    private final ReadServerResource readServerResource = new ReadServerResource(writeServerResource);

    @Rule
    public TestRule ruleChain = RuleChain.outerRule(writeServerResource).around(readServerResource);

    @Test
    public void testSimpleDiscovery()
            throws Exception {

        Config config = new XmlConfigBuilder().build();
        config.setProperty("hazelcast.discovery.enabled", "true");

        JoinConfig join = config.getNetworkConfig().getJoin();
        join.getMulticastConfig().setEnabled(false);
        join.getTcpIpConfig().setEnabled(false);
        join.getAwsConfig().setEnabled(false);

        DiscoveryStrategyConfig strategyConfig = new DiscoveryStrategyConfig(new EurekaDiscoveryStrategyFactory());
        strategyConfig.addProperty("application.name", "test");
        strategyConfig.addProperty("application.group", "test-group");
        strategyConfig.addProperty("resolver.hostname", "localhost");
        strategyConfig.addProperty("resolver.writer.port", String.valueOf(writeServerResource.getDiscoveryPort()));
        strategyConfig.addProperty("resolver.reader.port", String.valueOf(readServerResource.getDiscoveryPort()));

        DiscoveryConfig discoveryConfig = join.getDiscoveryConfig();
        discoveryConfig.addDiscoveryStrategyConfig(strategyConfig);

        try {
            HazelcastInstance hz1 = Hazelcast.newHazelcastInstance(config);
            HazelcastInstance hz2 = Hazelcast.newHazelcastInstance(config);

            assertTrueEventually(new AssertTask() {
                @Override
                public void run()
                        throws Exception {

                    assertEquals(2, hz1.getCluster().getMembers().size());
                    assertEquals(2, hz2.getCluster().getMembers().size());
                }
            });

        } finally {
            Hazelcast.shutdownAll();
        }
    }

}
