package com.ververica.cdc.connectors.tdsql.source;

import org.apache.flink.runtime.minicluster.RpcServiceSharing;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.util.TestLogger;

import com.ververica.cdc.connectors.tdsql.testutils.MySqlContainer;
import com.ververica.cdc.connectors.tdsql.testutils.MySqlVersion;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/** base test for {@link TdSqlSource}. */
public abstract class TdSqlSourceTestBase extends TestLogger {
    private static final Logger LOG = LoggerFactory.getLogger(TdSqlSourceTestBase.class);

    protected static final int DEFAULT_PARALLELISM = 4;
    protected static final MySqlContainer TDSQL_SET_1 =
            createMySqlContainer(MySqlVersion.V8_0, 3306);
    protected static final MySqlContainer TDSQL_SET_2 =
            createMySqlContainer(MySqlVersion.V8_0, 3307);

    @Rule
    public final MiniClusterWithClientResource miniClusterResource =
            new MiniClusterWithClientResource(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberTaskManagers(1)
                            .setNumberSlotsPerTaskManager(DEFAULT_PARALLELISM)
                            .setRpcServiceSharing(RpcServiceSharing.DEDICATED)
                            .withHaLeadershipControl()
                            .build());

    @BeforeClass
    public static void startContainers() {
        LOG.info("Starting containers...");
        Startables.deepStart(tdSqlSets()).join();
        LOG.info("Containers are started.");
    }

    public static List<MySqlContainer> tdSqlSets() {
        return Stream.of(TDSQL_SET_1, TDSQL_SET_2).collect(Collectors.toList());
    }

    protected static MySqlContainer createMySqlContainer(MySqlVersion version, int port) {
        return (MySqlContainer)
                new MySqlContainer(version, port)
                        .withConfigurationOverride("docker/server-gtids/my.cnf")
                        .withSetupSQL("docker/setup.sql")
                        .withDatabaseName("flink-test")
                        .withUsername("flinkuser")
                        .withPassword("flinkpw")
                        .withLogConsumer(new Slf4jLogConsumer(LOG));
    }

    public static void assertEqualsInAnyOrder(List<String> expected, List<String> actual) {
        assertTrue(expected != null && actual != null);
        assertEqualsInOrder(
                expected.stream().sorted().collect(Collectors.toList()),
                actual.stream().sorted().collect(Collectors.toList()));
    }

    public static void assertEqualsInOrder(List<String> expected, List<String> actual) {
        assertTrue(expected != null && actual != null);
        assertEquals(expected.size(), actual.size());
        assertArrayEquals(expected.toArray(new String[0]), actual.toArray(new String[0]));
    }
}
