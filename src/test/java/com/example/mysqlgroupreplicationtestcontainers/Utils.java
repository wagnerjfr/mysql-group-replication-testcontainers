package com.example.mysqlgroupreplicationtestcontainers;

import com.example.mysqlgroupreplicationtestcontainers.groupmember.GroupMember;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.containers.Network;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Slf4j
public class Utils {

    private static final Logger logger = LoggerFactory.getLogger(MultiPrimaryGroupReplicationTest.class);

    public static MySQLContainer<?> getContainer(String dockerImage, int id, String hostNamePrefix, boolean isSinglePrimary, Network network) {
        return new MySQLContainer<>(dockerImage)
                //.withLogConsumer(new Slf4jLogConsumer(logger))
                .withCommand(Utils.getCommand(id, isSinglePrimary, hostNamePrefix))
                .withUsername("root")
                .withPassword("mypass")
                .withCreateContainerCmdModifier(it -> it.withHostName(hostNamePrefix + id))
                .withNetwork(network);
    }

    public static String getCommand(int id, boolean isSinglePrimary, String hostNamePrefix) {
        final String flag1 = isSinglePrimary ? "ON" : "OFF";
        final String flag2 = isSinglePrimary ? "OFF" : "ON";
        return String.format("mysqld --server-id=%s " +
                        "--log-bin=mysql-bin-1.log --relay-log=relay-bin.log " +
                        "--enforce-gtid-consistency=ON " +
                        "--log-slave-updates=ON " +
                        "--gtid-mode=ON " +
                        "--transaction-write-set-extraction=XXHASH64 " +
                        "--binlog-checksum=NONE " +
                        "--master-info-repository=TABLE " +
                        "--relay-log-info-repository=TABLE " +
                        "--relay-log-recovery=ON " +
                        "--loose-group-replication-start-on-boot=OFF " +
                        "--loose-group-replication-group-name=aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee " +
                        "--loose-group-replication-local-address=%s:33061 " +
                        "--loose-group-replication-group-seeds=%s1:33061,%s2:33061,%s3:33061 " +
                        "--loose-group-replication-single-primary-mode=%s " +
                        "--loose-group-replication-enforce-update-everywhere-checks=%s",
                id, hostNamePrefix + id, hostNamePrefix, hostNamePrefix, hostNamePrefix, flag1, flag2);
    }

    public static MySQLServer startMySQLService(String id, String hostNamePrefix, MySQLContainer<?> container) {
        container.start();
        String url = container.getJdbcUrl();
        ConnectionPool connectionPool =  new ConnectionPool(url, container.getUsername(), container.getPassword());
        return new MySQLServer(id, hostNamePrefix, connectionPool);
    }

    public static void stopMySQLService(MySQLContainer<?> container) {
        if (container != null) {
            container.stop();
        }
    }

    public static void printGroupMembers(Map<String, GroupMember> map) {
        for (GroupMember node : map.values()) {
            log.info("{}", node);
        }
    }

    public static void assertSinglePrimaryGR(GroupMember member, String type) {
        assertEquals(type, member.role());
        assertEquals("ONLINE", member.state());
        assertEquals("3306", member.port());
        assertEquals("XCom", member.communicationStack());
    }

}
