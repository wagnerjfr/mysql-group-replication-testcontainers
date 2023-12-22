package com.example.mysqlgroupreplicationtestcontainers;

import com.example.mysqlgroupreplicationtestcontainers.groupmember.GroupMember;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.TestMethodOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.test.context.SpringBootTest;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Slf4j
@SpringBootTest
@Testcontainers
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class SinglePrimaryMySQLGroupReplicationTest {

	private static final Logger logger = LoggerFactory.getLogger(SinglePrimaryMySQLGroupReplicationTest.class);

	private static final String DOCKER_IMAGE = "mysql:8.0";
	private static final String HOSTNAME_PREFIX = "node";
	private static final String DATABASE_NAME = "Testcontainers";
	private static int testCount;
	private static boolean groupBootstrapped, nodesJoined;


	static Network network = Network.newNetwork();

	static MySQLContainer<?> mySQLContainer1 = getContainer(1);

	static MySQLContainer<?> mySQLContainer2 = getContainer(2);

	static MySQLContainer<?> mySQLContainer3 = getContainer(3);

	static MySQLService node1, node2, node3;

	@BeforeAll
	static void startDbs() throws SQLException {
		node1 = startMySQLService("1", mySQLContainer1);
		node2 = startMySQLService("2", mySQLContainer2);
		node3 = startMySQLService("3", mySQLContainer3);

		List<MySQLService> nodes = Arrays.asList(node1, node2, node3);
		for (MySQLService node : nodes) {
			node.resetMaster();
			assertTrue(node.getGlobalGtidExecuted().isEmpty(), "There are transactions in the database");
		}

		for (MySQLService node : nodes) {
			node.installGroupReplicationPlugIn();
			assertTrue(node.showPlugins().containsKey("group_replication"), "Plugin not found in the set");
		}
	}

	@AfterAll
	static void stopDbs(){
		stopMySQLService(mySQLContainer1);
		stopMySQLService(mySQLContainer2);
		stopMySQLService(mySQLContainer3);
		network.close();
	}

	@BeforeEach
	void beforeEach(TestInfo testInfo) {
		log.info("Test {}: {}", ++testCount, testInfo.getDisplayName());
	}

	@Test
	@Order(1)
	@DisplayName("Bootstrap GR")
	public void bootStrapGroupReplication() throws SQLException {
		assertTrue(mySQLContainer1.isRunning());

		assertEquals(node1.getId(), node1.getServerId());

		try {
			node1.bootStrapGroupReplication();
		} catch (Exception e) {
			log.error(mySQLContainer1.getLogs());
			throw e;
		}

		Map<String, GroupMember> map = node1.getPerformanceSchemaReplicationGroupMembers();
		assertEquals(1, map.keySet().size(), "It's expected to have just 1 member");

		GroupMember member = map.get(node1.getHostName());
		assertEquals(node1.getHostName(), member.host(), "Hostname is different");
		assertSinglePrimaryGR(member, "PRIMARY");
		groupBootstrapped = true;
	}

	@Test
	@Order(2)
	@DisplayName("Joining more nodes")
	public void joiningOtherNodes() throws SQLException {
		Assumptions.assumeTrue(groupBootstrapped);

		assertTrue(mySQLContainer2.isRunning());
		assertEquals(node2.getId(), node2.getServerId());
		try {
			node2.joinGroupReplication();
		} catch (Exception e) {
			log.error(mySQLContainer2.getLogs());
			throw e;
		}

		assertEquals(node3.getId(), node3.getServerId());
		try {
			node3.joinGroupReplication();
		} catch (Exception e) {
			log.error(mySQLContainer3.getLogs());
			throw e;
		}

		Map<String, GroupMember> map = node1.getPerformanceSchemaReplicationGroupMembers();
		for (MySQLService node : Arrays.asList(node2, node3)) {
			GroupMember member = map.get(node.getHostName());
			assertEquals(node.getHostName(), member.host(), "Hostname is different");
			assertSinglePrimaryGR(member, "SECONDARY");
		}
		nodesJoined = true;
	}

	@Test
	@Order(3)
	@DisplayName("Try to create Database in Secondaries")
	public void creatingDatabaseInSecondaries() {
		Assumptions.assumeTrue(nodesJoined);

		for (MySQLService mySQLService : Arrays.asList(node2, node3)) {
			Exception exception = assertThrows(SQLException.class, () -> mySQLService.createDatabase(DATABASE_NAME));
			assertEquals("The MySQL server is running with the --super-read-only option so it cannot execute this statement", exception.getMessage());
		}
	}

	@Test
	@Order(4)
	@DisplayName("Creating Database in Primary")
	public void creatingDatabaseInPrimary() throws SQLException {
		Assumptions.assumeTrue(nodesJoined);

		for (MySQLService mySQLService : Arrays.asList(node1, node2, node3)) {
			assertFalse(mySQLService.showDatabases().contains(DATABASE_NAME), "Database exists.");
		}

		node1.createDatabase(DATABASE_NAME);
		Set<String> databases = node1.showDatabases();
		log.info("Primary databases: {}", databases);
		assertTrue(databases.contains(DATABASE_NAME), "Database doesn't exist.");
	}

	@Test
	@Order(5)
	@DisplayName("Checking database in Secondaries")
	public void checkDatabaseInSecondaries() throws SQLException {
		Assumptions.assumeTrue(nodesJoined);

		for (MySQLService mySQLService : Arrays.asList(node2, node3)) {
			Set<String> databases = mySQLService.showDatabases();
			assertTrue(databases.contains(DATABASE_NAME), "Database doesn't exist.");
			log.info("Secondary databases: {}", databases);
		}
	}

	private static MySQLService startMySQLService(String id, MySQLContainer<?> container) {
		container.start();
		String url = container.getJdbcUrl();
		ConnectionPool connectionPool =  new ConnectionPool(url, container.getUsername(), container.getPassword());
		return new MySQLService(id, HOSTNAME_PREFIX, connectionPool);
	}

	private static void stopMySQLService(MySQLContainer<?> container) {
		if (container != null) {
			container.stop();
		}
	}

	private static MySQLContainer<?> getContainer(int id) {
		return new MySQLContainer<>(DOCKER_IMAGE)
				//.withLogConsumer(new Slf4jLogConsumer(logger))
				.withCommand(getCommand(id))
				.withUsername("root")
				.withPassword("mypass")
				.withCreateContainerCmdModifier(it -> it.withHostName(HOSTNAME_PREFIX + id))
				.withNetwork(network);
	}

	private static String getCommand(int id) {
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
				"--loose-group-replication-group-seeds=node1:33061,node2:33061,node3:33061 " +
				"--loose-group-replication-single-primary-mode=ON " +
				"--loose-group-replication-enforce-update-everywhere-checks=OFF", id, HOSTNAME_PREFIX + id);
	}

	private void assertSinglePrimaryGR(GroupMember member, String type) {
		assertEquals(type, member.role());
		assertEquals("ONLINE", member.state());
		assertEquals("3306", member.port());
		assertEquals("XCom", member.communicationStack());
	}
}
