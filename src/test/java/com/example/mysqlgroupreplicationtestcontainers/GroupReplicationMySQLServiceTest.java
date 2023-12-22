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
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Slf4j
@SpringBootTest
@Testcontainers
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class GroupReplicationMySQLServiceTest {

	private static final Logger logger = LoggerFactory.getLogger(GroupReplicationMySQLServiceTest.class);

	private static final String DOCKER_IMAGE = "mysql:8.0";
	private static final String DATABASE_NAME = "Testcontainers";
	private static int testCount;
	private static boolean groupBootstrapped, nodesJoined;


	static Network network = Network.newNetwork();

	static MySQLContainer<?> mySQLContainer1 = getContainer(Node.Node1);

	static MySQLContainer<?> mySQLContainer2 = getContainer(Node.Node2);

	static MySQLContainer<?> mySQLContainer3 = getContainer(Node.Node3);

	static MySQLService mySQLService1, mySQLService2, mySQLService3;

	@BeforeAll
	static void startDbs() throws SQLException {
		mySQLService1 = startMySQLService(mySQLContainer1);
		mySQLService2 = startMySQLService(mySQLContainer2);
		mySQLService3 = startMySQLService(mySQLContainer3);

		for (MySQLService mySQLService : Arrays.asList(mySQLService1, mySQLService2, mySQLService3)) {
			mySQLService.resetMaster();
			assertTrue(mySQLService.getGlobalGtidExecuted().isEmpty());
		}

		for (MySQLService mySQLService : Arrays.asList(mySQLService1, mySQLService2, mySQLService3)) {
			mySQLService.installGroupReplicationPlugIn();
			assertTrue(mySQLService.showPlugins().containsKey("group_replication"));
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

		final Node node = Node.Node1;
		assertEquals(node.id, mySQLService1.getServerId());

		try {
			mySQLService1.bootStrapGroupReplication();
		} catch (Exception e) {
			log.error(mySQLContainer1.getLogs());
			throw e;
		}

		Map<String, GroupMember> map = mySQLService1.getPerformanceSchemaReplicationGroupMembers();
		assertEquals(1, map.keySet().size(), "It's expected to have just 1 member");
		GroupMember member = map.get(node.hostName);
		assertEquals(node.hostName, member.host());
		assertSinglePrimaryGR(member, "PRIMARY");
		groupBootstrapped = true;
	}

	@Test
	@Order(2)
	@DisplayName("Joining more nodes")
	public void joiningOtherNodes() throws SQLException {
		Assumptions.assumeTrue(groupBootstrapped);

		assertTrue(mySQLContainer2.isRunning());
		assertEquals(Node.Node2.id, mySQLService2.getServerId());
		try {
			mySQLService2.joinGroupReplication();
		} catch (Exception e) {
			log.error(mySQLContainer2.getLogs());
			throw e;
		}

		assertEquals(Node.Node3.id, mySQLService3.getServerId());
		try {
			mySQLService3.joinGroupReplication();
		} catch (Exception e) {
			log.error(mySQLContainer3.getLogs());
			throw e;
		}

		Map<String, GroupMember> map = mySQLService1.getPerformanceSchemaReplicationGroupMembers();
		for (Node node : Arrays.asList(Node.Node2, Node.Node3)) {
			GroupMember member = map.get(node.hostName);
			assertEquals(node.hostName, member.host());
			assertSinglePrimaryGR(member, "SECONDARY");
		}
		nodesJoined = true;
	}

	@Test
	@Order(3)
	@DisplayName("Try to create Database in Secondaries")
	public void creatingDatabaseInSecondaries() {
		Assumptions.assumeTrue(nodesJoined);

		for (MySQLService mySQLService : Arrays.asList(mySQLService2, mySQLService3)) {
			Exception exception = assertThrows(SQLException.class, () -> mySQLService.createDatabase(DATABASE_NAME));
			assertEquals("The MySQL server is running with the --super-read-only option so it cannot execute this statement", exception.getMessage());
		}
	}

	@Test
	@Order(4)
	@DisplayName("Creating Database in Primary")
	public void creatingDatabaseInPrimary() throws SQLException {
		Assumptions.assumeTrue(nodesJoined);

		for (MySQLService mySQLService : Arrays.asList(mySQLService1, mySQLService2, mySQLService3)) {
			assertFalse(mySQLService.showDatabases().contains(DATABASE_NAME), "Database exists.");
		}

		mySQLService1.createDatabase(DATABASE_NAME);
		Set<String> databases = mySQLService1.showDatabases();
		log.info("Source databases: {}", databases);
		assertTrue(databases.contains(DATABASE_NAME), "Database doesn't exist.");
	}

	@Test
	@Order(5)
	@DisplayName("Checking database in Secondaries")
	public void checkDatabaseInSecondaries() throws SQLException {
		Assumptions.assumeTrue(nodesJoined);

		for (MySQLService mySQLService : Arrays.asList(mySQLService2, mySQLService3)) {
			Set<String> databases = mySQLService.showDatabases();
			assertTrue(databases.contains(DATABASE_NAME), "Database doesn't exist.");
			log.info("Source databases: {}", databases);
		}
	}

	private static MySQLService startMySQLService(MySQLContainer<?> container) {
		container.start();
		String url = container.getJdbcUrl();
		ConnectionPool connectionPool =  new ConnectionPool(url, container.getUsername(), container.getPassword());
		return new MySQLService(connectionPool);
	}

	private static void stopMySQLService(MySQLContainer<?> container) {
		if (container != null) {
			container.stop();
		}
	}

	private static MySQLContainer<?> getContainer(final Node node) {
		return new MySQLContainer<>(DOCKER_IMAGE)
				//.withLogConsumer(new Slf4jLogConsumer(logger))
				.withCommand(getCommand(node))
				.withUsername("root")
				.withPassword("mypass")
				.withCreateContainerCmdModifier(it -> it.withHostName(node.hostName))
				.withNetwork(network);
	}

	private static String getCommand(Node node) {
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
				"--loose-group-replication-enforce-update-everywhere-checks=OFF", node.id, node.hostName);
	}

	private void assertSinglePrimaryGR(GroupMember member, String type) {
		assertEquals(type, member.role());
		assertEquals("ONLINE", member.state());
		assertEquals("3306", member.port());
		assertEquals("XCom", member.communicationStack());
	}

	private enum Node {
		Node1("1", "node1"), Node2("2", "node2"), Node3("3", "node3");

		private final String id;
		private final String hostName;
		Node(String id, String hostName) {
			this.id = id;
			this.hostName = hostName;
		}
	}
}
