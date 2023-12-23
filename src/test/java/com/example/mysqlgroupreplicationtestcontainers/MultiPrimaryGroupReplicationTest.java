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
import static org.junit.jupiter.api.Assertions.assertTrue;

@Slf4j
@SpringBootTest
@Testcontainers
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class MultiPrimaryGroupReplicationTest {

	private static final String DOCKER_IMAGE = "mysql:8.0";
	private static final String HOSTNAME_PREFIX = "node";
	private static final boolean SINGLE_PRIMARY = false;
	private static final List<String> DATABASE_NAME_LIST = Arrays.asList("DB1", "DB2", "DB3");
	private static int testCount;
	private static boolean groupBootstrapped, nodesJoined;
	private static List<MySQLServer> nodes;


	static Network network = Network.newNetwork();

	static MySQLContainer<?> mySQLContainer1 = Utils.getContainer(DOCKER_IMAGE, 1, HOSTNAME_PREFIX, SINGLE_PRIMARY, network);

	static MySQLContainer<?> mySQLContainer2 = Utils.getContainer(DOCKER_IMAGE, 2, HOSTNAME_PREFIX, SINGLE_PRIMARY, network);

	static MySQLContainer<?> mySQLContainer3 = Utils.getContainer(DOCKER_IMAGE, 3, HOSTNAME_PREFIX, SINGLE_PRIMARY, network);

	static MySQLServer node1, node2, node3;

	@BeforeAll
	static void startDbs() throws SQLException {
		node1 = Utils.startMySQLService("1", HOSTNAME_PREFIX, mySQLContainer1);
		node2 = Utils.startMySQLService("2", HOSTNAME_PREFIX, mySQLContainer2);
		node3 = Utils.startMySQLService("3", HOSTNAME_PREFIX, mySQLContainer3);

		nodes = Arrays.asList(node1, node2, node3);
		for (MySQLServer node : nodes) {
			node.resetMaster();
			assertTrue(node.getGlobalGtidExecuted().isEmpty(), "There are transactions in the database");
		}

		for (MySQLServer node : nodes) {
			node.installGroupReplicationPlugIn();
			assertTrue(node.showPlugins().containsKey("group_replication"), "Plugin not found in the set");
		}
	}

	@AfterAll
	static void stopDbs(){
		Utils.stopMySQLService(mySQLContainer1);
		Utils.stopMySQLService(mySQLContainer2);
		Utils.stopMySQLService(mySQLContainer3);
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
		Utils.assertSinglePrimaryGR(member, "PRIMARY");
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
		assertEquals(3, map.keySet().size(), "It's expected to have 3 members");

		for (MySQLServer node : Arrays.asList(node2, node3)) {
			GroupMember member = map.get(node.getHostName());
			assertEquals(node.getHostName(), member.host(), "Hostname is different");
			Utils.assertSinglePrimaryGR(member, "PRIMARY");
		}
		nodesJoined = true;

		Utils.printGroupMembers(map);
	}

	@Test
	@Order(3)
	@DisplayName("Creating databases")
	public void creatingDatabaseInPrimaries() throws SQLException {
		Assumptions.assumeTrue(nodesJoined);

		for (MySQLServer node : nodes) {
			for (String database : DATABASE_NAME_LIST) {
				assertFalse(node.showDatabases().contains(database), "Database exists.");
			}
		}

		for (MySQLServer node : nodes) {
			final int idx = nodes.indexOf(node);
			final String databaseName = DATABASE_NAME_LIST.get(idx);
			node.createDatabase(databaseName);
			Set<String> databases = node.showDatabases();
			assertTrue(databases.contains(databaseName), "Database doesn't exist.");
		}
	}

	@Test
	@Order(4)
	@DisplayName("Checking whether databases are replicated")
	public void checkDatabaseInPrimaries() throws SQLException {
		Assumptions.assumeTrue(nodesJoined);

		for (MySQLServer node : nodes) {
			Set<String> databases = node.showDatabases();
			assertTrue(databases.containsAll(DATABASE_NAME_LIST), "Node doesn't have all databases");
		}
	}
}
