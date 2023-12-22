package com.example.mysqlgroupreplicationtestcontainers;

import com.example.mysqlgroupreplicationtestcontainers.groupmember.GroupMember;
import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Slf4j
public class MySQLService {
    private final ConnectionPool connectionPool;

    public MySQLService(ConnectionPool connectionPool) {
        this.connectionPool = connectionPool;
    }

    public String getServerId() throws SQLException {
        try (Connection connection = this.connectionPool.getConnection()) {
            String query = "SELECT @@server_id as SERVER_ID;";
            PreparedStatement ps = connection.prepareStatement(query);
            ResultSet rs = ps.executeQuery();
            rs.next();
            return rs.getString("server_id");
        }
    }

    public void createDatabase(String name) throws SQLException {
        String createUserSql = "CREATE DATABASE " + name;
        try (Connection connection = this.connectionPool.getConnection()) {
            Statement createStatement = connection.createStatement();
            createStatement.execute(createUserSql);
        }
    }

    public Set<String> showDatabases() throws SQLException {
        try (Connection connection = this.connectionPool.getConnection()) {
            String query = "SHOW DATABASES;";
            PreparedStatement ps = connection.prepareStatement(query);
            ResultSet rs = ps.executeQuery();

            Set<String> set = new HashSet<>();
            while (rs.next()) {
                set.add(rs.getString("Database"));
            }
            return set;
        }
    }

    public void installGroupReplicationPlugIn() throws SQLException {
        try (Connection connection = this.connectionPool.getConnection()) {
            String query = "INSTALL PLUGIN group_replication SONAME 'group_replication.so';";
            PreparedStatement ps = connection.prepareStatement(query);
            ps.executeUpdate();
        }
    }

    public void resetMaster() throws SQLException {
        try (Connection connection = this.connectionPool.getConnection()) {
            String query = "RESET MASTER;";
            PreparedStatement ps = connection.prepareStatement(query);
            ps.executeUpdate();
        }
    }

    public String getGlobalGtidExecuted() throws SQLException {
        try (Connection connection = this.connectionPool.getConnection()) {
            String query = "SELECT @@global.gtid_executed;";
            PreparedStatement ps = connection.prepareStatement(query);
            ResultSet rs = ps.executeQuery();
            rs.next();
            return rs.getString(1);
        }
    }

    public void bootStrapGroupReplication() throws SQLException {
        try (Connection connection = this.connectionPool.getConnection()) {
            List<String> queries = new ArrayList<>();
            queries.add("SET @@GLOBAL.group_replication_bootstrap_group=1;");
            queries.add("create user 'repl'@'%';");
            queries.add("GRANT REPLICATION SLAVE ON *.* TO repl@'%';");
            queries.add("flush privileges;");
            queries.add("change master to master_user='repl' for channel 'group_replication_recovery';");
            queries.add("START GROUP_REPLICATION;");
            queries.add("SET @@GLOBAL.group_replication_bootstrap_group=0;");

            for (String query : queries) {
                PreparedStatement ps = connection.prepareStatement(query);
                ps.executeUpdate();
            }
        }
    }

    public void joinGroupReplication() throws SQLException {
        try (Connection connection = this.connectionPool.getConnection()) {
            List<String> queries = new ArrayList<>();
            queries.add("change master to master_user='repl' for channel 'group_replication_recovery';");
            queries.add("START GROUP_REPLICATION;");

            for (String query : queries) {
                PreparedStatement ps = connection.prepareStatement(query);
                ps.executeUpdate();
            }
        }
    }

    public Map<String, String> showPlugins() throws SQLException {
        try (Connection connection = this.connectionPool.getConnection()) {
            String query = "SHOW PLUGINS;";
            PreparedStatement ps = connection.prepareStatement(query);
            ResultSet rs = ps.executeQuery();

            Map<String, String> map = new HashMap<>();
            while (rs.next()) {
                map.put(rs.getString("Name"), rs.getString("Status"));
            }
            return map;
        }
    }

    public Map<String, GroupMember> getPerformanceSchemaReplicationGroupMembers() throws SQLException {
        try (Connection connection = this.connectionPool.getConnection()) {
            String query = "SELECT * FROM performance_schema.replication_group_members;";
            PreparedStatement ps = connection.prepareStatement(query);
            ResultSet rs = ps.executeQuery();

            Map<String, GroupMember> map = new HashMap<>();
            while (rs.next()) {
                GroupMember member = GroupMember.create(rs);
                map.put(member.host(), member);
            }
            return map;
        }
    }
}
