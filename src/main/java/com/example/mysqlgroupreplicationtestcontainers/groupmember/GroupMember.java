package com.example.mysqlgroupreplicationtestcontainers.groupmember;

import java.sql.ResultSet;
import java.sql.SQLException;

public record GroupMember(String id, String channelName, String host, String port, String state, String role,
                          String version, String communicationStack) {

    public static GroupMember create(ResultSet rs) throws SQLException {
        String id = rs.getString(Variable.MEMBER_ID.name());
        String channelName = rs.getString(Variable.CHANNEL_NAME.name());
        String host = rs.getString(Variable.MEMBER_HOST.name());
        String port = rs.getString(Variable.MEMBER_PORT.name());
        String state = rs.getString(Variable.MEMBER_STATE.name());
        String role = rs.getString(Variable.MEMBER_ROLE.name());
        String version = rs.getString(Variable.MEMBER_VERSION.name());
        String communicationStack = rs.getString(Variable.MEMBER_COMMUNICATION_STACK.name());

        return new GroupMember(id, channelName, host, port, state, role, version, communicationStack);
    }
}
