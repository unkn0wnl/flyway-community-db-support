/*
 * Copyright (C) Red Gate Software Ltd 2010-2024
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.flywaydb.community.database.clickhouse;

import org.flywaydb.core.internal.database.base.Schema;
import org.flywaydb.core.internal.jdbc.JdbcTemplate;
import org.flywaydb.core.internal.util.StringUtils;

import java.sql.SQLException;
import java.util.List;
import java.util.Optional;

public class ClickHouseSchema extends Schema<ClickHouseDatabase, ClickHouseTable> {

    public static final String SYSTEM_SCHEMA = "system";

    /**
     * @param jdbcTemplate The Jdbc Template for communicating with the DB.
     * @param database The database-specific support.
     * @param name The name of the schema.
     */
    public ClickHouseSchema(JdbcTemplate jdbcTemplate, ClickHouseDatabase database, String name) {
        super(jdbcTemplate, database, name);
    }

    @Override
    protected boolean doExists() throws SQLException {
        database.useSchema(SYSTEM_SCHEMA);
        int i = jdbcTemplate.queryForInt("SELECT COUNT() FROM system.databases WHERE name = ?", name);
        database.restoreOriginalSchema();
        return i > 0;
    }

    @Override
    protected boolean doEmpty() throws SQLException {
        database.useSchema(SYSTEM_SCHEMA);
        int i = jdbcTemplate.queryForInt("SELECT COUNT() FROM system.tables WHERE database = ?", name);
        database.restoreOriginalSchema();
        return i == 0;
    }

    @Override
    protected void doCreate() throws SQLException {
        database.useSchema(SYSTEM_SCHEMA);
        String clusterName = database.getClusterName();
        boolean isClustered = StringUtils.hasText(clusterName);
        jdbcTemplate.executeStatement("CREATE DATABASE " + database.quote(name) + (isClustered ? (" ON CLUSTER " + clusterName) : ""));
        database.restoreOriginalSchema();
    }

    @Override
    protected void doDrop() throws SQLException {
        if (database.getMainConnection().getCurrentSchemaNameOrSearchPath().equals(name)) {
            String schema = Optional.ofNullable(database.getConfiguration().getDefaultSchema()).orElse(SYSTEM_SCHEMA);
            database.useSchema(schema);
        }
        String clusterName = database.getClusterName();
        boolean isClustered = StringUtils.hasText(clusterName);
        jdbcTemplate.executeStatement("DROP DATABASE " + database.quote(name) + (isClustered ? (" ON CLUSTER " + clusterName) : ""));
    }

    @Override
    protected void doClean() throws SQLException {
        for (ClickHouseTable table : allTables()) {
            table.drop();
        }
    }

    @Override
    protected ClickHouseTable[] doAllTables() throws SQLException {
        database.useSchema(SYSTEM_SCHEMA);
        List<String> tables = jdbcTemplate.queryForStringList("SELECT name FROM system.tables WHERE database = ?", name);
        database.restoreOriginalSchema();
        return tables.stream()
                .map(this::getTable)
                .toArray(ClickHouseTable[]::new);
    }

    @Override
    public ClickHouseTable getTable(String tableName) {
        return new ClickHouseTable(jdbcTemplate, database, this, tableName);
    }
}
