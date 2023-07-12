/*
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.

James G Willmore - LJ Computing - (C) 2023
*/
package net.ljcomputing.flinkplumber;

import static org.junit.jupiter.api.extension.ExtensionContext.Namespace.GLOBAL;

import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableDescriptor;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.MSSQLServerContainer;
import org.testcontainers.utility.DockerImageName;

class FlinkTestContainers implements BeforeAllCallback, ExtensionContext.Store.CloseableResource {
    private static final Logger log = LoggerFactory.getLogger(FlinkTestContainers.class);

    public static Boolean containersStarted = Boolean.FALSE;

    private static String url;

    public static MSSQLServerContainer mssqlserver;

    public static MSSQLServerDS mSSQLServerDS = new MSSQLServerDS();

    @Override
    public void beforeAll(ExtensionContext context) throws Exception {
        log.info("starting containers");

        if (!containersStarted) {
            DockerImageName myImage =
                    DockerImageName.parse("mcr.microsoft.com/mssql/server:2022-latest")
                            .asCompatibleSubstituteFor("mcr.microsoft.com/mssql/server");
            mssqlserver = new MSSQLServerContainer(myImage).acceptLicense();
            mssqlserver.withEnv("MSSQL_SA_PASSWORD", "P@ssW0rd");
            // mssqlserver.withUsername("sa");
            mssqlserver.withPassword("P@ssW0rd");
            mssqlserver.withInitScript("sql/mssql_ddl.sql");
            mssqlserver.addExposedPorts(1433);
            mssqlserver.withExposedPorts(1433);
            mssqlserver.start();
            log.debug("mssqlserver.getJdbcUrl(): {}", mssqlserver.getJdbcUrl());
            url =
                    mssqlserver.getJdbcUrl()
                            + ";databaseName=willmores;trustServerCertificate=true;user=sa;password=P@ssW0rd";
            containersStarted = true;
            context.getRoot().getStore(GLOBAL).put("testcontainers-started", this);
        }

        log.info("starting containers - DONE");
    }

    @Override
    public void close() throws Throwable {
        log.info("shuting down containers");
        mssqlserver.close();
        containersStarted = false;
    }

    public static class MSSQLServerDS {
        public JdbcConnectionOptions msSqlConnectionOptions() {
            log.warn("url: {}", url);
            return new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                    .withUrl(url)
                    .withDriverName("com.microsoft.sqlserver.jdbc.SQLServerDriver")
                    .withUsername("sa")
                    .withPassword("P@ssW0rd")
                    .build();
        }

        public TableDescriptor msSqlWillmores() {
            log.warn("url: {}", url);
            final Schema schema =
                    Schema.newBuilder()
                            .column("id", DataTypes.STRING())
                            .column("given_name", DataTypes.STRING())
                            .column("middle_name", DataTypes.STRING())
                            .column("surname", DataTypes.STRING())
                            .column("suffix", DataTypes.STRING())
                            .column("created_at", DataTypes.TIMESTAMP(3))
                            .column("updated_at", DataTypes.TIMESTAMP(3))
                            .build();
            final TableDescriptor descriptor =
                    TableDescriptor.forConnector("jdbc")
                            .option("table-name", "willmores")
                            .option("url", url)
                            .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
                            .schema(schema)
                            .build();
            return descriptor;
        }
    }
}
