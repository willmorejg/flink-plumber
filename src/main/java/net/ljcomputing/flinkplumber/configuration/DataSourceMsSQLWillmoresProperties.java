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
package net.ljcomputing.flinkplumber.configuration;

import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableDescriptor;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/** MS SQL Server database configuration options. */
@Configuration
@ConfigurationProperties(prefix = "application.datasource.mssql.willmores")
public class DataSourceMsSQLWillmoresProperties extends DataSourcePropertiesBase {
    /**
     * MS SQL Server JDBC Connection options.
     *
     * @return
     */
    @Bean
    public JdbcConnectionOptions msSqlConnectionOptions() {
        return new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                .withUrl(url)
                .withDriverName(driverName)
                .withUsername(username)
                .withPassword(password)
                .build();
    }

    @Bean
    public TableDescriptor msSqlWillmores() {
        final Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.STRING())
                        .column("given_name", DataTypes.STRING())
                        .column("middle_name", DataTypes.STRING())
                        .column("surname", DataTypes.STRING())
                        .column("suffix", DataTypes.STRING())
                        .column("created_at", DataTypes.TIMESTAMP())
                        .column("updated_at", DataTypes.TIMESTAMP())
                        .build();
        final TableDescriptor descriptor =
                TableDescriptor.forConnector("jdbc")
                        .option("table-name", "willmores")
                        .option("url", url)
                        .option("driver", driverName)
                        .schema(schema)
                        .build();
        return descriptor;
    }
}
