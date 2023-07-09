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

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableDescriptor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/** MS SQL Server database configuration options. */
@Configuration
public class DataSourceAddlDbProperties extends DataSourcePropertiesBase {

    /**
     * Table descriptor to generate people data using the Flink datagen connector.
     *
     * @return
     */
    @Bean
    public TableDescriptor datagenPeopleTableDescriptor() {
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
                TableDescriptor.forConnector("datagen")
                        .option("number-of-rows", "10")
                        .schema(schema)
                        .build();
        return descriptor;
    }

    /**
     * Table descriptor for a person avro data source.
     *
     * @return
     */
    @Bean
    public TableDescriptor avroPeopleTableDescriptor() {
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
                TableDescriptor.forConnector("filesystem")
                        .option(
                                "path",
                                "file:///home/jim/eclipse-workspace/net.ljcomputing/flink-plumber/src/test/resources/out/avro")
                        .option("format", "avro")
                        .option("avro.codec", "null")
                        .schema(schema)
                        .build();
        return descriptor;
    }
}
