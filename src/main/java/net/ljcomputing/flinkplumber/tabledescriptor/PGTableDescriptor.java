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
package net.ljcomputing.flinkplumber.tabledescriptor;

import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableDescriptor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class PGTableDescriptor implements TableDescriptorBean {
    @Autowired private JdbcConnectionOptions postgresConnectionOptions;

    @Override
    public String getTableDescriptorName() {
        return DefinedTableDescriptors.POSTGRES.getName();
    }

    @Override
    public TableDescriptor getTableDescriptor(final Schema schema, final String tablename) {
        return TableDescriptor.forConnector("jdbc")
                .option("table-name", tablename)
                .option("url", postgresConnectionOptions.getDbURL())
                .option("driver", postgresConnectionOptions.getDriverName())
                .schema(schema)
                .build();
    }
}
