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
package net.ljcomputing.flinkplumber.schema;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.springframework.stereotype.Component;

@Component
public class PolicySchema implements SchemaBean {

    @Override
    public String getSchemaName() {
        return DefinedSchemas.POLICY.getName();
    }

    @Override
    public Schema getSchema() {
        return Schema.newBuilder()
                .column("policy_id", DataTypes.INT())
                .column("policy_uuid", DataTypes.STRING())
                .column("policy_number", DataTypes.STRING())
                .column("date_code", DataTypes.INT())
                .column("status", DataTypes.STRING())
                .column("effective_date", DataTypes.DATE())
                .column("expiration_date", DataTypes.DATE())
                .column("created_at", DataTypes.TIMESTAMP(3))
                .column("updated_at", DataTypes.TIMESTAMP(3))
                .build();
    }
}
