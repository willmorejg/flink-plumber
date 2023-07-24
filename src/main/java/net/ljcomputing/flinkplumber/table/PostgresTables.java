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
package net.ljcomputing.flinkplumber.table;

import net.ljcomputing.flinkplumber.schema.DefinedSchemas;
import net.ljcomputing.flinkplumber.tabledescriptor.AbstractTableDescriptor;
import net.ljcomputing.flinkplumber.tabledescriptor.DefinedTableDescriptors;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableDescriptor;
import org.springframework.stereotype.Component;

/** Pre-determined PostgreSQL Table Descriptors. */
@Component
public class PostgresTables extends AbstractTableDescriptor {
    /**
     * Policy Table Descriptor.
     * 
     * @return
     */
    public TableDescriptor policy() {
        final String tablename = DefinedSchemas.POLICY.getName();
        final Schema schema = schemaFactory.locate(DefinedSchemas.POLICY);
        return tableDescriptorFactory.locate(DefinedTableDescriptors.POSTGRES, schema, tablename);
    }

    /**
     * "willmores" Table Descriptor.
     * 
     * @return
     */
    public TableDescriptor willmores() {
        final String tablename = DefinedSchemas.WILLMORES.getName();
        final Schema schema = schemaFactory.locate(DefinedSchemas.WILLMORES);
        return tableDescriptorFactory.locate(DefinedTableDescriptors.POSTGRES, schema, tablename);
    }
}
