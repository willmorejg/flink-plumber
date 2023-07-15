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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableDescriptor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/** A table descriptor bean factory. */
@Component
@Slf4j
public class TableDescriptorBeanFactory {
    /** A Map of TableDescriptors. */
    private final Map<String, TableDescriptorBean> tableDescriptors = new HashMap<>();

    /**
     * Constructor. Initialization of the Map of TableDescriptors.
     *
     * @param beans
     */
    public TableDescriptorBeanFactory(@Autowired final List<TableDescriptorBean> beans) {
        for (final TableDescriptorBean current : beans) {
            tableDescriptors.put(current.getTableDescriptorName(), current);
        }
    }

    /**
     * Locate the TableDescriptor associated with the given definied TableDescriptor.
     *
     * @param definedTableDescriptor
     * @param schema
     * @param tablename
     * @return
     */
    public TableDescriptor locate(
            final DefinedTableDescriptors definedTableDescriptor,
            final Schema schema,
            final String tablename) {
        return locate(definedTableDescriptor.getName(), schema, tablename);
    }

    /**
     * Locate the TableDescriptor associated with the given definied TableDescriptor.
     *
     * @param definedTableDescriptor
     * @param schema
     * @param tablename
     * @return
     */
    public TableDescriptor locate(
            final String definedTableDescriptor, final Schema schema, final String tablename) {
        final TableDescriptorBean result = tableDescriptors.get(definedTableDescriptor);

        if (result == null) {
            log.warn("No TableDescriptor defined: {}", definedTableDescriptor);
            return null;
        }

        return result.getTableDescriptor(schema, tablename);
    }
}
