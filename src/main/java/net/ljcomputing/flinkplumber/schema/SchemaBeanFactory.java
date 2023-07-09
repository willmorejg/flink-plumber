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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.table.api.Schema;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/** A schema bean factory. */
@Component
@Slf4j
public class SchemaBeanFactory {
    /** A Map of schemas. */
    final Map<String, SchemaBean> schemas = new HashMap<>();

    /**
     * Constructor. Initialization of the Map of schemas.
     *
     * @param beans
     */
    public SchemaBeanFactory(@Autowired final List<SchemaBean> beans) {
        for (final SchemaBean current : beans) {
            schemas.put(current.getSchemaName(), current);
        }
    }

    /**
     * Locate the schema associated with the given definied schema.
     *
     * @param definedSchema
     * @return
     */
    public Schema locate(final DefinedSchemas definedSchema) {
        return locate(definedSchema.getName());
    }

    /**
     * Locate the schema associated with the given definied schema.
     *
     * @param definedSchema
     * @return
     */
    public Schema locate(final String definedSchema) {
        final SchemaBean result = schemas.get(definedSchema);

        if (result == null) {
            log.warn("No schema defined: {}", definedSchema);
            return null;
        }

        return result.getSchema();
    }
}
