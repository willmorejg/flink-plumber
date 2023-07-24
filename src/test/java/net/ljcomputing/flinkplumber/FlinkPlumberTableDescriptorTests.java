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

import static org.junit.Assert.assertNotNull;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.junit.jupiter.api.MethodOrderer.OrderAnnotation;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import net.ljcomputing.flinkplumber.schema.DefinedSchemas;
import net.ljcomputing.flinkplumber.schema.SchemaBeanFactory;
import net.ljcomputing.flinkplumber.table.CsvTables;
import net.ljcomputing.flinkplumber.table.PostgresTables;
import net.ljcomputing.flinkplumber.tabledescriptor.DefinedTableDescriptors;
import net.ljcomputing.flinkplumber.tabledescriptor.TableDescriptorBeanFactory;

@SpringBootTest
@TestMethodOrder(OrderAnnotation.class)
@Order(40)
@ActiveProfiles("test")
class FlinkPlumberTableDescriptorTests {
    private static final Logger log = LoggerFactory.getLogger(FlinkPlumberDatabaseTests.class);

    @Autowired private StreamTableEnvironment streamTableEnvironment;

    @Autowired private SchemaBeanFactory schemaFactory;

    @Autowired private TableDescriptorBeanFactory tableDescriptorFactory;

    @Autowired private PostgresTables postgresTables;

    @Autowired private CsvTables csvTables;

    /** Test factory. */
    @Test
    @Order(1)
    void testFactory() {
        assertNotNull(schemaFactory.locate(DefinedSchemas.POLICY));
        assertNotNull(schemaFactory.locate(DefinedSchemas.RISK));
        assertNotNull(
                tableDescriptorFactory.locate(
                        DefinedTableDescriptors.POSTGRES,
                        schemaFactory.locate(DefinedSchemas.POLICY),
                        DefinedSchemas.POLICY.getName()));
        assertNotNull(postgresTables);
        assertNotNull(postgresTables.policy());
    }

    /** Test using table descriptor. */
    @Test
    @Order(10)
    void testTableDescriptor() {
        final String pgPolicy = DefinedTableDescriptors.POSTGRES + DefinedSchemas.POLICY.getName();
        final String csvPolicy = DefinedTableDescriptors.CSV + DefinedSchemas.POLICY.getName();

        streamTableEnvironment.createTable(pgPolicy, postgresTables.policy());
        streamTableEnvironment.createTable(csvPolicy, csvTables.policy());
        streamTableEnvironment.executeSql(
                "INSERT INTO " + csvPolicy + " SELECT * FROM " + pgPolicy);
        streamTableEnvironment.executeSql("SELECT * FROM " + csvPolicy).print();
    }

    /** Test using table descriptors defining different schemas. */
    @Test
    @Order(11)
    void testTableDescriptor2() {
        final String pgPolicy =
                DefinedTableDescriptors.POSTGRES + DefinedSchemas.WILLMORES.getName();
        final String csvPolicy = DefinedTableDescriptors.CSV + DefinedSchemas.FULLNAME.getName();

        streamTableEnvironment.createTable(pgPolicy, postgresTables.willmores());
        streamTableEnvironment.createTable(csvPolicy, csvTables.fullname());
        streamTableEnvironment.executeSql(
                "INSERT INTO "
                        + csvPolicy
                        + " SELECT given_name || ' ' || middle_name || ' ' || surname || ' ' ||"
                        + " suffix FROM "
                        + pgPolicy
                        + " WHERE surname = 'Willmore' AND (suffix IS NOT NULL AND suffix <> '')");
        streamTableEnvironment.executeSql("SELECT * FROM " + csvPolicy).print();
    }
}
