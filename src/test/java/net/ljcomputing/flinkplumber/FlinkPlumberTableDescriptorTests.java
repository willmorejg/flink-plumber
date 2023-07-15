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

import net.ljcomputing.flinkplumber.schema.DefinedSchemas;
import net.ljcomputing.flinkplumber.schema.SchemaBeanFactory;
import net.ljcomputing.flinkplumber.tabledescriptor.DefinedTableDescriptors;
import net.ljcomputing.flinkplumber.tabledescriptor.TableDescriptorBeanFactory;
import org.junit.jupiter.api.MethodOrderer.OrderAnnotation;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

@SpringBootTest
@TestMethodOrder(OrderAnnotation.class)
@Order(40)
@ActiveProfiles("test")
class FlinkPlumberTableDescriptorTests {
    private static final Logger log = LoggerFactory.getLogger(FlinkPlumberDatabaseTests.class);

    @Autowired private SchemaBeanFactory schemaFactory;

    @Autowired private TableDescriptorBeanFactory tableDescriptorFactory;

    /** Test factory. */
    @Test
    @Order(5)
    void testFactory() {
        assertNotNull(schemaFactory.locate(DefinedSchemas.POLICY));
        assertNotNull(schemaFactory.locate(DefinedSchemas.RISK));
        assertNotNull(
                tableDescriptorFactory.locate(
                        DefinedTableDescriptors.POSTGRES,
                        schemaFactory.locate(DefinedSchemas.POLICY),
                        DefinedSchemas.POLICY.getName()));
    }

    /** Test to retrieve data from database. */
    // @Test
    // @Order(2)
    // void testDataFromDb() {
    //     streamExecutionEnvironment.setParallelism(1).addSource(pgDataSourceFunction).print();

    //     try {
    //         streamExecutionEnvironment.execute();
    //         assertTrue(true);
    //     } catch (Exception e) {
    //         e.printStackTrace();
    //         assertTrue(false);
    //     }
    // }
}
