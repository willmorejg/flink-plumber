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

import java.util.Date;
import net.ljcomputing.flinkplumber.function.PersonToRowFunction;
import net.ljcomputing.flinkplumber.function.RowToPersonFunction;
import net.ljcomputing.flinkplumber.model.Person;
import net.ljcomputing.flinkplumber.schema.DefinedSchemas;
import net.ljcomputing.flinkplumber.schema.SchemaBeanFactory;
import net.ljcomputing.flinkplumber.sink.MSSQLInsertWillmores;
import net.ljcomputing.flinkplumber.sink.MariaDBInsertWillmores;
import net.ljcomputing.flinkplumber.sink.MariaDBInsertWillmoresRows;
import net.ljcomputing.flinkplumber.sink.PGInsertWillmores;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableDescriptor;
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

@SpringBootTest
@TestMethodOrder(OrderAnnotation.class)
@Order(10)
@ActiveProfiles("test")
class FlinkPlumberTempDbTests {
    private static final Logger log = LoggerFactory.getLogger(FlinkPlumberDatabaseTests.class);

    @Autowired private StreamExecutionEnvironment streamExecutionEnvironment;

    @Autowired private StreamTableEnvironment streamTableEnvironment;

    @Autowired private PGInsertWillmores pgInsertWillmores;

    @Autowired private MariaDBInsertWillmores mariadbInsertWillmores;

    @Autowired private MariaDBInsertWillmoresRows mariadbInsertWillmoresRows;

    @Autowired private MSSQLInsertWillmores mssqlInsertWillmores;

    @Autowired private RowToPersonFunction rowToPersonFunction;

    @Autowired private PersonToRowFunction personToRowFunction;

    @Autowired private TableDescriptor pgInsurance;

    @Autowired private TableDescriptor pgPolicy;

    @Autowired private TableDescriptor pgRisk;

    @Autowired private TableDescriptor msSqlWillmores;

    @Autowired private TableDescriptor datagenPeopleTableDescriptor;

    @Autowired private TableDescriptor avroPeopleTableDescriptor;

    @Autowired private SchemaBeanFactory schemaFactory;

    /**
     * People test data stream.
     *
     * @return
     */
    private DataStream<Person> people() {
        return streamExecutionEnvironment.fromElements(
                new Person("Jim", "", "Willmore", "1", null, null),
                new Person("Andy", "", "Smith", "", null, null),
                new Person("Jim", "", "Willmore", "2", null, null),
                new Person("Andy", "", "Smith", "", null, null),
                new Person("Jim", "", "Willmore", "3", null, null),
                new Person("Andy", "", "Smith", "", null, null),
                new Person("Jim", "", "Willmore", "4", null, null),
                new Person("Andy", "", "Smith", "", null, null),
                new Person("Jim", "", "Willmore", "5", null, null),
                new Person("Andy", "", "Smith", "", null, null),
                new Person("Jim", "", "Willmore", "6", null, null),
                new Person("Andy", "", "Smith", "", null, null),
                new Person("John", "", "Willmore", "", null, null));
    }

    /** Test factory. */
    @Test
    @Order(5)
    void testFactory() {
        assertNotNull(schemaFactory.locate(DefinedSchemas.POLICY));
        assertNotNull(schemaFactory.locate(DefinedSchemas.RISK));
        assertNotNull(pgPolicy);
        assertNotNull(pgRisk);
    }

    /** Test Avro format. */
    @Test
    @Order(5)
    void testCreateTempDb() {
        streamTableEnvironment.createTable(
                "datagenPeopleTableDescriptor", datagenPeopleTableDescriptor);
        streamTableEnvironment.createTable("avroPeopleTable", avroPeopleTableDescriptor);
        streamTableEnvironment
                .from("datagenPeopleTableDescriptor")
                .executeInsert("avroPeopleTable");
        streamTableEnvironment.executeSql("SELECT * FROM avroPeopleTable").print();
    }

    /** Test Avro format. */
    @Test
    @Order(10)
    void testSelectDb() {
        streamTableEnvironment.createTemporaryTable("pgPolicy", pgPolicy);
        // streamTableEnvironment.executeSql("SELECT * FROM pgPolicy").print();
        streamTableEnvironment.createTemporaryTable("pgRisk", pgRisk);
        // streamTableEnvironment.executeSql("SELECT * FROM pgRisk").print();

        final Schema avroSchema =
                Schema.newBuilder()
                        .column("policy_id", DataTypes.INT())
                        .column("policy_uuid", DataTypes.STRING())
                        .column("policy_number", DataTypes.STRING())
                        .column("date_code", DataTypes.INT())
                        .column("risk_id", DataTypes.INT())
                        .column("risk_uuid", DataTypes.STRING())
                        .column("parent_risk_id", DataTypes.INT())
                        .column("risk_type", DataTypes.STRING())
                        .column("description", DataTypes.STRING())
                        .build();

        final TableDescriptor avroTableDesc =
                TableDescriptor.forConnector("filesystem")
                        .option(
                                "path",
                                "file:///home/jim/eclipse-workspace/net.ljcomputing/flink-plumber/src/test/resources/out/avro/"
                                        + new Date().getTime())
                        .option("format", "avro")
                        .option("avro.codec", "null")
                        .schema(avroSchema)
                        .build();

        streamTableEnvironment.createTemporaryTable("avroRisk", avroTableDesc);

        streamTableEnvironment
                .sqlQuery(
                        "SELECT b.policy_id, b.policy_uuid, b.policy_number, b.date_code,"
                                + " a.risk_id, a.risk_uuid, a.parent_risk_id, a.risk_type,"
                                + " a.description FROM pgRisk a JOIN pgPolicy b ON a.policy_id"
                                + " = b.policy_id WHERE a.policy_id = 2")
                .executeInsert("avroRisk");

        streamTableEnvironment.executeSql("SELECT * FROM avroRisk").print();
        streamTableEnvironment.dropTemporaryTable("avroRisk");
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
