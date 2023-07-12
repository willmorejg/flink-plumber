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

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import net.ljcomputing.flinkplumber.function.RowToPersonFunction;
import net.ljcomputing.flinkplumber.model.Person;
import net.ljcomputing.flinkplumber.sink.MSSQLInsertWillmores;
import net.ljcomputing.flinkplumber.sink.MariaDBInsertWillmores;
import net.ljcomputing.flinkplumber.sink.MariaDBInsertWillmoresRows;
import net.ljcomputing.flinkplumber.sink.PGInsertWillmores;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.MethodOrderer.OrderAnnotation;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

@SpringBootTest
@TestMethodOrder(OrderAnnotation.class)
@Order(20)
@ActiveProfiles("test")
@ExtendWith({FlinkTestContainers.class})
class FlinkPlumberDatabaseTests {
    private static final Logger log = LoggerFactory.getLogger(FlinkPlumberDatabaseTests.class);

    @Autowired private StreamExecutionEnvironment streamExecutionEnvironment;

    @Autowired private StreamTableEnvironment streamTableEnvironment;

    @Autowired private PGInsertWillmores pgInsertWillmores;

    @Autowired private MariaDBInsertWillmores mariadbInsertWillmores;

    @Autowired private MariaDBInsertWillmoresRows mariadbInsertWillmoresRows;

    @Autowired private MSSQLInsertWillmores mssqlInsertWillmores;

    @Autowired private RowToPersonFunction rowToPersonFunction;

    // @Autowired private TableDescriptor msSqlWillmores;

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

    /** Test stream to 2 data source. */
    @Test
    @Order(1)
    void testWillmores() {
        final JdbcConnectionOptions msSqlJdbcConnectionOptions =
                FlinkTestContainers.mSSQLServerDS.msSqlConnectionOptions();
        final TableDescriptor msSqlWillmores = FlinkTestContainers.mSSQLServerDS.msSqlWillmores();
        assertNotNull(msSqlWillmores);
        log.info("msSqlWillmores: {}", msSqlWillmores);
        streamTableEnvironment.createTemporaryTable("msSqlWillmores", msSqlWillmores);

        final DataStream<Person> people = people();
        people.addSink(
                        JdbcSink.sink(
                                "INSERT INTO willmores (given_name, middle_name,"
                                        + " surname, suffix) VALUES (?,?,?,?)",
                                (statement, value) -> {
                                    int idx = 1;
                                    statement.setObject(idx++, value.getGivenName());
                                    statement.setObject(idx++, value.getMiddleName());
                                    statement.setObject(idx++, value.getSurname());
                                    statement.setObject(idx++, value.getSuffix());
                                    statement.addBatch();
                                },
                                JdbcExecutionOptions.builder()
                                        .withBatchSize(100)
                                        .withMaxRetries(5)
                                        .build(),
                                msSqlJdbcConnectionOptions))
                .setParallelism(1);

        try {
            streamExecutionEnvironment.execute();
            assertTrue(true);
        } catch (Exception e) {
            log.error("ERROR: ", e);
            assertTrue(false);
        }

        final Table table =
                streamTableEnvironment.sqlQuery(
                        "SELECT given_name, middle_name, surname, suffix FROM msSqlWillmores");

        table.execute().print();

        streamTableEnvironment
                .toDataStream(table)
                .map(rowToPersonFunction)
                .addSink(pgInsertWillmores)
                .setParallelism(1);

        try {
            streamExecutionEnvironment.execute();
            assertTrue(true);
        } catch (Exception e) {
            log.error("ERROR: ", e);
            assertTrue(false);
        }
    }

    /** Test read from table write to JSON. */
    @Test
    @Order(5)
    void testWriteToJsonFile() {
        final JdbcConnectionOptions msSqlJdbcConnectionOptions =
                FlinkTestContainers.mSSQLServerDS.msSqlConnectionOptions();
        final TableDescriptor msSqlWillmores = FlinkTestContainers.mSSQLServerDS.msSqlWillmores();
        streamTableEnvironment.createTemporaryTable("msSqlWillmores1", msSqlWillmores);
        people().addSink(
                        JdbcSink.sink(
                                "INSERT INTO willmores (given_name, middle_name,"
                                        + " surname, suffix) VALUES (?,?,?,?)",
                                (statement, value) -> {
                                    int idx = 1;
                                    statement.setObject(idx++, value.getGivenName());
                                    statement.setObject(idx++, value.getMiddleName());
                                    statement.setObject(idx++, value.getSurname());
                                    statement.setObject(idx++, value.getSuffix());
                                    statement.addBatch();
                                },
                                JdbcExecutionOptions.builder()
                                        .withBatchSize(100)
                                        .withMaxRetries(5)
                                        .build(),
                                msSqlJdbcConnectionOptions))
                .setParallelism(1);

        try {
            streamExecutionEnvironment.execute();
            assertTrue(true);
        } catch (Exception e) {
            log.error("ERROR: ", e);
            assertTrue(false);
        }

        final Table table =
                streamTableEnvironment.sqlQuery(
                        "SELECT given_name, middle_name, surname, suffix FROM"
                                + " msSqlWillmores1");

        final ResolvedSchema resolvedSchema = table.getResolvedSchema();
        log.info("schema: {}", resolvedSchema.toString());

        final Schema.Builder schemaBuilder = Schema.newBuilder();
        final Schema schema = schemaBuilder.fromResolvedSchema(resolvedSchema).build();

        log.info("schema: {}", schema.toString());

        final TableDescriptor descriptor =
                TableDescriptor.forConnector("filesystem")
                        .option("format", "json")
                        .option(
                                "path",
                                "file:///home/jim/eclipse-workspace/net.ljcomputing/flink-plumber/src/test/resources/out")
                        .schema(schema)
                        .build();

        streamTableEnvironment.createTable("csvFile", descriptor);

        final Path csvFilePath = new Path(descriptor.getOptions().get("path").toString());

        final FileSink<Row> sink =
                FileSink.forRowFormat(csvFilePath, new SimpleStringEncoder<Row>("UTF-8")).build();

        final DataStream<Row> fromStream = streamTableEnvironment.toDataStream(table);
        fromStream.addSink((SinkFunction<Row>) sink);
        
        try {
            streamExecutionEnvironment.execute();
            assertTrue(true);
        } catch (Exception e) {
            e.printStackTrace();
            assertTrue(false);
        }
        // .insertInto("csvFile").execute();

        streamTableEnvironment.sqlQuery("SELECT * FROM csvFile").execute().print();

        // try {
        //     streamExecutionEnvironment.execute();
        //     assertTrue(true);
        // } catch (Exception e) {
        //     e.printStackTrace();
        //     assertTrue(false);
        // }
    }

    /** Test stream to 2 data source. */
    @Test
    @Order(10)
    void testElementsToMariaDS() {
        final DataStream<Person> people = people();

        // appears that setting parallelism to '1' will allow the sink to batch process the rows
        people.addSink(mariadbInsertWillmores).setParallelism(1);
        people.addSink(pgInsertWillmores).setParallelism(1);

        try {
            streamExecutionEnvironment.execute();
            assertTrue(true);
        } catch (Exception e) {
            log.error("ERROR: ", e);
            assertTrue(false);
        }
    }

    /** Test using a select statement to filter a view, which enriches the initial data stream. */
    @Test
    @Order(20)
    void testElementsToViewToDS() {
        final DataStream<Person> people = people();

        /* Create a view from a stream. */
        final Table peopleTable = streamTableEnvironment.fromDataStream(people);
        streamTableEnvironment.createTemporaryView("PeopleView2", peopleTable);

        /* Create a table using SQL to select records from a view. */
        final Table resultTable =
                streamTableEnvironment.sqlQuery(
                        "SELECT givenName, middleName, surname, suffix,"
                                + " TO_TIMESTAMP_LTZ(RAND()*985000000, 0) as birthdate,"
                                + " CAST(0 AS INT) as age"
                                + " FROM PeopleView2 WHERE UPPER(surname)='WILLMORE'");

        /* Persist the selecting from the view to a data source. */
        streamTableEnvironment
                .toDataStream(resultTable)
                .addSink(mariadbInsertWillmoresRows)
                .setParallelism(1);

        try {
            streamExecutionEnvironment.execute();
            assertTrue(true);
        } catch (Exception e) {
            log.error("ERROR: ", e);
            assertTrue(false);
        }
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
