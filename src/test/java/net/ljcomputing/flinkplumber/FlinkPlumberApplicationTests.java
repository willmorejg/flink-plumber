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
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
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
import net.ljcomputing.flinkplumber.configuration.DataSourceMariaDBWillmoresProperties;
import net.ljcomputing.flinkplumber.configuration.DataSourcePgInsuranceProperties;
import net.ljcomputing.flinkplumber.filter.WillmoreFilter;
import net.ljcomputing.flinkplumber.model.Person;
import net.ljcomputing.flinkplumber.sink.MariaDBInsertWillmores;
import net.ljcomputing.flinkplumber.sink.MariaDBInsertWillmoresRows;
import net.ljcomputing.flinkplumber.sink.PGInsertWillmores;

@SpringBootTest
@TestMethodOrder(OrderAnnotation.class)
@ActiveProfiles("test")
class FlinkPlumberApplicationTests {
    private static final Logger log = LoggerFactory.getLogger(FlinkPlumberApplicationTests.class);

    @Autowired DataSourcePgInsuranceProperties pgInsuranceProperties;

    @Autowired DataSourceMariaDBWillmoresProperties mariaDBWillmoresProperties;

    @Autowired private StreamExecutionEnvironment streamExecutionEnvironment;

    @Autowired private StreamTableEnvironment streamTableEnvironment;

    @Autowired private WillmoreFilter willmoreFilter;

    @Autowired private PGInsertWillmores pgInsertWillmores;

    @Autowired private MariaDBInsertWillmores mariadbInsertWillmores;

    @Autowired private MariaDBInsertWillmoresRows mariadbInsertWillmoresRows;

    /**
     * People test data stream.
     *
     * @return
     */
    private DataStream<Person> people() {
        return streamExecutionEnvironment.fromElements(
                new Person("Jim", "", "Willmore", "1"),
                new Person("Andy", "", "Smith", ""),
                new Person("Jim", "", "Willmore", "2"),
                new Person("Andy", "", "Smith", ""),
                new Person("Jim", "", "Willmore", "3"),
                new Person("Andy", "", "Smith", ""),
                new Person("Jim", "", "Willmore", "4"),
                new Person("Andy", "", "Smith", ""),
                new Person("Jim", "", "Willmore", "5"),
                new Person("Andy", "", "Smith", ""),
                new Person("Jim", "", "Willmore", "6"),
                new Person("Andy", "", "Smith", ""),
                new Person("John", "", "Willmore", ""));
    }

    /** Test autowiring beans and other application specific resources. */
    @Test
    @Order(1)
    void contextLoads() {
        assertNotNull(streamExecutionEnvironment);
        assertNotNull(streamTableEnvironment);
        assertNotNull(pgInsuranceProperties);
        assertNotNull(mariaDBWillmoresProperties);
    }

    /** Test custom filters. */
    @Test
    @Order(10)
    void testFilters() {
        final DataStream<Person> people = people();

        people.filter(willmoreFilter).print();

        try {
            streamExecutionEnvironment.execute();
            assertTrue(true);
        } catch (Exception e) {
            e.printStackTrace();
            assertTrue(false);
        }
    }

    /** Test stream to 2 data source. */
    @Test
    @Order(11)
    void testElementsToMariaDS() {
        final DataStream<Person> people = people();

        // appears that setting parallelism to '1' will allow the sink to batch process the rows
        people.addSink(mariadbInsertWillmores).setParallelism(1);
        people.addSink(pgInsertWillmores).setParallelism(1);

        try {
            streamExecutionEnvironment.execute();
            assertTrue(true);
        } catch (Exception e) {
            e.printStackTrace();
            assertTrue(false);
        }
    }

    @Test
    @Order(12)
    void testElementsToViewToDS() {
        final DataStream<Person> people = people();

        /* Create a view from a stream. */
        final Table peopleTable = streamTableEnvironment.fromDataStream(people);
        streamTableEnvironment.createTemporaryView("PeopleView", peopleTable);

        /* Create a table using SQL to select records from a view. */
        final Table resultTable =
                streamTableEnvironment.sqlQuery(
                        "SELECT givenName, middleName, surname, suffix,"
                                + " TO_TIMESTAMP_LTZ(RAND()*985000000, 0) as birthdate,"
                                + " CAST(0 AS INT) as age"
                                + " FROM PeopleView WHERE UPPER(surname)='WILLMORE'");

        /* Persist the selecting from the view to a data source. */
        streamTableEnvironment
                .toDataStream(resultTable)
                .addSink(mariadbInsertWillmoresRows)
                .setParallelism(1);

        try {
            streamExecutionEnvironment.execute();
            assertTrue(true);
        } catch (Exception e) {
            e.printStackTrace();
            assertTrue(false);
        }
    }
}
