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

import static org.junit.jupiter.api.Assertions.assertTrue;

import net.ljcomputing.flinkplumber.model.Person;
import net.ljcomputing.flinkplumber.sink.MariaDBInsertWillmores;
import net.ljcomputing.flinkplumber.sink.MariaDBInsertWillmoresRows;
import net.ljcomputing.flinkplumber.sink.PGInsertWillmores;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.junit.jupiter.api.MethodOrderer.OrderAnnotation;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

@SpringBootTest
@TestMethodOrder(OrderAnnotation.class)
@Order(10)
@ActiveProfiles("test")
class FlinkPlumberDatabaseTests {
    @Autowired private StreamExecutionEnvironment streamExecutionEnvironment;

    @Autowired private StreamTableEnvironment streamTableEnvironment;

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

    /** Test using a select statement to filter a view, which enriches the initial data stream. */
    @Test
    @Order(2)
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
