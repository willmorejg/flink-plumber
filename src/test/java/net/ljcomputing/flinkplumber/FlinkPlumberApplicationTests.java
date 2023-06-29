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

import net.ljcomputing.flinkplumber.filter.WillmoreFilter;
import net.ljcomputing.flinkplumber.model.Person;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.MethodOrderer.OrderAnnotation;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.test.context.ActiveProfiles;

@SpringBootTest
@TestMethodOrder(OrderAnnotation.class)
@ActiveProfiles("test")
class FlinkPlumberApplicationTests {
    private static final Logger log = LoggerFactory.getLogger(FlinkPlumberApplicationTests.class);

    @TestConfiguration
    static class FlinkStreamingSpringBootTestPropertiesConfiguration {

        @Bean
        String outputFileName() {
            return "src/test/logs/willmores.txt";
        }
    }

    @Autowired private String outputFileName;

    @Autowired private StreamExecutionEnvironment streamExecutionEnvironment;

    @Autowired private StreamTableEnvironment streamTableEnvironment;

    @Autowired private JdbcExecutionOptions jdbcExecutionOptions;

    @Autowired private JdbcConnectionOptions postgresConnectionOptions;

    @Test
    @Order(1)
    void contextLoads() {
        assertNotNull(streamExecutionEnvironment);
        assertNotNull(streamTableEnvironment);
    }

    @Test
    @Order(10)
    void testModel() {
        final Person person1 = new Person("Jim", "", "Willmore", "");
        final Person person2 = new Person("Andy", "", "Smith", "");

        final DataStream<Person> people = streamExecutionEnvironment.fromElements(person1, person2);
        final Table peopleTable = streamTableEnvironment.fromDataStream(people);
        streamTableEnvironment.createTemporaryView("PeopleView", peopleTable);

        final Table resultTable =
                streamTableEnvironment.sqlQuery(
                        "SELECT * FROM PeopleView WHERE surname='Willmore'");
        final DataStream<Row> resultsDs = streamTableEnvironment.toDataStream(resultTable);

        resultsDs.print();

        people.filter(new WillmoreFilter())
                .addSink(
                        JdbcSink.sink(
                                "INSERT INTO willmores (given_name, middle_name, surname, suffix)"
                                        + " VALUES (?,?,?,?)",
                                (statement, row) -> {
                                    int idx = 1;
                                    statement.setObject(idx++, row.getGivenName());
                                    statement.setObject(idx++, row.getMiddleName());
                                    statement.setObject(idx++, row.getSurname());
                                    statement.setObject(idx++, row.getSuffix());
                                },
                                jdbcExecutionOptions,
                                postgresConnectionOptions));

        try {
            streamExecutionEnvironment.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
