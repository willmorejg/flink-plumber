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

import net.ljcomputing.flinkplumber.configuration.DataSourceMariaDBWillmoresProperties;
import net.ljcomputing.flinkplumber.configuration.DataSourcePgInsuranceProperties;
import net.ljcomputing.flinkplumber.filter.AddBirthdateFunction;
import net.ljcomputing.flinkplumber.filter.WillmoreFilter;
import net.ljcomputing.flinkplumber.model.Person;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
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
@Order(1)
@TestMethodOrder(OrderAnnotation.class)
@ActiveProfiles("test")
class FlinkPlumberApplicationTests {
    private static final Logger log = LoggerFactory.getLogger(FlinkPlumberApplicationTests.class);

    @Autowired DataSourcePgInsuranceProperties pgInsuranceProperties;

    @Autowired DataSourceMariaDBWillmoresProperties mariaDBWillmoresProperties;

    @Autowired private StreamExecutionEnvironment streamExecutionEnvironment;

    @Autowired private StreamTableEnvironment streamTableEnvironment;

    @Autowired private WillmoreFilter willmoreFilter;

    @Autowired private AddBirthdateFunction addBirthdateFunction;

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

    /** Test of a function that sets a random birthdate and age to the data stream. */
    @Test
    @Order(13)
    void test() {
        try {
            final DataStream<Person> people = people();

            people.map(addBirthdateFunction).print();

            streamExecutionEnvironment.execute();
            assertTrue(true);
        } catch (Exception e) {
            e.printStackTrace();
            assertTrue(false);
        }
    }

    // @Test
    // @Order(13)
    // void test() {
    //     try {
    //         streamExecutionEnvironment.execute();
    //         assertTrue(true);
    //     } catch (Exception e) {
    //         e.printStackTrace();
    //         assertTrue(false);
    //     }
    // }
}
