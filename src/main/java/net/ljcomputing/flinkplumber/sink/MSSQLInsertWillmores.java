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
package net.ljcomputing.flinkplumber.sink;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import lombok.extern.slf4j.Slf4j;
import net.ljcomputing.flinkplumber.model.Person;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/** Sink function to insert values into willmores table. */
@Component
@Slf4j
public class MSSQLInsertWillmores extends RichSinkFunction<Person> {
    private static final String sql =
            "INSERT INTO willmores (given_name, middle_name, surname, suffix) VALUES (?,?,?,?)";

    private Connection connection;

    private PreparedStatement statement;

    @Autowired private JdbcConnectionOptions msSqlConnectionOptions;

    /** {@inheritDoc} */
    @Override
    public void close() throws Exception {
        log.debug("closing ...");
        statement.executeBatch();
        connection.close();
        super.close();
    }

    /** {@inheritDoc} */
    @Override
    public void open(final Configuration parameters) throws Exception {
        super.open(parameters);

        Class.forName(msSqlConnectionOptions.getDriverName());

        connection = DriverManager.getConnection(msSqlConnectionOptions.getDbURL());
        statement = connection.prepareStatement(sql);
    }

    /** {@inheritDoc} */
    @Override
    public void invoke(Person value, Context context) throws Exception {
        log.debug("writing value: {}", value);
        int idx = 1;
        statement.setObject(idx++, value.getGivenName());
        statement.setObject(idx++, value.getMiddleName());
        statement.setObject(idx++, value.getSurname());
        statement.setObject(idx++, value.getSuffix());
        statement.addBatch();
    }
}
