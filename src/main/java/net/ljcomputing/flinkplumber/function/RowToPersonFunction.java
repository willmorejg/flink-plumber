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
package net.ljcomputing.flinkplumber.function;

import net.ljcomputing.flinkplumber.model.Person;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.types.Row;
import org.springframework.stereotype.Component;

/** Convert a Row into a Person POJO. */
@Component
public class RowToPersonFunction implements MapFunction<Row, Person> {

    /** {@inheritDoc} */
    @Override
    public Person map(final Row value) throws Exception {
        final Person person = new Person();
        person.setGivenName((String) value.getField("given_name"));
        person.setMiddleName((String) value.getField("middle_name"));
        person.setSurname((String) value.getField("surname"));
        person.setSuffix((String) value.getField("suffix"));

        return person;
    }
}
