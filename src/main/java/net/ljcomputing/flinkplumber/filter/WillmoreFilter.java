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
package net.ljcomputing.flinkplumber.filter;

import lombok.extern.slf4j.Slf4j;
import net.ljcomputing.flinkplumber.model.Person;
import org.apache.flink.api.common.functions.FilterFunction;

@Slf4j
public class WillmoreFilter implements FilterFunction<Person> {

    @Override
    public boolean filter(Person value) throws Exception {
        log.debug("value: {}", value);
        final String comparison =
                value.getSurname() != null ? value.getSurname().toUpperCase() : "";
        log.debug("returning: {}", "WILLMORE".equals(comparison));
        return "WILLMORE".equals(comparison);
    }
}
