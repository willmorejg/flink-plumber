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
package net.ljcomputing.flinkplumber.configuration;

import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/** Flink configurations */
@Configuration
public class FlinkConfiguration {
    /**
     * Streaming execution environment bean.
     *
     * @return
     */
    @Bean
    public StreamExecutionEnvironment streamingExecutionEnvironment() {
        return StreamExecutionEnvironment.getExecutionEnvironment();
    }

    /**
     * Streaming table environment.
     *
     * @param streamExecutionEnvironment
     * @return
     */
    @Bean
    public StreamTableEnvironment streamTableEnvironment(
            StreamExecutionEnvironment streamExecutionEnvironment) {
        return StreamTableEnvironment.create(streamExecutionEnvironment);
    }

    /**
     * Flink JDBC execution options.
     *
     * @return
     */
    @Bean
    public JdbcExecutionOptions jdbcExecutionOptions() {
        return JdbcExecutionOptions.builder()
                .withBatchIntervalMs(
                        6000) // optional: default = 0, meaning no time-based execution is done
                .withBatchSize(1000) // optional: default = 5000 values
                .withMaxRetries(5) // optional: default = 3
                .build();
    }
}
