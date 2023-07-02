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
package net.ljcomputing.flinkplumber.utils;

import java.time.LocalDate;
import java.time.Period;
import java.time.ZoneId;
import java.util.Date;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

/** Date utilities. */
public enum DateUtils {
    INSTANCE;

    /**
     * Calculate a person's age from today.
     *
     * @param birthdate
     * @return
     */
    public static Integer calculateAge(final Date birthdate) {
        return calculateAge(birthdate, new Date());
    }

    /**
     * Calculate a person's age from the given date.
     *
     * @param birthdate
     * @param fromDate
     * @return
     */
    public static int calculateAge(final Date birthdate, final Date fromDate) {
        final LocalDate birthdateLocalDate =
                birthdate.toInstant().atZone(ZoneId.systemDefault()).toLocalDate();
        final LocalDate toDateLocalDate =
                fromDate.toInstant().atZone(ZoneId.systemDefault()).toLocalDate();
        final Period diff = Period.between(birthdateLocalDate, toDateLocalDate);
        return diff.getYears();
    }

    /**
     * Generate a random date between 1 and 100 years from now.
     *
     * @return
     */
    public static Date randomDate() {
        final long aDay = TimeUnit.DAYS.toMillis(1);
        final long now = new Date().getTime();
        final Date sixteenYearsAgo = new Date(now - aDay * 365 * 1);
        final Date hundredYearsAgo = new Date(now - aDay * 365 * 100);
        final long startMillis = hundredYearsAgo.getTime();
        final long endMillis = sixteenYearsAgo.getTime();
        final long randomMillisSinceEpoch =
                ThreadLocalRandom.current().nextLong(startMillis, endMillis);
        return new Date(randomMillisSinceEpoch);
    }
}
