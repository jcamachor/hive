/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.common.type;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoField;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * This is the internal type for Timestamp.
 * The full qualified input format of Timestamp is
 * "yyyy-MM-dd HH:mm:ss[.SSS...]", where the time part is optional.
 * If time part is absent, a default '00:00:00.0' will be used.
 */
public class Timestamp implements Comparable<Timestamp> {

  private static final LocalDateTime EPOCH = LocalDateTime.of(1970, 1, 1, 0, 0, 0);
  private static final Pattern SINGLE_DIGIT_PATTERN = Pattern.compile("[\\+-]\\d:\\d\\d");
  private static final DateTimeFormatter FORMATTER;
  static {
    DateTimeFormatterBuilder builder = new DateTimeFormatterBuilder();
    // Date part
    builder.append(DateTimeFormatter.ofPattern("yyyy-MM-dd"));
    // Time part
    builder.optionalStart().
        appendLiteral(" ").append(DateTimeFormatter.ofPattern("HH:mm:ss")).
        optionalStart().appendFraction(ChronoField.NANO_OF_SECOND, 1, 9, true).optionalEnd()
        .optionalEnd();
    FORMATTER = builder.toFormatter();
  }

  private LocalDateTime localDateTime;

  public Timestamp() {
    this(EPOCH);
  }

  public Timestamp(LocalDateTime localDateTime) {
    setLocalDateTime(localDateTime);
  }

//  public Timestamp(long seconds, int nanos) {
//    set(seconds, nanos);
//  }
//
//  /**
//   * Obtains an instance of Instant using seconds from the epoch of 1970-01-01T00:00:00Z and
//   * nanosecond fraction of second. Then, it creates a zoned date-time with the same instant
//   * as that specified but in the given time-zone.
//   */
//  public void set(long seconds, int nanos) {
//    setLocalDateTime(LocalDateTime.ofEpochSecond(seconds, nanos, ZoneOffset.UTC));
//  }

  public LocalDateTime getLocalDateTime() {
    return localDateTime;
  }

  public void setLocalDateTime(LocalDateTime localDateTime) {
    this.localDateTime = localDateTime != null ? localDateTime : EPOCH;
  }

  @Override
  public String toString() {
    return localDateTime.format(FORMATTER);
  }

  public int hashCode() {
    return localDateTime.hashCode();
  }

  @Override
  public boolean equals(Object other) {
    if (other instanceof Timestamp) {
      return compareTo((Timestamp) other) == 0;
    }
    return false;
  }

  @Override
  public int compareTo(Timestamp o) {
    return localDateTime.compareTo(o.localDateTime);
  }

  public long getSeconds() {
    return localDateTime.toEpochSecond(ZoneOffset.UTC);
  }

  public void setTimeInSeconds(long epochSecond) {
    setTimeInSeconds(epochSecond, 0);
  }

  public void setTimeInSeconds(long epochSecond, int nanos) {
    localDateTime = LocalDateTime.ofEpochSecond(
        epochSecond, nanos, ZoneOffset.UTC);
  }

  public long getMillis() {
    return localDateTime.toInstant(ZoneOffset.UTC).toEpochMilli();
  }

  public void setTimeInMillis(long epochMilli) {
    localDateTime = LocalDateTime.ofInstant(
        Instant.ofEpochMilli(epochMilli), ZoneOffset.UTC);
  }

  public void setTimeInMillis(long epochMilli, int nanos) {
    localDateTime = LocalDateTime.ofInstant(
        Instant.ofEpochMilli(epochMilli).plusNanos(nanos), ZoneOffset.UTC);
  }

  public int getNanos() {
    return localDateTime.getNano();
  }

  public static Timestamp valueOf(String s) {
    // need to handle offset with single digital hour, see JDK-8066806
    s = handleSingleDigitHourOffset(s);
    LocalDateTime localDateTime;
    try {
      localDateTime = LocalDateTime.parse(s, FORMATTER);
    } catch (DateTimeParseException e) {
      throw new IllegalArgumentException("Cannot create timestamp, parsing error");
    }
    return new Timestamp(localDateTime);
  }

  private static String handleSingleDigitHourOffset(String s) {
    Matcher matcher = SINGLE_DIGIT_PATTERN.matcher(s);
    if (matcher.find()) {
      int index = matcher.start() + 1;
      s = s.substring(0, index) + "0" + s.substring(index, s.length());
    }
    return s;
  }

  public static Timestamp ofEpochSecond(long epochSecond) {
    return ofEpochSecond(epochSecond, 0);
  }

  public static Timestamp ofEpochSecond(long epochSecond, int nanos) {
    return new Timestamp(
        LocalDateTime.ofEpochSecond(epochSecond, nanos, ZoneOffset.UTC));
  }

  public static Timestamp ofEpochMilli(long epochMilli) {
    return ofEpochMilli(epochMilli, 0);
  }

  public static Timestamp ofEpochMilli(long epochMilli, int nanos) {
    return new Timestamp(
        LocalDateTime.ofInstant(
            Instant.ofEpochMilli(epochMilli).plusNanos(nanos), ZoneOffset.UTC));
  }

  public void setNanos(int nanos) {
    localDateTime = localDateTime.plusNanos(nanos);
  }

  /**
   * Return a copy of this object.
   */
  public Object clone() {
    // LocalDateTime is immutable.
    return new Timestamp(this.localDateTime);
  }

}
