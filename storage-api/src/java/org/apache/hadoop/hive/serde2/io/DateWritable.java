/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.serde2.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.hive.common.type.Date;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;


/**
 * DateWritable
 * Writable equivalent of java.sql.Date.
 *
 * Dates are of the format
 *    YYYY-MM-DD
 *
 */
public class DateWritable implements WritableComparable<DateWritable> {

  private Date date = new Date();

  /* Constructors */
  public DateWritable() {
  }

  public DateWritable(DateWritable d) {
    set(d);
  }

  public DateWritable(Date d) {
    set(d);
  }

  public DateWritable(int d) {
    set(d);
  }

  /**
   * Set the DateWritable based on the days since epoch date.
   * @param d integer value representing days since epoch date
   */
  public void set(int d) {
    date = Date.ofEpochDay(d);
  }

  /**
   * Set the DateWritable based on the year/month/day of the date in the local timezone.
   * @param d Date value
   */
  public void set(Date d) {
    if (d == null) {
      date = new Date();
      return;
    }

    set(d.getDays());
  }

  public void set(DateWritable d) {
    set(d.getDays());
  }

  /**
   * @return Date value corresponding to the date in the local time zone
   */
  public Date get() {
    return date;
  }

  public int getDays() {
    return (int) date.getLocalDate().toEpochDay();
  }

  /**
   *
   * @return time in seconds corresponding to this DateWritable
   */
  public long getTimeInSeconds() {
    return date.getSeconds();
  }

  public static Date timeToDate(long seconds) {
    return Date.ofEpochMilli(seconds * 1000);
  }

  public static long daysToMillis(int days) {
    return Date.ofEpochDay(days).getMillis();
  }

  public static int millisToDays(long millis) {
    return Date.ofEpochMilli(millis).getDays();
  }

  public static int dateToDays(Date d) {
    return (int) d.getDays();
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    date.setTimeInDays(WritableUtils.readVInt(in));
  }

  @Override
  public void write(DataOutput out) throws IOException {
    WritableUtils.writeVInt(out, (int) date.getDays());
  }

  @Override
  public int compareTo(DateWritable d) {
    return date.compareTo(d.date);
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof DateWritable)) {
      return false;
    }
    return compareTo((DateWritable) o) == 0;
  }

  @Override
  public String toString() {
    return date.toString();
  }

  @Override
  public int hashCode() {
    return date.hashCode();
  }
}
