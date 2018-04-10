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
package org.apache.hadoop.hive.ql.udf.generic;

import static java.math.BigDecimal.ROUND_HALF_UP;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils.PrimitiveGrouping.DATE_GROUP;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils.PrimitiveGrouping.STRING_GROUP;

import java.math.BigDecimal;

import org.apache.hadoop.hive.common.type.Date;
import org.apache.hadoop.hive.common.type.Timestamp;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ConstantObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters.Converter;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

/**
 * UDFMonthsBetween.
 *
 */
@Description(name = "months_between", value = "_FUNC_(date1, date2, roundOff) "
    + "- returns number of months between dates date1 and date2",
    extended = "If date1 is later than date2, then the result is positive. "
    + "If date1 is earlier than date2, then the result is negative. "
    + "If date1 and date2 are either the same days of the month or both last days of months, "
    + "then the result is always an integer. "
    + "Otherwise the UDF calculates the fractional portion of the result based on a 31-day "
    + "month and considers the difference in time components date1 and date2.\n"
    + "date1 and date2 type can be date, timestamp or string in the format "
    + "'yyyy-MM-dd' or 'yyyy-MM-dd HH:mm:ss'. "
    + "The result is rounded to 8 decimal places by default. Set roundOff=false otherwise.\n"
    + " Example:\n"
    + "  > SELECT _FUNC_('1997-02-28 10:30:00', '1996-10-30');\n 3.94959677")
public class GenericUDFMonthsBetween extends GenericUDF {
  private transient Converter[] tsConverters = new Converter[2];
  private transient PrimitiveCategory[] tsInputTypes = new PrimitiveCategory[2];
  private transient Converter[] dtConverters = new Converter[2];
  private transient PrimitiveCategory[] dtInputTypes = new PrimitiveCategory[2];
  private final Date cal1 = new Date();
  private final Date cal2 = new Date();
  private final DoubleWritable output = new DoubleWritable();
  private boolean isRoundOffNeeded = true;

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
    checkArgsSize(arguments, 2, 3);

    checkArgPrimitive(arguments, 0);
    checkArgPrimitive(arguments, 1);

    if (arguments.length == 3) {
      if (arguments[2] instanceof ConstantObjectInspector) {
        isRoundOffNeeded = getConstantBooleanValue(arguments, 2);
      }
    }

    // the function should support both short date and full timestamp format
    // time part of the timestamp should not be skipped
    checkArgGroups(arguments, 0, tsInputTypes, STRING_GROUP, DATE_GROUP);
    checkArgGroups(arguments, 1, tsInputTypes, STRING_GROUP, DATE_GROUP);

    checkArgGroups(arguments, 0, dtInputTypes, STRING_GROUP, DATE_GROUP);
    checkArgGroups(arguments, 1, dtInputTypes, STRING_GROUP, DATE_GROUP);

    obtainTimestampConverter(arguments, 0, tsInputTypes, tsConverters);
    obtainTimestampConverter(arguments, 1, tsInputTypes, tsConverters);

    obtainDateConverter(arguments, 0, dtInputTypes, dtConverters);
    obtainDateConverter(arguments, 1, dtInputTypes, dtConverters);

    ObjectInspector outputOI = PrimitiveObjectInspectorFactory.writableDoubleObjectInspector;
    return outputOI;
  }

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    // the function should support both short date and full timestamp format
    // time part of the timestamp should not be skipped
    Timestamp ts1 = getTimestampValue(arguments, 0, tsConverters);
    Date date1;
    if (ts1 == null) {
      date1 = getDateValue(arguments, 0, dtInputTypes, dtConverters);
      if (date1 == null) {
        return null;
      }
    }
    date1 = Date.ofEpochMilli(ts1.getMillis());

    Timestamp ts2 = getTimestampValue(arguments, 1, tsConverters);
    Date date2;
    if (ts2 == null) {
      date2 = getDateValue(arguments, 1, dtInputTypes, dtConverters);
      if (date2 == null) {
        return null;
      }
    }
    date2 = Date.ofEpochMilli(ts2.getMillis());

    cal1.setTimeInDays(date1.getDays());
    cal2.setTimeInDays(date2.getDays());

    // skip day/time part if both dates are end of the month
    // or the same day of the month
    int monDiffInt = (cal1.getLocalDate().getYear() - cal2.getLocalDate().getYear()) * 12
        + (cal1.getLocalDate().getMonthValue() - cal2.getLocalDate().getMonthValue());
    if (cal1.getLocalDate().getDayOfMonth() == cal2.getLocalDate().getDayOfMonth()
        || (cal1.getLocalDate().getDayOfMonth() == cal1.getLocalDate().lengthOfMonth()
        && cal2.getLocalDate().getDayOfMonth() == cal2.getLocalDate().lengthOfMonth())) {
      output.set(monDiffInt);
      return output;
    }

    long sec1 = cal1.getSeconds();
    long sec2 = cal2.getSeconds();

    // 1 sec is 0.000000373 months (1/2678400). 1 month is 31 days.
    // there should be no adjustments for leap seconds
    double monBtwDbl = monDiffInt + (sec1 - sec2) / 2678400D;
    if (isRoundOffNeeded) {
      // Round a double to 8 decimal places.
      monBtwDbl = BigDecimal.valueOf(monBtwDbl).setScale(8, ROUND_HALF_UP).doubleValue();
    }
    output.set(monBtwDbl);
    return output;
  }

  @Override
  public String getDisplayString(String[] children) {
    return getStandardDisplayString(getFuncName(), children);
  }

  @Override
  protected String getFuncName() {
    return "months_between";
  }
}
