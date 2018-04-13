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

package org.apache.hadoop.hive.ql.udf;

import org.apache.hadoop.hive.common.type.HiveIntervalDayTime;
import org.apache.hadoop.hive.common.type.Timestamp;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedExpressions;
import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorUDFMinuteDate;
import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorUDFMinuteString;
import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorUDFMinuteTimestamp;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.NDV;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.IntWritable;

/**
 * UDFMinute.
 *
 */
@Description(name = "minute",
    value = "_FUNC_(param) - Returns the minute component of the string/timestamp/interval",
    extended = "param can be one of:\n"
    + "1. A string in the format of 'yyyy-MM-dd HH:mm:ss' or 'HH:mm:ss'.\n"
    + "2. A timestamp value\n"
    + "3. A day-time interval value"
    + "Example:\n "
    + "  > SELECT _FUNC_('2009-07-30 12:58:59') FROM src LIMIT 1;\n"
    + "  58\n"
    + "  > SELECT _FUNC_('12:58:59') FROM src LIMIT 1;\n" + "  58")
@VectorizedExpressions({VectorUDFMinuteDate.class, VectorUDFMinuteString.class, VectorUDFMinuteTimestamp.class})
@NDV(maxNdv = 60)
public class UDFMinute extends GenericUDF {

  private transient ObjectInspectorConverters.Converter[] converters = new ObjectInspectorConverters.Converter[1];
  private transient PrimitiveObjectInspector.PrimitiveCategory[] inputTypes = new PrimitiveObjectInspector.PrimitiveCategory[1];
  private final IntWritable output = new IntWritable();

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
    checkArgsSize(arguments, 1, 1);
    checkArgPrimitive(arguments, 0);
    switch (((PrimitiveObjectInspector) arguments[0]).getPrimitiveCategory()) {
      case INTERVAL_DAY_TIME:
        inputTypes[0] = PrimitiveObjectInspector.PrimitiveCategory.INTERVAL_DAY_TIME;
        converters[0] = ObjectInspectorConverters.getConverter(
            arguments[0], PrimitiveObjectInspectorFactory.writableHiveIntervalDayTimeObjectInspector);
        break;
      case STRING:
      case CHAR:
      case VARCHAR:
      case DATE:
      case TIMESTAMP:
      case TIMESTAMPLOCALTZ:
      case VOID:
        obtainTimestampConverter(arguments, 0, inputTypes, converters);
        break;
      default:
        // build error message
        StringBuilder sb = new StringBuilder();
        sb.append(getFuncName());
        sb.append(" does not take ");
        sb.append(((PrimitiveObjectInspector) arguments[0]).getPrimitiveCategory());
        sb.append(" type");
        throw new UDFArgumentTypeException(0, sb.toString());
    }

    ObjectInspector outputOI = PrimitiveObjectInspectorFactory.writableIntObjectInspector;
    return outputOI;
  }

  @Override
  public Object evaluate(GenericUDF.DeferredObject[] arguments) throws HiveException {
    switch (inputTypes[0]) {
      case INTERVAL_DAY_TIME:
        HiveIntervalDayTime intervalDayTime = getIntervalDayTimeValue(arguments, 0, inputTypes, converters);
        if (intervalDayTime == null) {
          return null;
        }
        output.set(intervalDayTime.getMinutes());
        break;
      case STRING:
      case CHAR:
      case VARCHAR:
      case DATE:
      case TIMESTAMP:
      case TIMESTAMPLOCALTZ:
      case VOID:
        Timestamp ts = getTimestampValue(arguments, 0, converters);
        if (ts == null) {
          return null;
        }
        output.set(ts.getMinutes());
    }
    return output;
  }

  @Override
  protected String getFuncName() {
    return "minute";
  }

  @Override
  public String getDisplayString(String[] children) {
    return getStandardDisplayString(getFuncName(), children);
  }
}
