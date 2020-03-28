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
package org.apache.hadoop.hive.ql.parse.type;

import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.hadoop.hive.ql.exec.FunctionInfo;
import org.apache.hadoop.hive.ql.exec.FunctionRegistry;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveExtractDate;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveFloorDate;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveToDateSqlOperator;
import org.apache.hadoop.hive.ql.optimizer.calcite.translator.SqlFunctionConverter;
import org.apache.hadoop.hive.ql.optimizer.calcite.translator.RexNodeConverter;
import org.apache.hadoop.hive.ql.optimizer.calcite.translator.TypeConverter;
import org.apache.hadoop.hive.ql.parse.SemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.SemanticAnalyzer.GenericUDAFInfo;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.ExprNodeDescUtils;
import org.apache.hadoop.hive.ql.plan.GroupByDesc;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator.Mode;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFBaseBinary;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFBaseCompare;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFBetween;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFIn;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils.PrimitiveGrouping;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HiveFunctionRegistry {

  private static final Logger LOG = LoggerFactory.getLogger(HiveFunctionRegistry.class);

  public static FunctionInfo getFunctionInfo(String functionText) throws SemanticException {
    return FunctionRegistry.getFunctionInfo(functionText);
  }

  public static RelDataType getReturnType(FunctionInfo fi, List<RelDataType> argsTypes,
      RexBuilder rexBuilder) throws SemanticException {
    // 1) Infer return type
    ObjectInspector[] inputsOIs = new ObjectInspector[argsTypes.size()];
    for (int i = 0; i < inputsOIs.length; i++) {
      inputsOIs[i] = TypeInfoUtils.getStandardWritableObjectInspectorFromTypeInfo(
          TypeConverter.convert(argsTypes.get(i)));
    }
    // 2) Initialize and obtain return type
    ObjectInspector oi = fi.getGenericUDF().initializeAndFoldConstants(inputsOIs);
    // 3) Convert to RelDataType
    return TypeConverter.convert(
        TypeInfoUtils.getTypeInfoFromObjectInspector(oi), rexBuilder.getTypeFactory());
  }

  public static List<RexNode> convert(FunctionInfo fi, List<RexNode> inputs,
      RelDataType returnType, RexBuilder rexBuilder) throws SemanticException {
    // 1) Obtain UDF
    final GenericUDF genericUDF = fi.getGenericUDF();
    final TypeInfo typeInfo = TypeConverter.convert(returnType);
    TypeInfo targetType = null;

    boolean isNumeric = genericUDF instanceof GenericUDFBaseBinary
        && typeInfo.getCategory() == Category.PRIMITIVE
        && PrimitiveGrouping.NUMERIC_GROUP == PrimitiveObjectInspectorUtils.getPrimitiveGrouping(((PrimitiveTypeInfo) typeInfo).getPrimitiveCategory());
    boolean isCompare = !isNumeric && genericUDF instanceof GenericUDFBaseCompare;
    boolean isBetween = !isNumeric && genericUDF instanceof GenericUDFBetween;
    boolean isIN = !isNumeric && genericUDF instanceof GenericUDFIn;

    if (isNumeric) {
      targetType = typeInfo;
    } else if (genericUDF instanceof GenericUDFBaseCompare) {
      targetType = FunctionRegistry.getCommonClassForComparison(
          TypeConverter.convert(inputs.get(0).getType()), TypeConverter.convert(inputs.get(1).getType()));
    } else if (genericUDF instanceof GenericUDFBetween) {
      assert inputs.size() == 4;
      // We skip first child as is not involved (is the revert boolean)
      // The target type needs to account for all 3 operands
      targetType = FunctionRegistry.getCommonClassForComparison(
          TypeConverter.convert(inputs.get(1).getType()),
          FunctionRegistry.getCommonClassForComparison(
              TypeConverter.convert(inputs.get(2).getType()),
              TypeConverter.convert(inputs.get(3).getType())));
    } else if (genericUDF instanceof GenericUDFIn) {
      // We're only considering the first element of the IN list for the type
      assert inputs.size() > 1;
      targetType = FunctionRegistry.getCommonClassForComparison(
          TypeConverter.convert(inputs.get(0).getType()),
          TypeConverter.convert(inputs.get(1).getType()));
    }

    boolean isAllPrimitive = true;
    if (targetType != null && targetType.getCategory() == Category.PRIMITIVE) {
      List<RexNode> newInputs = new ArrayList<>();
      // Convert inputs if needed
      for (int i =0; i < inputs.size(); ++i) {
        RexNode input = inputs.get(i);
        TypeInfo inputTypeInfo = TypeConverter.convert(input.getType());
        RexNode tmpExprNode = input;
        if (TypeInfoUtils.isConversionRequiredForComparison(targetType, inputTypeInfo)) {
          if (isCompare || isBetween || isIN) {
            // For compare, we will convert requisite children
            // For BETWEEN skip the first child (the revert boolean)
            if (!isBetween || i > 0) {
              tmpExprNode = RexNodeTypeCheck.getExprNodeDefaultExprProcessor(rexBuilder)
                  .createConversionCast(input, (PrimitiveTypeInfo) targetType);
              inputTypeInfo = TypeConverter.convert(tmpExprNode.getType());
            }
          } else if (isNumeric) {
            // For numeric, we'll do minimum necessary cast - if we cast to the type
            // of expression, bad things will happen.
            PrimitiveTypeInfo minArgType = ExprNodeDescUtils.deriveMinArgumentCast(inputTypeInfo, targetType);
            tmpExprNode = RexNodeTypeCheck.getExprNodeDefaultExprProcessor(rexBuilder)
                .createConversionCast(input, minArgType);
            inputTypeInfo = TypeConverter.convert(tmpExprNode.getType());
          } else {
            throw new AssertionError("Unexpected " + targetType + " - not a numeric op or compare");
          }
        }

        isAllPrimitive = isAllPrimitive && inputTypeInfo.getCategory() == Category.PRIMITIVE;
        newInputs.add(tmpExprNode);
      }
      return newInputs;
    }
    return inputs;
  }

  public static RexNode getRexNode(String functionText, FunctionInfo fi,
      List<RexNode> inputs, RelDataType returnType, RexBuilder rexBuilder)
      throws SemanticException {
    // See if this is an explicit cast.
    RexNode expr = RexNodeConverter.handleExplicitCast(
        fi.getGenericUDF(), returnType, inputs, rexBuilder);

    if (expr == null) {
      // This is not a cast; process the function.
      ImmutableList.Builder<RelDataType> argsTypes = ImmutableList.builder();
      for (RexNode input : inputs) {
        argsTypes.add(input.getType());
      }
      SqlOperator calciteOp = SqlFunctionConverter.getCalciteOperator(functionText,
          fi.getGenericUDF(), argsTypes.build(), returnType);
      if (calciteOp.getKind() == SqlKind.CASE) {
        // If it is a case operator, we need to rewrite it
        inputs = RexNodeConverter.rewriteCaseChildren(functionText, inputs, rexBuilder);
        // Adjust branch types by inserting explicit casts if the actual is ambigous
        inputs = RexNodeConverter.adjustCaseBranchTypes(inputs, returnType, rexBuilder);
      } else if (HiveExtractDate.ALL_FUNCTIONS.contains(calciteOp)) {
        // If it is a extract operator, we need to rewrite it
        inputs = RexNodeConverter.rewriteExtractDateChildren(calciteOp, inputs, rexBuilder);
      } else if (HiveFloorDate.ALL_FUNCTIONS.contains(calciteOp)) {
        // If it is a floor <date> operator, we need to rewrite it
        inputs = RexNodeConverter.rewriteFloorDateChildren(calciteOp, inputs, rexBuilder);
      } else if (calciteOp == HiveToDateSqlOperator.INSTANCE) {
        inputs = RexNodeConverter.rewriteToDateChildren(inputs, rexBuilder);
      }
      // TODO: We may need to handle IN and BETWEEN
      expr = rexBuilder.makeCall(returnType, calciteOp, inputs);
    }

    // Flatten. (Is this necessary?)
    if (expr instanceof RexCall && !expr.isA(SqlKind.CAST)) {
      RexCall call = (RexCall) expr;
      expr = rexBuilder.makeCall(returnType, call.getOperator(),
          RexUtil.flatten(call.getOperands(), call.getOperator()));
    }

    return expr;
  }

  public static AggregateInfo getAggregateFunctionInfo(boolean isDistinct, boolean isAllColumns,
      String aggName, List<RexNode> aggParameters) throws SemanticException {
    Mode udafMode = SemanticAnalyzer.groupByDescModeToUDAFMode(
        GroupByDesc.Mode.COMPLETE, isDistinct);
    List<ObjectInspector> aggParameterOIs = new ArrayList<>();
    for (RexNode aggParameter : aggParameters) {
      aggParameterOIs.add(TypeInfoUtils.getStandardWritableObjectInspectorFromTypeInfo(
          TypeConverter.convert(aggParameter.getType())));
    }
    GenericUDAFEvaluator genericUDAFEvaluator = SemanticAnalyzer.getGenericUDAFEvaluator2(
        aggName, aggParameterOIs, null, isDistinct, isAllColumns);
    assert (genericUDAFEvaluator != null);
    GenericUDAFInfo udaf = SemanticAnalyzer.getGenericUDAFInfo2(
        genericUDAFEvaluator, udafMode, aggParameterOIs);
    return new AggregateInfo(aggParameters, udaf.returnType, aggName, isDistinct);
  }

  public static AggregateInfo getWindowAggregateFunctionInfo(boolean isDistinct, boolean isAllColumns,
      String aggName, List<RexNode> aggParameters) throws SemanticException {
    TypeInfo returnType = null;

    if (FunctionRegistry.isRankingFunction(aggName)) {
      // Rank functions type is 'int'/'double'
      if (aggName.equalsIgnoreCase("percent_rank")) {
        returnType = TypeInfoFactory.doubleTypeInfo;
      } else {
        returnType = TypeInfoFactory.intTypeInfo;
      }
    } else {
      // Try obtaining UDAF evaluators to determine the ret type
      try {
        Mode udafMode = SemanticAnalyzer.groupByDescModeToUDAFMode(
            GroupByDesc.Mode.COMPLETE, isDistinct);
        List<ObjectInspector> aggParameterOIs = new ArrayList<>();
        for (RexNode aggParameter : aggParameters) {
          aggParameterOIs.add(TypeInfoUtils.getStandardWritableObjectInspectorFromTypeInfo(
              TypeConverter.convert(aggParameter.getType())));
        }
        GenericUDAFEvaluator genericUDAFEvaluator = null;
        if (aggName.toLowerCase().equals(FunctionRegistry.LEAD_FUNC_NAME)
            || aggName.toLowerCase().equals(FunctionRegistry.LAG_FUNC_NAME)) {
          genericUDAFEvaluator = FunctionRegistry.getGenericWindowingEvaluator(aggName,
              aggParameterOIs, isDistinct, isAllColumns);
          GenericUDAFInfo udaf = SemanticAnalyzer.getGenericUDAFInfo2(
              genericUDAFEvaluator, udafMode, aggParameterOIs);
          returnType = ((ListTypeInfo) udaf.returnType).getListElementTypeInfo();
        } else {
          genericUDAFEvaluator = SemanticAnalyzer.getGenericUDAFEvaluator2(
              aggName, aggParameterOIs, null, isDistinct, isAllColumns);
          assert (genericUDAFEvaluator != null);
          GenericUDAFInfo udaf = SemanticAnalyzer.getGenericUDAFInfo2(
              genericUDAFEvaluator, udafMode, aggParameterOIs);
          if (FunctionRegistry.pivotResult(aggName)) {
            returnType = ((ListTypeInfo) udaf.returnType).getListElementTypeInfo();
          } else {
            returnType = udaf.returnType;
          }
        }
      } catch (Exception e) {
        LOG.debug("CBO: Couldn't Obtain UDAF evaluators for " + aggName
            + ", trying to translate to GenericUDF");
      }
    }

    return returnType != null ?
        new AggregateInfo(aggParameters, returnType, aggName, isDistinct) : null;
  }

  /**
   * Class to store GenericUDAF related information.
   */
  public static class AggregateInfo {
    private final List<RexNode> parameters;
    private final TypeInfo returnType;
    private final String aggregateName;
    private final boolean distinct;

    public AggregateInfo(List<RexNode> parameters, TypeInfo returnType, String aggregateName,
        boolean distinct) {
      this.parameters = ImmutableList.copyOf(parameters);
      this.returnType = returnType;
      this.aggregateName = aggregateName;
      this.distinct = distinct;
    }

    public List<RexNode> getParameters() {
      return parameters;
    }

    public TypeInfo getReturnType() {
      return returnType;
    }

    public String getAggregateName() {
      return aggregateName;
    }

    public boolean isDistinct() {
      return distinct;
    }
  }

}
