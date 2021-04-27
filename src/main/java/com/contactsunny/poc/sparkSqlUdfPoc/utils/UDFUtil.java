package com.contactsunny.poc.sparkSqlUdfPoc.utils;

import static com.contactsunny.poc.sparkSqlUdfPoc.config.CustomConstants.AROUND_G;
import static com.contactsunny.poc.sparkSqlUdfPoc.config.CustomConstants.AROUND_TRAP;
import static com.contactsunny.poc.sparkSqlUdfPoc.config.CustomConstants.AROUND_TRI;
import static com.contactsunny.poc.sparkSqlUdfPoc.config.CustomConstants.ASSIGN_LEVEL;
import static com.contactsunny.poc.sparkSqlUdfPoc.config.CustomConstants.COLUMN_DOUBLE_UDF_NAME;
import static com.contactsunny.poc.sparkSqlUdfPoc.config.CustomConstants.COLUMN_UPPERCASE_UDF_NAME;
import static com.contactsunny.poc.sparkSqlUdfPoc.config.CustomConstants.FILTER_ALWAYS_FALSE_NAME;
import static com.contactsunny.poc.sparkSqlUdfPoc.config.CustomConstants.MEMBER_DEGREE;


import com.contactsunny.poc.sparkSqlUdfPoc.domain.TempLingValue;
import com.contactsunny.poc.sparkSqlUdfPoc.enums.Level;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.api.java.UDF0;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.api.java.UDF3;
import org.apache.spark.sql.api.java.UDF4;
import org.apache.spark.sql.api.java.UDF5;
import org.apache.spark.sql.types.DataTypes;

public class UDFUtil {
  private static final List<TempLingValue> TEMP_LING_VALUES = Arrays.asList(
      new TempLingValue(Level.LOW, 0, 0, 8, 16),
      new TempLingValue(Level.WARM, 12, 17, 19, 24),
      new TempLingValue(Level.HOT, 21, 27, 28, 35),
      new TempLingValue(Level.VERY_HOT, 32, 34, 42, 45)
  );
  private SQLContext sqlContext;


  public UDFUtil(SQLContext _sqlContext) {
    this.sqlContext = _sqlContext;
  }

  private static Double aroundTrap(Integer columnVal, Integer lower, Integer lowerMid, Integer upperMid,
                                   Integer upper) {
    if ((columnVal < lower) || (columnVal > upper)) {
      return 0.0;
    }
    if ((columnVal >= lowerMid) && (columnVal <= upperMid)) {
      return 1.0;
    }
    if (columnVal < lowerMid) {
      return (((double) columnVal - lower) / (lowerMid - lower));
    }
    return 1.0 - (((double)columnVal - upperMid) / (upper - upperMid));
  }

  private static Double aroundTri(Integer columnVal, Integer lower, Integer mid, Integer upper) {
    if ((columnVal < lower) || (columnVal > upper)) {
      return 0.0;
    }
    if (columnVal.equals(mid)) {
      return 1.0;
    }
    if (columnVal < mid) {
      return (((double) columnVal - lower) / (mid - lower));
    }
    return 1.0 - (((double) columnVal - mid) / (upper - mid));
  }

  private static Double aroundG(Integer columnVal, Double mean, Double stdDev) {
    return Math.exp(-(columnVal - mean) * (columnVal - mean) / (2.0 * stdDev * stdDev));
  }

  private static String assignLevel(Integer columnVal) {
    final Level assignedLvl = TEMP_LING_VALUES.stream()
        .max(Comparator.comparing(l ->
            aroundTrap(columnVal, l.getLower(), l.getLowerMid(), l.getUpperMid(), l.getUpper())))
        .get().getLevel();

    return assignedLvl.toString();
  }


  private static double membershipDegree(Integer tempVal, String levelString) {
    Level level = Arrays.stream(Level.values()).filter(enm -> enm.name().equals(levelString)).findFirst().get();

    TempLingValue val =
        TEMP_LING_VALUES.stream()
            .filter(l -> l.getLevel().equals(level))
            .findFirst()
            .get();

    return aroundTrap(tempVal, val.getLower(), val.getLowerMid(), val.getUpperMid(), val.getUpper());
  }

  public void registerColumnDoubleUdf() {

    this.sqlContext.udf().register(COLUMN_DOUBLE_UDF_NAME, (UDF1<String, Integer>)
        (columnValue) -> {

          return Integer.parseInt(columnValue) * 2;

        }, DataTypes.IntegerType);
  }

  public void registerColumnUppercaseUdf() {

    this.sqlContext.udf().register(COLUMN_UPPERCASE_UDF_NAME, (UDF1<String, String>)
        (columnValue) -> {

          return columnValue.toUpperCase();

        }, DataTypes.StringType);
  }

  public void registerFilterAlwaysFalseUdf() {
    this.sqlContext.udf().register(FILTER_ALWAYS_FALSE_NAME, (UDF0<Boolean>)
        () -> false, DataTypes.BooleanType);
  }
  //
  //    public void registerAroundG() {
  //        this.sqlContext
  //            .udf()
  //            .register(AROUND_G, (UDF3<Integer,Double,Double,Double>)
  //                (columnVal, mean, stdDev) ->
  //                    Math.exp(-(columnVal - mean) * (columnVal - mean) / (2 * stdDev * stdDev)),
  //            DataTypes.DoubleType);
  //    }


  //    public void registerAroundG() {
  //        this.sqlContext
  //            .udf()
  //            .register(AROUND_G, (UDF3<Integer,Double,Double,Double>)
  //                    (columnVal, mean, stdDev) ->
  //                        Math.exp(-(columnVal - mean) * (columnVal - mean) / (2.0 * stdDev * stdDev)),
  //                DataTypes.DoubleType);
  //    }

  public void registerAroundG() {
    this.sqlContext
        .udf()
        .register(AROUND_G, (UDF3<Integer, Double, Double, Double>)
            UDFUtil::aroundG, DataTypes.DoubleType);
  }

  public void registerAroundTri() {
    this.sqlContext
        .udf()
        .register(AROUND_TRI, (UDF4<Integer, Integer, Integer, Integer, Double>)
            UDFUtil::aroundTri, DataTypes.DoubleType);
  }

  public void registerAroundTrap() {
    this.sqlContext
        .udf()
        .register(AROUND_TRAP, (UDF5<Integer, Integer, Integer, Integer, Integer, Double>)
            UDFUtil::aroundTrap, DataTypes.DoubleType);
  }

  public void registerAssignLevel() {
    this.sqlContext
        .udf()
        .register(ASSIGN_LEVEL, (UDF1<Integer, String>)
            UDFUtil::assignLevel, DataTypes.StringType);
  }

  public void registerMemberDegree() {
    this.sqlContext
        .udf()
        .register(MEMBER_DEGREE, (UDF2<Integer, String, Double>)
            UDFUtil::membershipDegree, DataTypes.DoubleType);
  }
}
