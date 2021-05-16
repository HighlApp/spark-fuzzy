package com.contactsunny.poc.sparkSqlUdfPoc.utils;

import static com.contactsunny.poc.sparkSqlUdfPoc.config.CustomConstants.AROUND_G;
import static com.contactsunny.poc.sparkSqlUdfPoc.config.CustomConstants.AROUND_TRAP;
import static com.contactsunny.poc.sparkSqlUdfPoc.config.CustomConstants.AROUND_TRI;
import static com.contactsunny.poc.sparkSqlUdfPoc.config.CustomConstants.ASSIGN_LEVEL;
import static com.contactsunny.poc.sparkSqlUdfPoc.config.CustomConstants.FILTER_ALWAYS_FALSE_NAME;
import static com.contactsunny.poc.sparkSqlUdfPoc.config.CustomConstants.FUZZ_AND;
import static com.contactsunny.poc.sparkSqlUdfPoc.config.CustomConstants.FUZZ_OR;
import static com.contactsunny.poc.sparkSqlUdfPoc.config.CustomConstants.FUZZ_VALUE_JOIN;
import static com.contactsunny.poc.sparkSqlUdfPoc.config.CustomConstants.MEMBER_DEGREE;


import com.contactsunny.poc.sparkSqlUdfPoc.domain.TempLingValueNew;
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
  private SQLContext sqlContext;
  private static List<TempLingValueNew> TEMP_LING_VALUES;


  public UDFUtil(SQLContext _sqlContext, List<TempLingValueNew> tempLingValues) {
    this.sqlContext = _sqlContext;
    TEMP_LING_VALUES = tempLingValues;
  }

  public static Double aroundTrap(Integer columnVal, Integer lower, Integer lowerMid, Integer upperMid,
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

  public static Double aroundTri(Integer columnVal, Integer lower, Integer mid, Integer upper) {
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

  public static Double aroundG(Integer columnVal, Double mean, Double stdDev) {
    return Math.exp(-(columnVal - mean) * (columnVal - mean) / (2.0 * stdDev * stdDev));
  }

  public static String assignLevel(Integer columnVal) {
    final String assignedLvl = TEMP_LING_VALUES.stream()
        .max(Comparator.comparing(l ->
            aroundTrap(columnVal, l.getLower(), l.getLowerMid(), l.getUpperMid(), l.getUpper())))
        .get().getLevel();

    return assignedLvl;
  }


  private static double membershipDegree(Integer tempVal, String levelString) {
    TempLingValueNew lingVal = TEMP_LING_VALUES
        .stream()
        .filter(tempLingVal -> tempLingVal.getLevel().equals(levelString))
        .findFirst()
        .get();

    return aroundTrap(tempVal, lingVal.getLower(), lingVal.getLowerMid(), lingVal.getUpperMid(), lingVal.getUpper());
  }

  private static Double fuzzOr(Double val1, Double val2) {
    double max = Math.max(val1, val2);
    return max;
  }

  private static Double fuzzAnd(Double val1, Double val2) {
    double min = Math.min(val1, val2);
    return min;
  }

  private static Double fuzzValueJoin(Integer val1, Integer val2, Integer margin) {
    double m1, m2, b1, b2;
    if (val1.equals(val2)) {
      return 1.0;
    } else if (val1 < val2) {
      m1 = slope(val1, 1, val1 + margin, 0);
      m2 = slope(val2, 1, val2 - margin, 0);
    } else {
      m1 = slope(val1, 1, val1 - margin, 0);
      m2 = slope(val2, 1, val2 + margin, 0);
    }
    //y = m(x - a) + b --> y = mx + (b-ma) gdzie a to x, b to y
    b1 = (1 - m1 * val1);
    b2 = (1 - m2 * val2);

    final double degree = calculateIntersectionPointDegree(m1, b1, m2, b2);
    return degree;
  }

  private static double slope(int x1, int y1, int x2, int y2)
  {
    return ((double)y2 - y1) / (x2 - x1);
  }

  private static double calculateIntersectionPointDegree(
      double m1,
      double b1,
      double m2,
      double b2) {

    if (m1 == m2) {
      return 0.0;
    }

    double x = (b2 - b1) / (m1 - m2);
    double y = m1 * x + b1;

    if (y <= 0 || y >=1 ) {
      return 0.0;
    }
    return y;
  }

  public void registerFilterAlwaysFalseUdf() {
    this.sqlContext.udf().register(FILTER_ALWAYS_FALSE_NAME, (UDF0<Boolean>)
        () -> false, DataTypes.BooleanType);
  }

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

  public void registerFuzzOr() {
    this.sqlContext
        .udf()
        .register(FUZZ_OR,
            (UDF2<Double, Double, Double>)
                UDFUtil::fuzzOr, DataTypes.DoubleType);
  }

  public void registerFuzzAnd() {
    this.sqlContext
        .udf()
        .register(FUZZ_AND,
            (UDF2<Double, Double, Double>)
                UDFUtil::fuzzAnd, DataTypes.DoubleType);
  }

  public void registerFuzzValueJoin() {
    this.sqlContext
        .udf()
        .register(FUZZ_VALUE_JOIN,
            (UDF3<Integer, Integer, Integer, Double>)
                UDFUtil::fuzzValueJoin, DataTypes.DoubleType);
  }
}
