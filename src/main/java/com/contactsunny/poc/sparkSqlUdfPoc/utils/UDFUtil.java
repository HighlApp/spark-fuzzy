package com.contactsunny.poc.sparkSqlUdfPoc.utils;

import static com.contactsunny.poc.sparkSqlUdfPoc.config.CustomConstants.AROUND_G;
import static com.contactsunny.poc.sparkSqlUdfPoc.config.CustomConstants.COLUMN_DOUBLE_UDF_NAME;
import static com.contactsunny.poc.sparkSqlUdfPoc.config.CustomConstants.COLUMN_UPPERCASE_UDF_NAME;
import static com.contactsunny.poc.sparkSqlUdfPoc.config.CustomConstants.FILTER_ALWAYS_FALSE_NAME;


import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.api.java.UDF0;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.api.java.UDF3;
import org.apache.spark.sql.types.DataTypes;

public class UDFUtil {

    private SQLContext sqlContext;

    public UDFUtil(SQLContext _sqlContext) {
        this.sqlContext = _sqlContext;
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


    public void registerAroundG() {
        this.sqlContext
            .udf()
            .register(AROUND_G, (UDF3<Integer,Double,Double,Double>)
                    (columnVal, mean, stdDev) ->
                        (double)(Math.exp(-(columnVal - mean) * (columnVal - mean) / (2 * stdDev * stdDev))),
                DataTypes.DoubleType);
    }
}
