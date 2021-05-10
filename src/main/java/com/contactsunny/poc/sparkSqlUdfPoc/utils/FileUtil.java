package com.contactsunny.poc.sparkSqlUdfPoc.utils;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class FileUtil {

    private SparkSession sparkSession;

    public FileUtil(SparkSession _sparkSession) {
        this.sparkSession = _sparkSession;
    }

    public Dataset<Row> getDatasetFromFile(String filePath) {
        Dataset<Row> fileDataSet = this.sparkSession.read().option("header", "true").csv(filePath);
        return fileDataSet;
    }
}
