package com.contactsunny.poc.sparkSqlUdfPoc.utils;

import com.contactsunny.poc.sparkSqlUdfPoc.domain.TempLingValueNew;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
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

    public List<TempLingValueNew> readJsonDefinitions(String filePath) {
        ObjectMapper objectMapper = new ObjectMapper();
        String json;
        try {
            json = new String(Files.readAllBytes(Paths.get(filePath)));

            List<TempLingValueNew> tempValues = objectMapper.readValue(json, new TypeReference<List<TempLingValueNew>>(){});
            return tempValues;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
