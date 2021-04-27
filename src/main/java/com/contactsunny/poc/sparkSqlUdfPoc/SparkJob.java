package com.contactsunny.poc.sparkSqlUdfPoc;

import com.contactsunny.poc.sparkSqlUdfPoc.domain.FileInputLine;
import com.contactsunny.poc.sparkSqlUdfPoc.exceptions.ValidationException;
import com.contactsunny.poc.sparkSqlUdfPoc.utils.FileUtil;
import com.contactsunny.poc.sparkSqlUdfPoc.utils.UDFUtil;
import java.util.ArrayList;
import java.util.List;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import static com.contactsunny.poc.sparkSqlUdfPoc.config.CustomConstants.*;
import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lit;

class SparkJob {

    private String[] args;

    private final Logger logger = Logger.getLogger(SparkJob.class);

    private String sparkMaster, inputFilePath;

    private JavaSparkContext javaSparkContext;
    private SQLContext sqlContext;
    private SparkSession sparkSession;

    private UDFUtil udfUtil;
    private FileUtil fileUtil;

    SparkJob(String[] _args) {
        this.args = _args;
    }

    void startJob() throws ValidationException, IOException {
        logger.setLevel(Level.INFO);
        Logger.getLogger("org.spark_project").setLevel(Level.ERROR);
        Logger.getLogger("org.apache").setLevel(Level.ERROR);

        logger.info("Validating arguments");
        validateArguments();

        logger.info("Loading properties");
        loadProperties();

        logger.info("Registering UDFs");
        registerUdfs();

        logger.info("Process ready");

        Dataset<FileInputLine> inputFileDataset = fileUtil.getDatasetFromFile(inputFilePath);
        inputFileDataset.show();

        Dataset<Row> preprocessedDf = inputFileDataset
            .toDF()
            .withColumn(TEMPERATURE,col(TEMPERATURE).cast("Integer"))
            .withColumn(HUMIDITY,col(HUMIDITY).cast("Integer"));

//        Dataset<Row> result = preprocessedDf
//            .filter(callUDF(AROUND_G, col(HUMIDITY), lit(60.0), lit(5.0)).$greater$eq(0.6));

//        Dataset<Row> result = preprocessedDf
//            .filter(callUDF(AROUND_TRI, col(HUMIDITY), lit(45), lit(47), lit(60)).$greater$eq(0.75));

//        Dataset<Row> result = preprocessedDf
//            .filter(callUDF(AROUND_TRAP, col(HUMIDITY), lit(45), lit(47), lit(49), lit(60)).$greater$eq(0.75));

        Dataset<Row> result = preprocessedDf
            .withColumn(TEMPERATURE_LEVEL, callUDF(ASSIGN_LEVEL, col(TEMPERATURE)))
            .withColumn(TEMPERATURE_LEVEL_DEGREE, callUDF(MEMBER_DEGREE, col(TEMPERATURE), col(TEMPERATURE_LEVEL)));


        result.show();














//        upperCaseColumnDataset.coalesce(1).write()
//            .option("header","true")
//            .option("sep",",")
//            .mode("overwrite")
//            .csv("src\\main\\resources\\edited");

    }

    private void loadProperties() throws IOException {
        Properties properties = new Properties();
        String propFileName = "application.properties";

        InputStream inputStream = App.class.getClassLoader().getResourceAsStream(propFileName);

        try {
            properties.load(inputStream);
        } catch (IOException e) {
            logger.error(e.getMessage());
            throw e;
        }

        initialize(properties);
    }

    private void registerUdfs() {

        this.udfUtil.registerColumnDoubleUdf();
        this.udfUtil.registerColumnUppercaseUdf();
        this.udfUtil.registerFilterAlwaysFalseUdf();
        this.udfUtil.registerAroundG();
        this.udfUtil.registerAroundTri();
        this.udfUtil.registerAroundTrap();
        this.udfUtil.registerAssignLevel();
        this.udfUtil.registerMemberDegree();
    }

    private void initialize(Properties properties) {
        logger.info("Initializing SPARK");
        sparkMaster = properties.getProperty("spark.master");

        javaSparkContext = createJavaSparkContext();
        sqlContext = new SQLContext(javaSparkContext);
        sparkSession = sqlContext.sparkSession();
        inputFilePath = this.args[0];
        udfUtil = new UDFUtil(sqlContext);
        fileUtil = new FileUtil(sparkSession);
    }

    private void validateArguments() throws ValidationException {

        if (args.length < 1) {
            logger.error("Invalid arguments.");
            logger.error("1. Input file path.");
            logger.error("Example: java -jar <jarFileName.jar> /path/to/input/file");

            throw new ValidationException("Invalid arguments, check help text for instructions.");
        }
    }

    private JavaSparkContext createJavaSparkContext() {

        /* Create the SparkSession.
         * If config arguments are passed from the command line using --conf,
         * parse args for the values to set.
         */

        SparkConf conf = new SparkConf().setAppName("SparkSql-UDF-POC").setMaster(sparkMaster);

        return new JavaSparkContext(conf);
    }
}
