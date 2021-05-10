package com.contactsunny.poc.sparkSqlUdfPoc;

import static com.contactsunny.poc.sparkSqlUdfPoc.config.CustomConstants.AROUND_TRAP;
import static com.contactsunny.poc.sparkSqlUdfPoc.config.CustomConstants.AROUND_TRI;
import static com.contactsunny.poc.sparkSqlUdfPoc.config.CustomConstants.ASSIGN_LEVEL;
import static com.contactsunny.poc.sparkSqlUdfPoc.config.CustomConstants.FUZZ_AND;
import static com.contactsunny.poc.sparkSqlUdfPoc.config.CustomConstants.FUZZ_OR;
import static com.contactsunny.poc.sparkSqlUdfPoc.config.CustomConstants.FUZZ_VALUE_JOIN;
import static com.contactsunny.poc.sparkSqlUdfPoc.config.CustomConstants.HUMIDITY;
import static com.contactsunny.poc.sparkSqlUdfPoc.config.CustomConstants.MEMBER_DEGREE;
import static com.contactsunny.poc.sparkSqlUdfPoc.config.CustomConstants.PPM;
import static com.contactsunny.poc.sparkSqlUdfPoc.config.CustomConstants.TEMPERATURE;
import static com.contactsunny.poc.sparkSqlUdfPoc.config.CustomConstants.TEMPERATURE_LEVEL;
import static com.contactsunny.poc.sparkSqlUdfPoc.config.CustomConstants.TEMPERATURE_LEVEL_DEGREE;
import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lit;


import com.contactsunny.poc.sparkSqlUdfPoc.enums.Level;
import com.contactsunny.poc.sparkSqlUdfPoc.exceptions.ValidationException;
import com.contactsunny.poc.sparkSqlUdfPoc.utils.FileUtil;
import com.contactsunny.poc.sparkSqlUdfPoc.utils.UDFUtil;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;

class SparkJob {

  private final Logger logger = Logger.getLogger(SparkJob.class);
  private String[] args;
  private String sparkMaster, inputFilePath1, inputFilePath2;

  private JavaSparkContext javaSparkContext;
  private SQLContext sqlContext;
  private SparkSession sparkSession;

  private UDFUtil udfUtil;
  private FileUtil fileUtil;

  SparkJob(String[] _args) {
    this.args = _args;
  }

  void startJob() throws ValidationException, IOException {
    logger.setLevel(org.apache.log4j.Level.INFO);
    Logger.getLogger("org.spark_project").setLevel(org.apache.log4j.Level.ERROR);
    Logger.getLogger("org.apache").setLevel(org.apache.log4j.Level.ERROR);

    logger.info("Validating arguments");
    validateArguments();
    logger.info("Loading properties");
    loadProperties();
    logger.info("Registering UDFs");
    registerUdfs();
    logger.info("Process ready");

    Dataset<Row> fileTempHumid = fileUtil.getDatasetFromFile(inputFilePath1);
    Dataset<Row> fileTempPpm = fileUtil.getDatasetFromFile(inputFilePath2);

    System.out.println("dataset TempHumid");
    fileTempHumid.show();

    Dataset<Row> dfTempHumid = fileTempHumid
        .toDF()
        .withColumn(TEMPERATURE, col(TEMPERATURE).cast("Integer"))
        .withColumn(HUMIDITY, col(HUMIDITY).cast("Integer"));

    Dataset<Row> dfTempPpm = fileTempPpm
        .toDF()
        .withColumn(TEMPERATURE, col(TEMPERATURE).cast("Integer"))
        .withColumn(PPM, col(PPM).cast("Integer"));

    //        Dataset<Row> result = dfTempHumid
    //            .filter(callUDF(AROUND_G, col(HUMIDITY), lit(60.0), lit(5.0)).$greater$eq(0.6));

//    Dataset<Row> resultTri =
//        dfTempHumid.filter(callUDF(AROUND_TRI, col(HUMIDITY), lit(45), lit(47), lit(60)).$greater$eq(0.75));
//    resultTri.show();

    //        Dataset<Row> result = dfTempHumid
    //            .filter(callUDF(AROUND_TRI, col(HUMIDITY), lit(45), lit(47), lit(60)).$greater$eq(0.75));

    //        Dataset<Row> result = dfTempHumid
    //            .filter(callUDF(AROUND_TRAP, col(HUMIDITY), lit(45), lit(47), lit(49), lit(60)).$greater$eq(0.75));

    Dataset<Row> result = dfTempHumid
        .withColumn(TEMPERATURE_LEVEL, callUDF(ASSIGN_LEVEL, col(TEMPERATURE)))
        .withColumn(TEMPERATURE_LEVEL_DEGREE, callUDF(MEMBER_DEGREE, col(TEMPERATURE), col(TEMPERATURE_LEVEL)));

    result.show();


    result = result
        .filter(callUDF(MEMBER_DEGREE, col(TEMPERATURE),
            lit(Level.HOT.name())).$greater$eq(0.7));

    result.show();


    //        result = result
    //            .filter(callUDF(FUZZ_OR,
    //                JavaConversions.asScalaBuffer(Arrays.asList(
    //                    callUDF(AROUND_TRAP, col(HUMIDITY), lit(45), lit(47), lit(49), lit(60)),
    //                    callUDF(AROUND_TRAP, col(TEMPERATURE), lit(13), lit(15), lit(17), lit(19)))).seq()
    //            ));
    Dataset<Row> resultOrSingle = result
        .filter(callUDF(FUZZ_OR,
            callUDF(AROUND_TRAP, col(HUMIDITY), lit(45), lit(47), lit(49), lit(60)),
            callUDF(AROUND_TRAP, col(TEMPERATURE), lit(13), lit(15), lit(17), lit(19)))
            .$greater$eq(0.7)
        );

    resultOrSingle.show();

    Dataset<Row> resultOrDouble = result
        .filter(callUDF(FUZZ_OR,
            callUDF(AROUND_TRAP, col(TEMPERATURE), lit(23), lit(25), lit(29), lit(34)),
            callUDF(FUZZ_OR,
                callUDF(AROUND_TRAP, col(HUMIDITY), lit(45), lit(47), lit(49), lit(60)),
                callUDF(AROUND_TRAP, col(TEMPERATURE), lit(13), lit(15), lit(17), lit(19)))
            ).$greater$eq(0.7)
        );
    resultOrDouble.show();


    Dataset<Row> resultAndSingle = result
        .filter(callUDF(FUZZ_AND,
            callUDF(AROUND_TRAP, col(HUMIDITY), lit(45), lit(47), lit(49), lit(60)),
            callUDF(AROUND_TRAP, col(HUMIDITY), lit(45), lit(47), lit(49), lit(60)))
            .$greater$eq(0.7)
        );

    System.out.println("fuzzy and");
    resultAndSingle.show();

    Dataset<Row> resultAndDouble = result
        .filter(callUDF(FUZZ_AND,
            callUDF(AROUND_TRAP, col(TEMPERATURE), lit(23), lit(25), lit(29), lit(34)),
            callUDF(FUZZ_AND,
                callUDF(AROUND_TRAP, col(HUMIDITY), lit(45), lit(47), lit(49), lit(60)),
                callUDF(AROUND_TRAP, col(HUMIDITY), lit(45), lit(47), lit(49), lit(60)))
            )
                .$greater$eq(0.7)
        );
    resultAndDouble.show();

    result = dfTempHumid.groupBy(callUDF(ASSIGN_LEVEL, col(TEMPERATURE))).count();
    result.show();


    dfTempPpm.show();
    String thAlias = "th";
    String tpAlias = "tp";
    dfTempHumid = dfTempHumid.alias(thAlias);
    dfTempPpm = dfTempPpm.alias(tpAlias);
    thAlias += ".";
    tpAlias += ".";


    dfTempHumid = dfTempHumid.withColumn(TEMPERATURE_LEVEL, callUDF(ASSIGN_LEVEL, col(TEMPERATURE)));
    dfTempPpm = dfTempPpm.withColumn(TEMPERATURE_LEVEL, callUDF(ASSIGN_LEVEL, col(TEMPERATURE)));


    //first type of join - joining by ling values
    result = dfTempHumid.join(dfTempPpm,
        callUDF(ASSIGN_LEVEL,
            col(thAlias + TEMPERATURE)).equalTo(callUDF(ASSIGN_LEVEL, col(tpAlias + TEMPERATURE))));
    result.show();


    //second type of join - fuzzy values (triangle intersection)
    result = dfTempHumid.join(dfTempPpm,
        callUDF(FUZZ_VALUE_JOIN,
            col(thAlias + TEMPERATURE),
            col(tpAlias + TEMPERATURE),
            lit(4)
        ).$greater$eq(0.7));
    result.show(100);


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
    this.udfUtil.registerFilterAlwaysFalseUdf();
    this.udfUtil.registerAroundG();
    this.udfUtil.registerAroundTri();
    this.udfUtil.registerAroundTrap();
    this.udfUtil.registerAssignLevel();
    this.udfUtil.registerMemberDegree();
    this.udfUtil.registerFuzzOr();
    this.udfUtil.registerFuzzAnd();
    this.udfUtil.registerFuzzValueJoin();
  }

  private void initialize(Properties properties) {
    logger.info("Initializing SPARK");
    sparkMaster = properties.getProperty("spark.master");

    javaSparkContext = createJavaSparkContext();
    sqlContext = new SQLContext(javaSparkContext);
    sparkSession = sqlContext.sparkSession();
    inputFilePath1 = this.args[0];
    inputFilePath2 = this.args[1];
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
