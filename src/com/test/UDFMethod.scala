package com.test

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.Row
import scala.collection.mutable.ListBuffer
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.types.ArrayType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import scala.io.Source

object UdfMethod {
  def main(args: Array[String]): Unit = {
    
    val source = Source.fromFile("resources/BANK_BALANCE.txt")
    val fileList = try source.getLines().toList finally source.close()
    val files = fileList ++ List("dummy.txt")
    println(files)
    
    val columns: ListBuffer[String] = ListBuffer[String]()
    columns.+=("one")
    columns.+=("onte")
    println(columns)

    println(columns.toSeq)
    val row = Row.fromSeq(columns.toSeq)
    println(row)

    val sparkConf = new SparkConf().setAppName("AggregationJob").setMaster("local[*]")

    sparkConf.set("spark.dynamicAllocation.enabled", "true");
    sparkConf.set("spark.shuffle.service.enabled", "true");

    //val sc: SparkContext = new SparkContext(sparkConf)
    //val sqlContext: SQLContext = new SQLContext(sc)

    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    //xmlLoad(spark)

    //valuePrecision(spark)
   
    val groupBy = "PROCESSORNAME, ROWTYPE, FILENAME, FILEDATE, PROGRAMID, PROGRAMMENAME, PROGRAMNAME, CURRENCY, COUNTRY, BALANCEDATE, CARDSTATUS"
    val metrics = "FX_GBP_AMOUNTBLOCKED,FX_USD_AMOUNTBLOCKED,FX_EUR_AMOUNTBLOCKED,FX_CAD_AMOUNTBLOCKED,FX_GBP_AVAILABLEBALANCE,FX_USD_AVAILABLEBALANCE,FX_EUR_AVAILABLEBALANCE,FX_CAD_AVAILABLEBALANCE,FX_GBP_ACCOUNTBALANCE,FX_USD_ACCOUNTBALANCE,FX_EUR_ACCOUNTBALANCE,FX_CAD_ACCOUNTBALANCE,AMOUNTBLOCKED,AVAILABLEBALANCE,ACCOUNTBALANCE"
    var metricList = if (metrics == null || metrics.isEmpty()) List[String]() else metrics.split(",").toList
   
    
    //val query = generateQuery(groupBy.split(",").toList, metricList)
 //println(query)
 
 val groupByDF = spark.sqlContext.sql("select concat_ws('_#', PROCESSORNAME, md5(concat_ws('',  PROCESSORNAME,  ROWTYPE,  FILENAME,  FILEDATE,  PROGRAMID,  PROGRAMMENAME,  PROGRAMNAME,  CURRENCY,  COUNTRY,  BALANCEDATE,  CARDSTATUS)), FILEDATE) AS ROWKEY,  PROCESSORNAME,  ROWTYPE,  FILENAME,  FILEDATE,  PROGRAMID,  PROGRAMMENAME,  PROGRAMNAME,  CURRENCY,  COUNTRY,  BALANCEDATE,  CARDSTATUS, SUM( CAST( FX_GBP_AMOUNTBLOCKED  AS DECIMAL(38, 2))) as TOTAL_FX_GBP_AMOUNTBLOCKED, SUM( CAST( FX_USD_AMOUNTBLOCKED  AS DECIMAL(38, 2))) as TOTAL_FX_USD_AMOUNTBLOCKED, SUM( CAST( FX_EUR_AMOUNTBLOCKED  AS DECIMAL(38, 2))) as TOTAL_FX_EUR_AMOUNTBLOCKED, SUM( CAST( FX_CAD_AMOUNTBLOCKED  AS DECIMAL(38, 2))) as TOTAL_FX_CAD_AMOUNTBLOCKED, SUM( CAST( FX_GBP_AVAILABLEBALANCE  AS DECIMAL(38, 2))) as TOTAL_FX_GBP_AVAILABLEBALANCE, SUM( CAST( FX_USD_AVAILABLEBALANCE  AS DECIMAL(38, 2))) as TOTAL_FX_USD_AVAILABLEBALANCE, SUM( CAST( FX_EUR_AVAILABLEBALANCE  AS DECIMAL(38, 2))) as TOTAL_FX_EUR_AVAILABLEBALANCE, SUM( CAST( FX_CAD_AVAILABLEBALANCE  AS DECIMAL(38, 2))) as TOTAL_FX_CAD_AVAILABLEBALANCE, SUM( CAST( FX_GBP_ACCOUNTBALANCE  AS DECIMAL(38, 2))) as TOTAL_FX_GBP_ACCOUNTBALANCE, SUM( CAST( FX_USD_ACCOUNTBALANCE  AS DECIMAL(38, 2))) as TOTAL_FX_USD_ACCOUNTBALANCE, SUM( CAST( FX_EUR_ACCOUNTBALANCE  AS DECIMAL(38, 2))) as TOTAL_FX_EUR_ACCOUNTBALANCE, SUM( CAST( FX_CAD_ACCOUNTBALANCE  AS DECIMAL(38, 2))) as TOTAL_FX_CAD_ACCOUNTBALANCE, SUM( CAST( REGEXP_REPLACE(AMOUNTBLOCKED,',','')  AS DECIMAL(38, 2))) as TOTAL_AMOUNTBLOCKED, SUM( CAST( REGEXP_REPLACE(AVAILABLEBALANCE,',','')  AS DECIMAL(38, 2))) as TOTAL_AVAILABLEBALANCE, SUM( CAST( REGEXP_REPLACE(ACCOUNTBALANCE,',','')  AS DECIMAL(38, 2))) as TOTAL_ACCOUNTBALANCE, COUNT(*) as TOTAL_RECORDS from summary group by PROCESSORNAME,  ROWTYPE,  FILENAME,  FILEDATE,  PROGRAMID,  PROGRAMMENAME,  PROGRAMNAME,  CURRENCY,  COUNTRY,  BALANCEDATE,  CARDSTATUS")
 //groupByDF.count()
 
    //    
    //
    // 
    //    val hashUdf = udf({s: String => s + "new"})
    //    
    //    var rawDataDF = spark.read.format("csv")
    //                      .option("header", "true") // Use first line of all files as header
    //                      .load("resources/testFile.csv")
    //    val limitDF = rawDataDF.limit(20)
    //    //limitDF.show(3)
    //    rawDataDF.registerTempTable("testTable")
    //    
    //    val rdd = spark.sparkContext.textFile("resources/testJsonFile.txt", 1)
    //    
    //    println("@@@@@@@@@@")
    //spark.read.json(rdd).show()

    //spark.udf.register("hashUDF", (s: String) => s + "new")

    //val newUDF = spark.sql("select *, hashUDF(MTI), concat_ws('##', CAST(MTI AS VARCHAR), CAST(TRANSACTIONCODE AS VARCHAR)) as newCol from testTable")
    //val data = newUDF.foreach { x => println(x) }

  }
  
    def generateQuery(groupBy: List[String], metric: List[String]): String = {
    var query = new StringBuilder("select")
    var groupbyStr = new StringBuilder

    groupBy.foreach(x =>
      groupbyStr.append(" " + x + ","))

    query.append(" concat_ws('_#', PROCESSORNAME, md5(concat_ws('', " + groupbyStr.dropRight(1) + ")), FILEDATE) AS ROWKEY, ")
      
    query.append(groupbyStr)
    

    metric.foreach(x =>
      query.append(" SUM( CAST( " + x + "  AS DECIMAL(38, 2))) as TOTAL_" + x + ","))

    query.append(" COUNT(*) as TOTAL_RECORDS from " + "summary" + " group by")

    query.append(groupbyStr)

    query.dropRight(1).toString()
  }

  def valuePrecision(spark: SparkSession) {
    val customSchema = StructType(Array(
      StructField("SETTLEMENTDATE", StringType, true),
      StructField("MTI", StringType, true),
      StructField("TRANSACTIONCODE", StringType, true),
      StructField("SETTLEMENTAMOUNT", StringType, true),
      StructField("FEEAMOUNT", StringType, true)))

    var rawDataDF = spark.read.format("csv")
      .option("header", "true") // Use first line of all files as header
      .schema(customSchema)
      .load("resources/testFile.csv")

    rawDataDF.registerTempTable("summary")

    //val sumDF = spark.sqlContext.sql("select SUM(CAST(SETTLEMENTAMOUNT AS DECIMAL(25, 3))) AS TOTAL_SETTAMNT from summary ")
    //sumDF.show()

//    println("Count of DF" + rawDataDF.count())
//    
//    rawDataDF = rawDataDF.dropDuplicates("SETTLEMENTDATE", "MTI")
//    
    println("Count of DF after" + rawDataDF.count())

    val groupByDF = spark.sqlContext.sql("select md5(concat_ws('_#',SETTLEMENTDATE,MTI)) as index, SETTLEMENTDATE,MTI, SUM(SETTLEMENTAMOUNT) as SETAMNT, SUM(CAST(SETTLEMENTAMOUNT AS DECIMAL(38, 3))) AS TOTAL_SETTLEMENTAMOUNT, CAST(COUNT(*) as DOUBLE) as TOTAL_RECORDS from summary group by SETTLEMENTDATE,MTI")
   groupByDF.show()
    //groupByDF.na.fill(0, Seq("TOTAL_SETTLEMENTAMOUNT", "TOTAL_RECORDS"))
    //groupByDF.write.format("com.databricks.spark.csv").save("resources/testOutFile.csv")
    
//   groupByDF.select("SETTLEMENTDATE").foreach { f => println(f) }
   // groupSett
    

    //val columns = groupByDF.columns.toSeq

/*    groupByDF.rdd.foreachPartition { partition =>
      partition.foreach { row =>
        val mapResult = getMapFromRow(row, columns)
        mapResult.map(x => println(x._1 + ":" + x._2))
      }
    }*/
  }

  def getMapFromRow(row: Row, columns: Seq[String]): Map[String, Object] = {
    println("mapFromRow:" + row)
    val colMap = columns.map(column => column -> row.getAs[String](column)).toMap
    println(colMap)
    colMap
  }

  def xmlLoad(spark: SparkSession) {
    var xmlLoadedDF = spark.read.format("com.databricks.spark.xml").option("rootTag", "ACCOUNT").option("rowTag", "ACCOUNT")
      //.option("attributePrefix", "")
      .load("resources/GPSBalanceTest.xml")

    xmlLoadedDF.printSchema()
    xmlLoadedDF.show()

    var changedSchema = changeSchemaType(xmlLoadedDF.schema)

    val newXmlLoadedDF = spark.read.format("com.databricks.spark.xml").option("rootTag", "ACCOUNT").option("rowTag", "ACCOUNT")
      .schema(changedSchema)
      .load("resources/GPSBalanceTest.xml")
    newXmlLoadedDF.printSchema()
    newXmlLoadedDF.show()

  }

  def changeSchemaType(schema: StructType): StructType = {
    println("Changing all Schema attributes to String type")
    var newschema = new StructType

    for (ss <- schema.seq) {
      if (ss.dataType.isInstanceOf[StructType]) {
        newschema = newschema.add(ss.name, changeSchemaType(ss.dataType.asInstanceOf[StructType]))
      } else if (ss.dataType.isInstanceOf[ArrayType]) {
        var arraySchema = ss.dataType.asInstanceOf[ArrayType]

        if (arraySchema.elementType.isInstanceOf[StructType]) {
          var elementSchema = DataTypes.createArrayType(changeSchemaType(arraySchema.elementType.asInstanceOf[StructType]))
          newschema = newschema.add(ss.name, elementSchema)
        } else {
          newschema = newschema.add(ss.name, ss.dataType)
        }
      } else {
        newschema = newschema.add(ss.name, StringType)
      }
    }

    return newschema
  }
}