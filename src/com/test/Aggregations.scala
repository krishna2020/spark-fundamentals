package com.test

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import java.util.HashMap
import scala.util.matching.Regex
import scala.util.matching.Regex.Match
import org.apache.spark.sql.Column

import org.apache.spark.sql.functions.lit
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent._
import scala.language.implicitConversions
import scala.util.{ Failure, Success }
import org.apache.spark.sql.Column
import java.util.Calendar
import java.text.SimpleDateFormat

object Aggregations {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("AggregationJob").setMaster("local[*]")

    sparkConf.set("spark.dynamicAllocation.enabled", "true");
    sparkConf.set("spark.shuffle.service.enabled", "true");

    //    val sc: SparkContext = new SparkContext(sparkConf)
    //    val sqlContext: SQLContext = new SQLContext(sc)
    //    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    
       val sdf = new SimpleDateFormat("YYYY-MM-dd")

    val cal = Calendar.getInstance()
    cal.set(2018, 2, 1)
    val startDate = sdf.format(cal.getTime())
    cal.set(2018, 4, 1)
    cal.set(Calendar.DAY_OF_MONTH, cal.getActualMaximum(Calendar.DAY_OF_MONTH))
    val endDate = sdf.format(cal.getTime)
    
    val dd = "2018-10-01"
    val ss = dd.split("-")
    println(ss{0})
    println(ss{1})
    println(startDate)
    println(endDate)
  }
  
  def dateOps {
    val cal = Calendar.getInstance()
    println(cal.get(Calendar.MONTH))

    cal.set(2018, cal.get(Calendar.MONTH), 1)
    val mon = new SimpleDateFormat("MMMM").format(cal.getTime)
    println(mon)
  }

  def stringOps = {
    val aggregationColumns = List("SETTLEMENTAMOUNT")
    val testList = List("SETTLEMENTAMOUNT", "SETTLEMENTCURR")
    //println("existingCHARGEBACK".split("existing").map (p => println(p)))
    //println("existingSETTLEMENTAMOUNT".replace("existing", "").contains(aggregationColumns))
    //println(aggregationColumns contains "existingSETTLEMENTAMOUNT".replace("existing", ""))
    //println(aggregationColumns contains testList)
  }

  def joinColDf(spark: SparkSession) = {
    val dataFrame = {
      val rows = Seq[Row](
        Row(1, "2018-07-11", 3),
        Row(4, "2018-07-10", 6),
        Row(7, "2018-07-09", 9)).asJava

      val schema = StructType(Seq(StructField("newPROCESSORNAME", IntegerType), StructField("SETTLEMENTDATE", StringType), StructField("SETTLEMENTCURRENCY", IntegerType)))
      spark.createDataFrame(rows, schema)
    }

    dataFrame.filter(dataFrame("SETTLEMENTDATE").>("2018-07-09")).show

    val dataFrame2 = {
      val rows = Seq[Row](
        Row(1, 2, 3),
        Row(4, 5, 6),
        Row(7, 8, 9)).asJava

      val schema = StructType(Seq(StructField("oldPROCESSORNAME", IntegerType), StructField("SETTLEMENTDATE", IntegerType), StructField("SETTLEMENTCURRENCY", IntegerType)))
      spark.createDataFrame(rows, schema)
    }

    val joined = dataFrame.join(dataFrame2, dataFrame("newPROCESSORNAME") === dataFrame2("oldPROCESSORNAME"), "left")
    //joined.show()
  }

  def joinDf(spark: SparkSession) = {
    import spark.sqlContext.implicits._
    val llist = Seq(("bob", "2015-01-13", 4), ("alic", "2015-04-23", 10))
    val left = llist.toDF("name", "date", "duration")
    val right = Seq(("alice", 100), ("bob", 23)).toDF("name", "upload")

    val df = left.join(right, left.col("name") === right.col("name"), "left")
    df.printSchema()
    df.show()

  }

  def testListOrder() = {
    val Line12Fields = List("MESSAGE1")
    val Line13Fields = List("MESSAGE2")
    val Line14Fields = List("PREFACQENDPT", "CNTRLNUM", "ISSRTVLREQDT", "ACQRTVLRSP_DATE")
    val Line15Fields = List("IMGREVDEC_DATE", "ISSREJRSN", "ISSRTVLRSP_DATE")
    val Line16Fields = List("CSDFIRSTCBSENDERPROCDATE", "CSDSECONDCBSENDERPROCDATE")
    val Line17Fields = List("HISTORYCBTYPE1", "CBTYPE1REASON", "CBTYPE1RETURNDATE", "CBTYPE1E_ERSNCD", "CBTYPE1E_ERESULTSCD", "CBTYPE1CBRTRNAMT_CURR")
    val Line18Fields = List("FIRSTRTRNDATARECORD")
    val Line19Fields = List("HISTORYCBTYPE2", "CBTYPE2REASON", "CBTYPE2RETURNDATE", "CBTYPE2E_ERSNCD", "CBTYPE2E_ERESULTSCD", "CBTYPE2CBRTRNAMT_CURR")
    val Line20Fields = List("SECONDRTRNDATARECORD")
    val Line21Fields = List("FDRCBT1CBRSN", "FDRCBT1CLEARCODE", "FDRCBT1CLEARDATE", "FDRCBT1MSG")
    val Line22Fields = List("FDRCBT2CBRSN", "FDRCBT2CLEARCODE", "FDRCBT2CLEARDATE", "FDRCBT2MSG")
    val Line23Fields = List("FDRCBT3CBRSN", "FDRCBT3CLEARCODE", "FDRCBT3CLEARDATE", "FDRCBT3MSG")

    val lineSchema = List(Line12Fields, Line13Fields, Line14Fields, Line15Fields, Line16Fields, Line17Fields,
      Line18Fields, Line19Fields, Line20Fields, Line21Fields, Line22Fields, Line23Fields)
    val schema = lineSchema.reduce((x, y) => x ++ y)
    //println(schema)
    println(schema.slice(0, 1))

  }

  def testArgs(header1: Option[String] = None,
    message1: Option[String] = None,
    header2: Option[String] = None,
    message2: Option[String] = None) = {
    if (header1.isDefined && message1.isDefined) {
      println(header1.get)
      println(message1.get)
    }
    println(header2)
  }

  def blockRegex() = {
    val blockHeaderRegex = """"1MD-173""".r
    val firstLineMatcher = getMatchedData("1MD-173", blockHeaderRegex)
    println(firstLineMatcher.groupCount)

    val keyList = List("FILENAME", "COL1", "COL2")
    val valueList = List("abc.txt", "column1", "column2")
    val mapList = (keyList zip valueList).toMap
    val mapList2 = (keyList zip valueList).toMap
    val mapLists = List(mapList, mapList2)
    //val mapList = Map("FILENAME" -> "abc.txt", "COL1" -> "column1", "COL2" -> "column2")
    mapLists.reduce((x, y) => x ++ y)
  }

  def xmlDf(spark: SparkSession) = {
    var xmlDF = spark.read.format("com.databricks.spark.xml").option("rootTag", "CardAuthorisation")
      .option("rowTag", "CardAuthorisation").load("resources/gps-settlement.xml")

    xmlDF.printSchema()
    println(xmlDF.columns.size)

    var dropColumns = List("ApprCode", "AuthId")
    xmlDF.columns.foreach(col =>
      if ("".contains(col))
        dropColumns :+= col)
    xmlDF = xmlDF.select(xmlDF.columns.filter { column => !dropColumns.contains(column) }.map { colName => new Column(colName) }: _*)
    println("######")
    xmlDF.printSchema()
    println(xmlDF.columns.size)
  }

  def getMatchedData(line: String, pattern: Regex): Match = {
    val result = pattern.findFirstMatchIn(line)
    result match {
      case Some(_) => return result.get;
      case None => return null;
    }
  }

  def test(sqlContext: SQLContext) = {

    val customSchema = StructType(Array(
      StructField("SETTLEMENTDATE", DateType, true),
      StructField("MTI", StringType, true),
      StructField("TRANSACTIONCODE", StringType, true),
      StructField("SETTLEMENTAMOUNT", DoubleType, true),
      StructField("FEEAMOUNT", DoubleType, true)))

    var rawDataDF = sqlContext.read.format("com.test.CustomCSVFileFormat")
      .option("header", "true") // Use first line of all files as header
      //.schema(customSchema)
      .load("resources/testFile.csv")
    //println("##########" + rawDataDF.count())
    val limitDF = rawDataDF.limit(20)
    //limitDF.show
    //rawDataDF.printSchema()

    rawDataDF.registerTempTable("Summary")

    var aggQuery: String = "select distinct SETTLEMENTDATE, SUM(SETTLEMENTAMOUNT) OVER (PARTITION BY SETTLEMENTDATE ORDER BY SETTLEMENTDATE) AS SETTLEMENTSUMMARY"
      .+(" from Summary WHERE MTI='220' AND TRANSACTIONCODE IN ('0', '1') ORDER BY SETTLEMENTDATE")

    aggQuery = "select * from ((select distinct SETTLEMENTDATE AS SUM_220_00_01_DATE, SUM(SETTLEMENTAMOUNT) OVER (PARTITION BY SETTLEMENTDATE ORDER BY SETTLEMENTDATE) AS SUM_220_00_01 from Summary HAVING MTI='220' AND TRANSACTIONCODE IN ('0', '1')) A join (select distinct SETTLEMENTDATE, SUM(SETTLEMENTAMOUNT) OVER (PARTITION BY SETTLEMENTDATE ORDER BY SETTLEMENTDATE) AS SUM_220_20_26 from Summary HAVING MTI='220' AND TRANSACTIONCODE IN ('20', '26') ) B on A.SUM_220_00_01_DATE = B.SETTLEMENTDATE ) ORDER BY SETTLEMENTDATE"
    val query: String = "select * from Summary ORDER BY SETTLEMENTDATE, MTI, TRANSACTIONCODE"
    val countDF = sqlContext.sql(query)
    //countDF.show

    //Spark in-built CSV datasource
    var csvDataDF = sqlContext.read.csv("resources/testFile.csv")
    //csvDataDF.count()

    //val numberOfLines = spark.read.text("myfile.csv").limit(10)

    // RDD Aggregation
    val rawRdd = rawDataDF.rdd.map { row => toMap(row) }
    rawRdd.collect().map(f => {
      println("#########")
      for (x <- f.keySet().toArray()) {
        println(x + ":" + f.get(x))
      }
    })

    val initialValues = Array.fill(3)(0.0)
    val groupRdd = rawRdd.aggregate(initialValues)(
      (aggregation, value) => combinerCount(aggregation, value, Array("SETTLEMENTAMOUNT", "FEEAMOUNT")),

      (aggregation1, aggregation2) => reducerCount(aggregation1, aggregation2))
  }

  def toMap(row: Row): HashMap[String, String] = {
    val schema = row.schema.fieldNames
    var valuesMap = new java.util.HashMap[String, String]
    for (x <- schema) {
      valuesMap.put(x, row.getAs(x).toString())
    }
    valuesMap
  }

  def combinerCount(aggregation: Array[Double], value: java.util.HashMap[String, String], columns: Array[String]): Array[Double] = {
    var resultValues = Array.ofDim[Double](aggregation.length)
    //For Count
    resultValues(0) = aggregation(0) + 1
    //For Sum Aggregation
    for (i <- 1 until aggregation.length) {
      var tempValue = 0.0
      println(columns(i) + ":" + value.get(columns(i)))
      if (!isAllDigits(value.get(columns(i)).toString())) {
        tempValue = value.get(columns(i)).toString().toDouble
      }

      resultValues(i) = aggregation(i) + tempValue
    }
    resultValues
  }

  def reducerCount(aggregation1: Array[Double], aggregation2: Array[Double]) = {
    var resultValues = Array.ofDim[Double](aggregation1.length)
    for (i <- 0 until aggregation1.length) {
      resultValues(i) = aggregation1(i) + aggregation2(i)
    }
    resultValues
  }

  def isAllDigits(x: String) = x forall Character.isDigit

}

