package com.fp.lab2

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, lit}

object IstanbulStockExchangeApp {
  private val datasetPath = "data/istanbul_stock_exchange.csv"

  def createSparkSession: SparkSession = {
    System.setProperty("spark.driver.host", "127.0.0.1")
    val spark = SparkSession.builder()
      .appName("Istanbul Stock Exchange Lab")
      .master("local[*]")
      .config("spark.ui.enabled", "false")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    spark
  }

  def normalizeColumns(df: DataFrame): DataFrame = {
    val normalizedNames = df.columns.zipWithIndex.map {
      case ("ISE", 1) => "ISE_1"
      case ("ISE", 2) => "ISE_2"
      case ("date", _) => "date"
      case (name, _) => name
    }

    df.toDF(normalizedNames: _*)
  }

  def readDataset(spark: SparkSession): DataFrame =
    normalizeColumns(
      spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(datasetPath)
    )

  def printHeader(df: DataFrame): Unit =
    println(s"Header: ${df.columns.mkString(", ")}")

  def printRows(df: DataFrame, count: Int): Unit =
    df.take(count).foreach(row => println(row.mkString(" | ")))

  def sortByDateDescending(df: DataFrame): DataFrame =
    df.orderBy(col("date").desc)

  def removeEmColumn(df: DataFrame): DataFrame =
    df.drop("EM")

  def sortByHighestNikkei(df: DataFrame): DataFrame =
    df.orderBy(col("NIKKEI").desc)

  def addAverageDaxFtse(df: DataFrame): DataFrame =
    df.withColumn("DAX_FTSE_MEAN", (col("DAX") + col("FTSE")) / lit(2.0))

  def highestValueForColumn(df: DataFrame, columnNumber: Int): Double = {
    val normalizedColumnNumber = columnNumber match {
      case n if n <= 0 => 0
      case n           => n - 1
    }

    val targetColumn = df.columns.lift(normalizedColumnNumber)
      .getOrElse(throw new IllegalArgumentException(s"Column number $columnNumber is out of range for ${df.columns.length} columns"))

    df.orderBy(col(targetColumn).desc)
      .select(col(targetColumn))
      .first()
      .getDouble(0)
  }

  def saveDataFrame(df: DataFrame, outputPath: String): Unit =
    df.coalesce(1)
      .write
      .mode("overwrite")
      .option("header", "true")
      .csv(outputPath)

  def main(args: Array[String]): Unit = {
    val spark = createSparkSession
    val df = readDataset(spark)

    println("Task 2: Dataset successfully read.")

    println("\nTask 3: Header of DataFrame")
    printHeader(df)

    println("\nTask 4: First 5 rows")
    printRows(df, 5)

    println("\nTask 5: Sort by date descending")
    printRows(sortByDateDescending(df), 5)

    println("\nTask 6: Remove EM column")
    val dfWithoutEm = removeEmColumn(df)
    println(s"Columns after EM removal: ${dfWithoutEm.columns.mkString(", ")}")

    println("\nTask 7: Sort by highest NIKKEI index")
    printRows(sortByHighestNikkei(df), 5)

    println("\nTask 8: Add new column with mean value of DAX and FTSE")
    val dfWithMean = addAverageDaxFtse(df)
    dfWithMean.select("DAX", "FTSE", "DAX_FTSE_MEAN").show(5, truncate = false)

    println("\nTask 9: Highest value in a chosen column")
    println(s"Highest value in column 5 (DAX): ${highestValueForColumn(df, 5)}")
    println(s"Highest value in column 6 (FTSE): ${highestValueForColumn(df, 6)}")

    println("\nTask 10: Save final DataFrame as CSV")
    val finalDf = addAverageDaxFtse(removeEmColumn(sortByDateDescending(df)))
    saveDataFrame(finalDf, "output/istanbul_stock_exchange_result")
    println("Saved final DataFrame to output/istanbul_stock_exchange_result")

    spark.stop()
  }
}
