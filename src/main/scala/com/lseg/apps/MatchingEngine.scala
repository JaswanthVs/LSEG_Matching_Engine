package com.lseg.apps

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, row_number, when}
import org.apache.spark.sql.types._

object MatchingEngine {
  def main(args: Array[String]): Unit = {
    //Setting Hadoop Home Dir
    System.setProperty("hadoop.home.dir","J:\\winutils")

    val spark = SparkSession
      .builder()
      .appName("FX Orders Matching Engine")
      .config("spark.master","local")
      .enableHiveSupport()
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val ordersSchema = StructType(Array(
      StructField("order_id", LongType, true),
      StructField("user_name", StringType, true),
      StructField("order_time", LongType, true),
      StructField("order_type", StringType, true),
      StructField("quantity", IntegerType, true),
      StructField("price", LongType, true)
    ))

    var orderBook = spark.read
      .options(Map("header" -> "false"))
      .schema(ordersSchema)
      .csv("src/main/resources/input/exampleOrders.csv")

    val filteredBuyOrders = orderBook
      .filter(col("order_type") === "BUY")
      .toDF("buy_order_id","buy_user_name","buy_time","buy_type","buy_quantity","buy_price")

    val filteredSellOrders = orderBook
      .filter(col("order_type") === "SELL")
      .toDF("sell_order_id","sell_user_name","sell_time","sell_type","sell_quantity","sell_price")

    val matchedOrders = filteredBuyOrders
      .join(filteredSellOrders, filteredBuyOrders("buy_quantity") === filteredSellOrders("sell_quantity"))

    import spark.implicits._

    val matchedBuyOrderIdList = matchedOrders.select("buy_order_id").map(_.getLong(0)).collect.toSeq

    val matchedSellOrderIdList = matchedOrders.select("sell_order_id").map(_.getLong(0)).collect.toSeq

    val rankedBuyOrders = filteredBuyOrders.filter(col("buy_order_id").isin(matchedBuyOrderIdList:_*))
      .withColumn("buy_rank", row_number().over(Window.partitionBy(col("buy_quantity")).orderBy(col("buy_order_id").asc).orderBy(col("buy_price").asc)))
      .filter(col("buy_rank") === 1)

    val rankedSellOrders = filteredSellOrders.filter(col("sell_order_id").isin(matchedSellOrderIdList: _*))
      .withColumn("sell_rank", row_number().over(Window.partitionBy(col("sell_quantity")).orderBy(col("sell_order_id").asc).orderBy(col("sell_price").desc)))//.orderBy(col("sell_order_id").asc)))
      .filter(col("sell_rank") === 1)
    rankedSellOrders.show()
    val finalPlacedOrders = rankedBuyOrders
      .join(rankedSellOrders, rankedBuyOrders("buy_quantity") === rankedSellOrders("sell_quantity"))
      .withColumn("order_id",
        when(col("buy_order_id") > col("sell_order_id"), col("buy_order_id"))
          .otherwise(col("sell_order_id")))
      .withColumn("matched_order_id",
        when(col("buy_order_id") < col("sell_order_id"), col("buy_order_id"))
          .otherwise(col("sell_order_id")))
      .withColumn("order_placed_time",
        when(col("buy_order_id") > col("sell_order_id"), col("buy_time"))
          .otherwise(col("sell_time")))
      .withColumn("price",
        when(col("buy_order_id") < col("sell_order_id"), col("buy_price"))
          .otherwise(col("sell_price")))
      .select("order_id", "matched_order_id","order_placed_time","sell_quantity","price")

    finalPlacedOrders
      .write.option("header","false")
      .mode(SaveMode.Overwrite)
      .csv("src/main/resources/output/placedOrders")

    val finalOrderPlacedIdList = finalPlacedOrders.select("order_id").map(_.getLong(0)).collect.toSeq ++
      finalPlacedOrders.select("matched_order_id").map(_.getLong(0)).collect.toSeq

       orderBook = orderBook
       .filter(!col("order_id").isin(finalOrderPlacedIdList:_*))
     //updatedOrderBook.show()
    orderBook.write.option("header","false")
      .mode(SaveMode.Overwrite)
      .csv("src/main/resources/output/updatedOrderBooks")
  }
}
