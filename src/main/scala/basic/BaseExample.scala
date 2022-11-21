package example

import org.apache.spark.SparkConf
import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.types.StructType

// A basic Spark on Scala example that shows how to
// read multiple CSV files, join them up and show the results.

object BasicExample {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("BasicExample")
      .setMaster("local[*]")
      .set("spark.sql.session.timeZone", "UTC")

    val spark = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()

    val baseDataPath = sys.env.getOrElse("BASE_DATA_PATH", "./data")

    val userDf = readCsv(spark, baseDataPath, "users")
    val postDf = readCsv(spark, baseDataPath, "posts")
    val commentDf = readCsv(spark, baseDataPath, "comments")

    val out = postDf
      .join(userDf, postDf("user_id") === userDf("id"))
      .join(commentDf, postDf("id") === commentDf("post_id"))
      .select(
        userDf("id").as("user_id"),
        postDf("id").as("post_id"),
        commentDf("id").as("comment_id"),
        postDf("title"),
        commentDf("text")
      )

    out.show()
  }

  private def readCsv(
      spark: SparkSession,
      dataDir: String,
      fileName: String
  ): DataFrame = {
    spark.read
      .option("header", true)
      .csv(s"${dataDir}/${fileName}.csv")
  }
}
