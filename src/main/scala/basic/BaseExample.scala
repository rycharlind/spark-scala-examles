package basic

import org.apache.spark.SparkConf
import org.apache.spark.sql.{SparkSession, DataFrame}

object BasicExample {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("BasicExample")
      .setMaster("local[*]")

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
