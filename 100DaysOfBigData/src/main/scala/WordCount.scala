import org.apache.spark.sql.SparkSession
import org.apache.log4j._
object WordCount {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val sc=SparkSession.builder().master("local[*]").appName("WordCount").getOrCreate()
    val wordRdd=sc.sparkContext.parallelize(List("hai","hello","hai","hai","jithin","hello","jithin","jithin","jithin"))
    val count=wordRdd.countByValue().foreach(println)
    sc.stop()
  }
}
