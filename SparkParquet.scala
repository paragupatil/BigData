
/*Write an application using Python, Scala or Java that will use Spark to do the following:
 Read the data file ‘data.csv’.
 Create an optimised parquet file with the same data
 Load the parquet file into Spark
 Aggregate the values by country
 Write the results to a parquet file */

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger
import org.apache.log4j.Level

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._

 object SparkParquet {
  case class CountryValues(CountryName:String, Val1:Int, Val2:Int, Val3:Int, Val4: Int, Val5:Int)

  def main(args: Array[String]): Unit = {
    /*val conf = new SparkConf().
        setMaster("local").
        setAppName("Get country-wise aggregation")
     val sc = new SparkContext(conf)
     sc.setLogLevel("ERROR")

     val sqc = SQLContext(sc)
*/

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder()
                            .appName("Get country-wise aggregation")
                            .master("local")
                            .getOrCreate()

    val sc = spark.sparkContext

    //Read the data file ‘data.csv’.
   //"file:///c:\\tmp\\data\\data.csv"
    val rdd = sc.textFile(args(0))
    val header = rdd.first()

    val rdd1 = rdd.filter(x => x!=header)

    val rdd2 = rdd1.map(x=> {
        val line = x.split(Array(',',';'))
        val ret = CountryValues(line(0),
                                line(1).toInt,line(2).toInt,line(3).toInt, line(4).toInt,line(5).toInt)
        ret
    }
    )

    import spark.implicits._

    val df = rdd2.toDF()
    val inputPath = args(1) //"file:///c:\\work\\inputdata.parquet"
    val outputPath = args(2) //"file:///c:\\work\\outputdata.parquet"

    //Create an optimised parquet file with the same data
    df.write.parquet(inputPath)

    //Load the parquet file into Spark
    val InputDF = spark.read.parquet(inputPath)

    InputDF.createOrReplaceTempView("mydf")

    //Aggregate the values by country
    val OutputDF = spark.sql("Select CountryName as Country, " +
      "                               concat(Sum(Val1),';',Sum(Val2),';',Sum(Val3),';'," +
      "                               Sum(Val4),';',Sum(Val5)) as Values " +
      "                               From mydf group by CountryName order by CountryName")

    //Write the results to a parquet file
    OutputDF.write.parquet(outputPath)

   }
}
