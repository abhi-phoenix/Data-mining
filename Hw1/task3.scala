import org.apache.spark.sql.SparkSession
import org.apache.spark._
import org.json4s._
import org.json4s.jackson.JsonMethods._
import java.io.File
import java.io.PrintWriter
import org.json4s.jackson.Serialization.writePretty
import scala.collection.mutable.ArrayBuffer


object task3 {
  def main(args: Array[String]): Unit = {
    val ss = SparkSession
      .builder()
      .appName(name ="task1")
      .config("spark.master","local[*]")
      .getOrCreate()

    val sc = ss.sparkContext
    sc.setLogLevel("ERROR")

    val review_file_path = args(0)
    val output_file_path = args(1)
    val partition_type = args(2)
    val n_partitions = args(3).toInt
    val n = args(4).toInt

    def drill_press(filename: String, s: String) {
      val writer = new PrintWriter(new File(filename))
      writer.write(s)
      writer.close()
    }

    def json_file_parse(s: String)  =
      try {
        implicit val formats = DefaultFormats
        parse(s).extract[JValue]
      }
      catch {
        case t: Throwable =>
          throw t
      }

    val rdd = sc.textFile(review_file_path)
    val parsing_row = rdd.map(row => json_file_parse(row))
    //implicit val formats = DefaultFormats
    if( partition_type == "default") {
      val Map1 = parsing_row.map(row => ( {
        implicit val json_trots = DefaultFormats;
        (row \ "business_id").extract[String];
      }, {
        implicit val json_trots = DefaultFormats;
        (row \ "text").extract[String]
      })).filter(row => row._2 != null)
      val temp_map = Map1.map(row => (row._1, 1))
      val reduced = temp_map.reduceByKey(_ + _).filter(row => row._2 > n).collect().toList.map(row => List(row._1, row._2))
      val n_partions = Map1.partitions.size
      val n_item = Map1.mapPartitionsWithIndex { case (i, iters) => Iterator(iters.size) }.collect().toList
      println(n_item)
      //val items = Map1.mapPartitions(iter => Array(iter.size).iterator, true).collect()
      val my_sweat = Map[String, Any]("n_partitions" -> n_partions, "n_items" -> n_item, "result" -> reduced)
      implicit val formats = DefaultFormats
      drill_press(output_file_path, writePretty(my_sweat).toString)
      //.filter(row => row(1)>n )

    }
    else {
      val Map1 = parsing_row.map(row => ( {
        implicit val json_trots = DefaultFormats;
        (row \ "business_id").extract[String];
      }, {
        implicit val json_trots = DefaultFormats;
        (row \ "text").extract[String]
      })).filter(row => row._2 != null)

      val custom =  Map1.partitionBy(new HashPartitioner(n_partitions))
      val temp_map = custom.map(row => (row._1, 1))
      val reduced = temp_map.reduceByKey(_ + _).filter(row => row._2 > n).collect().toList.map(row => List(row._1, row._2))
      val n_partions = custom.getNumPartitions
      val n_item = custom.mapPartitionsWithIndex{case (i,iters) => Iterator(iters.size)}.collect().toList
      val my_sweat = Map[String, Any]( "n_partitions" -> n_partions, "n_items" -> n_item, "result" -> reduced)
      implicit val formats = DefaultFormats
      drill_press(output_file_path, writePretty(my_sweat).toString)
    }

    /*
    if partition_type == "default" {

    }
    else{

    }
    */
  }


  }
