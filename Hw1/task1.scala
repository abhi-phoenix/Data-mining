import org.apache.spark.sql.SparkSession
import org.json4s._
import org.json4s.jackson.JsonMethods._
import java.io.File
import java.io.PrintWriter
import org.json4s.jackson.Serialization.writePretty
import scala.collection.mutable.ArrayBuffer


object task1 {
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
    val stop_words_file = args(2)
    val y = args(3)
    val m = args(4).toInt
    val n = args(5).toInt

    def drill_press(filename: String, s: String) {
      val writer = new PrintWriter(new File(filename))
      writer.write(s)
      writer.close()
    }

    def json_file_parse(s: String)  =
      try {
      implicit val json_trots = DefaultFormats
      parse(s).extract[JValue]
    }
      catch {
      case t: Throwable =>
        throw t
    }

    val punc = Array("(", "[", ",", ".", "!", "?", ":", ";", "]", ")")
    val stop_words = ArrayBuffer[String]()
    for (pined <- scala.io.Source.fromFile(stop_words_file).getLines)
    {
      val temp = pined.mkString
      stop_words += temp
    }

    val rdd = sc.textFile(review_file_path)
    val parsing_row = rdd.map(row => json_file_parse(row))
    implicit val json_trots = DefaultFormats
    val Map1 = parsing_row.map(row => {implicit val json_trots = DefaultFormats;(row \ "review_id").extract[String]})
      val reduce1 = Map1.count()

    val Map2 = parsing_row.map(row => {implicit val json_trots = DefaultFormats;(row \ "date").extract[String]})
      val filter2 = Map2.filter( row => row.split("-")(0) == y)
        val reduce2 = filter2.count()

    val Map3 = parsing_row.map(row => {implicit val json_trots = DefaultFormats;(row \ "user_id").extract[String]})
      val reduce3 = Map3.distinct().count()

    val Map4 = parsing_row.map(row=>{implicit val json_trots = DefaultFormats;(row \ "user_id").extract[String]}).map( row => (row,1))
    val intermediate = Map4.reduceByKey(_+_)
    //val intermediate2 = intermediate.toSeq
    val sorting  = intermediate.sortBy(row => (-row._2,row._1))
    val reduce4 = intermediate.take(m).toList.map(row => List(row._1, row._2))

    val Map5 = parsing_row.map(row => {implicit val json_trots = DefaultFormats;(row \ "text").extract[String]})
    val process = Map5.flatMap(row => "[\\w']+".r.findAllIn(row))
    val mapping_key_value = process.map( row => (row,1))
    val filtered = mapping_key_value.filter(row => !(stop_words.contains(row._1)) && !(punc.contains(row._1)) && row._1 != "" && row._1.toLowerCase().equals(row._1))
    val reduce_final = filtered.reduceByKey(_+_)
    val reduce5 = reduce_final.sortBy(row => (-row._2,row._1)).take(n).toList.map(row => row._1)
    //val reduce55 = List[String]()
    //val ans_e_1 = reduce_final.map(row=> reduce55:+row._2)
    //val reduce5 = ans_e_1.map(row => row(0))

    //println(reduce5)
    /// HERHERHE

    val my_sweat = Map("A" ->reduce1,"B" -> reduce2, "C" -> reduce3, "D" -> reduce4, "E" -> reduce5)
    drill_press(output_file_path, writePretty(my_sweat).toString)


  }


}
