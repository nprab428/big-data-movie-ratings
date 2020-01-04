import kafka.serializer.StringDecoder
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.spark.SparkConf
import com.fasterxml.jackson.databind.{ DeserializationFeature, ObjectMapper }
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.client.Delete
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.client.Durability
import org.apache.hadoop.hbase.client.Increment
import org.apache.hadoop.hbase.util.Bytes
import Array._

object StreamRatings {
  val mapper = new ObjectMapper()
  mapper.registerModule(DefaultScalaModule)
  val hbaseConf: Configuration = HBaseConfiguration.create()
  hbaseConf.set("hbase.zookeeper.property.clientPort", "2181")
  
  // Use the following two lines if you are building for the cluster 
  hbaseConf.set("hbase.zookeeper.quorum","mpcs53014c10-m-6-20191016152730.us-central1-a.c.mpcs53014-2019.internal")
  hbaseConf.set("zookeeper.znode.parent", "/hbase-unsecure")
  
  // Use the following line if you are building for the VM
  // hbaseConf.set("hbase.zookeeper.quorum", "localhost")
  
  val hbaseConnection = ConnectionFactory.createConnection(hbaseConf)
  val movieLinks = hbaseConnection.getTable(TableName.valueOf("nprabhu_movie_links_hbase"))
  val movieGenres = hbaseConnection.getTable(TableName.valueOf("nprabhu_movie_genres_hbase"))
  val movieRatingsCount = hbaseConnection.getTable(TableName.valueOf("nprabhu_movie_ratings_count_hbase"))
  val movieRatingsAvg = hbaseConnection.getTable(TableName.valueOf("nprabhu_movie_ratings_average_by_genre_hbase"))
  val movieRatingsSecIx = hbaseConnection.getTable(TableName.valueOf("nprabhu_movie_ratings_average_secondary_index"))
  
  //https://alvinalexander.com/scala/how-to-compare-floating-point-numbers-in-scala-float-double
  def compareFloat(x: Double, y: Double, precision: Double) = {
    if ((x - y).abs < precision) true else false
  }

  def colToBigInt(row: Result, colFamily: String, colName: String) : BigDecimal = {
    return Bytes.toBigDecimal(row.getValue(Bytes.toBytes(colFamily), Bytes.toBytes(colName)))
  }

  def colToString(row: Result, colFamily: String, colName: String) : String = {
    return Bytes.toString(row.getValue(Bytes.toBytes(colFamily), Bytes.toBytes(colName)))
  }

  def ratingAverage(row: Result) : BigDecimal = {
      return (0.5 * colToBigInt(row, "ratings", "half") +
                 1 * colToBigInt(row, "ratings", "one") +
                 1.5 * colToBigInt(row, "ratings", "one_and_a_half") +
                 2 * colToBigInt(row, "ratings", "two") +
                 2.5 * colToBigInt(row, "ratings", "two_and_a_half") +
                 3 * colToBigInt(row, "ratings", "three") +
                 3.5 * colToBigInt(row, "ratings", "three_and_a_half") +
                 4 * colToBigInt(row, "ratings", "four") +
                 4.5 * colToBigInt(row, "ratings", "four_and_a_half") +
                 5 * colToBigInt(row, "ratings", "five")) /
                 colToBigInt(row, "ratings", "total")
  }
  
  def getImdbId(movieId: Int) : String = {
    val linkRow = movieLinks.get(new Get(Bytes.toBytes(movieId.toString())))
     if(linkRow.isEmpty()){
      return ""
     }
    return colToString(linkRow, "links", "imdbid")
  }

  def getGenres(imdbId: String) : Array[String] = {
    val genreRow = movieGenres.get(new Get(Bytes.toBytes(imdbId)))
    val genresStr = colToString(genreRow, "genres", "genres")
    return concat(genresStr.split(":::"), Array("ALL"))
  }

  def incrementRatings(rr : RatingRecord) : String = {
    // Map movieId to ImdbId
    val imdbId = getImdbId(rr.movieId)
    if(imdbId == ""){
      return f"No imdbId for movieId: " + rr.movieId;
    }

    // Get row in movieRatingsCount
    val imdbIdKey = Bytes.toBytes(imdbId)
    val ratingsRow = movieRatingsCount.get(new Get(imdbIdKey))
    if(ratingsRow.isEmpty()){
      return f"No ratings for imdbId: " + imdbId;
    }
    
    // Increment movieRatingsCount table with the new rating
    val inc = new Increment(imdbIdKey)
    inc.setDurability(Durability.ASYNC_WAL)
    inc.addColumn(Bytes.toBytes("ratings"), Bytes.toBytes("total"), 1)
    if(compareFloat(rr.rating, 5.0, 0.0001)){
      inc.addColumn(Bytes.toBytes("ratings"), Bytes.toBytes("five"),  1)
    } 
    movieRatingsCount.increment(inc)

    // Next compute the new average rating for the movie
    val newRatingsRow = movieRatingsCount.get(new Get(imdbIdKey))
    val newAvg = "%.7f".format(ratingAverage(newRatingsRow))

    // Get all genres for the movie
    val genresList = getGenres(imdbId)

    // Get old avgRating
    val secIxRow = movieRatingsSecIx.get(new Get(imdbIdKey))
    val oldAvg = colToString(secIxRow, "ratings", "avgRating")

    // Replace rows in movieRatingsAvg table (for every genre)
    for(g <- genresList){
      movieRatingsAvg.delete(new Delete(Bytes.toBytes(g + ":::" + oldAvg + ":::" + imdbId)))
      val newAvgKey = Bytes.toBytes(g + ":::" + newAvg + ":::" + imdbId)
      val put = new Put(newAvgKey)
      put.addColumn(Bytes.toBytes("ratings"), Bytes.toBytes("avgRating"), Bytes.toBytes(newAvg))
      put.addColumn(Bytes.toBytes("ratings"), Bytes.toBytes("imdbId"), imdbIdKey)
      put.addColumn(Bytes.toBytes("ratings"), Bytes.toBytes("title"), Bytes.toBytes(colToString(newRatingsRow,"ratings","title")))
      put.addColumn(Bytes.toBytes("ratings"), Bytes.toBytes("year"), Bytes.toBytes(colToString(newRatingsRow,"ratings","year")))
      movieRatingsAvg.put(put)
    }

    // Update ratingKey value in secondary index table
    val update = new Put(imdbIdKey)
    update.addColumn(Bytes.toBytes("ratings"), Bytes.toBytes("avgRating"), Bytes.toBytes(newAvg))
    movieRatingsSecIx.put(update)

    return "Updated movie ratings for imdbId: " + imdbId + " with: " + rr.rating + " and new avg: " + newAvg
  }
  
  def main(args: Array[String]) {
    if (args.length < 1) {
      System.err.println(s"""
        |Usage: StreamRatings <brokers> 
        |  <brokers> is a list of one or more Kafka brokers
        | 
        """.stripMargin)
      System.exit(1)
    }
    
    val Array(brokers) = args

    // Create context with 2 second batch interval
    val sparkConf = new SparkConf().setAppName("StreamRatings")
    val ssc = new StreamingContext(sparkConf, Seconds(2))

    // Create direct kafka stream with brokers and topics
    val topicsSet = Set("nprabhu_movie_ratings")
    // Create direct kafka stream with brokers and topics
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)
    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topicsSet)
   
    val serializedRecords = messages.map(_._2); // get second attrib of tuple

    val kfrs = serializedRecords.map(rec => mapper.readValue(rec, classOf[RatingRecord]))

    // Update speed table
    val processedRatings = kfrs.map(incrementRatings)
    processedRatings.print()
    
    // Start the computation
    ssc.start()
    ssc.awaitTermination()
  }

}