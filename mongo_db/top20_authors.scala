import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

import org.apache.hadoop.conf.Configuration
import org.bson.BSONObject

import com.mongodb.{
    MongoClient,
    MongoException,
    WriteConcern,
    DB,
    DBCollection,
    BasicDBObject,
    BasicDBList,
    DBObject,
    DBCursor
}

import com.mongodb.hadoop.{
    MongoInputFormat,
    MongoOutputFormat,
    BSONFileInputFormat,
    BSONFileOutputFormat
}

import com.mongodb.hadoop.io.MongoUpdateWritable


object MongoSpark {
    def main(args: Array[String]) {
        /* Uncomment to turn off Spark logs */
        //Logger.getLogger("org").setLevel(Level.OFF)
        //Logger.getLogger("akka").setLevel(Level.OFF)

        val conf = new SparkConf()
            .setMaster("local[*]")
            .setAppName("MongoSpark")
            .set("spark.driver.memory", "2g")
            .set("spark.executor.memory", "4g")

        val sc = new SparkContext(conf)

        val article_input_conf = new Configuration()
        article_input_conf.set("mongo.input.uri", "mongodb://localhost:27017/hw3db.Articles")
        article_input_conf.set("mongo.splitter.class","com.mongodb.hadoop.splitter.StandaloneMongoSplitter")

        val inproceedings_input_conf = new Configuration()
        inproceedings_input_conf.set("mongo.input.uri", "mongodb://localhost:27017/hw3db.Inproceedings")
        inproceedings_input_conf.set("mongo.splitter.class","com.mongodb.hadoop.splitter.StandaloneMongoSplitter")

        

        val article = sc.newAPIHadoopRDD(
            article_input_conf,         // config
            classOf[MongoInputFormat],  // input format
            classOf[Object],            // key type
            classOf[BSONObject]         // val type
        )

        val inproceedings = sc.newAPIHadoopRDD(
            inproceedings_input_conf,
            classOf[MongoInputFormat],
            classOf[Object],
            classOf[BSONObject]
        )

 /* Task 2
     * Find the TOP­ k authors who published the most number of papers in
     * the area (author published in multiple areas will be counted in all those
     * areas). */
     top_k(inproceedings,"Database",20) // Top 20 authors in Database

    }


    def top_k(inproceedings: RDD[(Object,BSONObject)],area: String, k:Int) {
        inproceedings.filter(x=> x._2.get("Area") ==area).map(x=> x._2.get("authors")).flatMap(x=>toTuple(x.toString)).
        map(x=> (x._2,x._1)).reduceByKey(_+_).takeOrdered(20)(Ordering[Int].reverse.on(x => x._2)).foreach(println)
    }


//Helper function to get the authors from the mongo Database
def toTuple( line: String)={
    val y =line.split(",")
    val out = new Array[(Int,String)](y.length)

   for( a <- 0 to y.length-1){
         out(a)=(1,y(a).replace("[","").replace("]","").trim)
      }
      out
}

}
