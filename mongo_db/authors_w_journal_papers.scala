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

    /* Task 3
     * Find the number of authors who wrote more journal papers than conference
     * papers (irrespective of research areas). */

  val journal_count=article.map(x=> x._2.get("authors")).flatMap(x=>toTuple(x.toString)).
        map(x=> (x._2,x._1)).reduceByKey(_+_).filter(x=> x._1 != "")
        val inproceeding_count=inproceedings.map(x=> x._2.get("authors")).flatMap(x=>toTuple(x.toString)).
        map(x=> (x._2,x._1)).reduceByKey(_+_).filter(x=> x._1 != "")


       val out = journal_count.leftOuterJoin(inproceeding_count)
       val count = out.filter(x=> x._2._2.isDefined).filter(x=> x._2._2.get< x._2._1).count()+out.filter(x=> x._2._2==None).count
       println(count)
    }


// Helper function for parsing the authors from the mongo database
def toTuple( line: String)={
    val y =line.split(",")
    val out = new Array[(Int,String)](y.length)

   for( a <- 0 to y.length-1){
         out(a)=(1,y(a).replace("[","").replace("]","").trim)
      }
      out
}

}
