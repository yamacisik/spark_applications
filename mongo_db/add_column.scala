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
        article_input_conf.set("mongo.input.uri", "mongodb://localhost:27017/dlbp.Articles")
        article_input_conf.set("mongo.splitter.class","com.mongodb.hadoop.splitter.StandaloneMongoSplitter")

        val inproceedings_input_conf = new Configuration()
        inproceedings_input_conf.set("mongo.input.uri", "mongodb://localhost:27017/dlbp.Inproceedings")
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

    
    /* Add a column "Area" in the Inproceedings table.
     * Then, populate the column "Area" with the values from the fields given below if
     * there is a match, otherwise set it to "UNKNOWN" */


 val inpro_config = new Configuration()
        inpro_config.set("mongo.output.uri",
        "mongodb://localhost:27017/hw3db.Inproceedings")


        val updates = inproceedings.map{case (k, v) => (k,new MongoUpdateWritable(
      new BasicDBObject("_id", v.get("_id")),  // Query
      new BasicDBObject("$set", new BasicDBObject("Area", set_Conference(v.get("booktitle").toString()))),  // Update operation
      false,  // b
      false,   // Update multiple documents
      false
    )
  )
}

     updates.saveAsNewAPIHadoopFile(
    "file:///this-is-completely-unused",
    classOf[Object],
    classOf[MongoUpdateWritable],
    classOf[MongoOutputFormat[Object, MongoUpdateWritable]],
    inpro_config)


    }

  // Helper function to get areas of based on the journal where each paper is published

  def set_Conference(conf: String)= {
   val dat: String = "Database"
   val the: String = "Theory"
   val sys: String ="Systems"
   val ml: String = "ML-AI"
   val unk: String =  "UNKNOWN"

   val Database= List("SIGMOD Conference","VLDB","ICDE","PODS")
   val Theory= List("STOC","FOCS","SODA","ICALP")
   val Systems=List("SIGCOMM","ISCA","HPCA","PLDI")
   val ML_AI=List("ICML","NIPS","AAAI","IJCAI")

   if(Database contains conf){
    dat
} else if(Theory contains conf){
  the
}  else if(Systems contains conf){
  sys
}  else if(ML_AI contains conf){
  ml
} 
else {
  unk
}
}


}
