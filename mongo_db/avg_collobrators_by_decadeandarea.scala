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

        // question numbers correspond to those in HW1
        q2(inproceedings)
        q3b(inproceedings)
        q3d(article, inproceedings)
        q4b(article, inproceedings)
    }

    /* Q2.
     * Add a column "Area" in the Inproceedings table.
     * Then, populate the column "Area" with the values from the above table if
     * there is a match, otherwise set it to "UNKNOWN" */
    def q2(inproceedings: RDD[(Object,BSONObject)]) {

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

    /* Q3b.
     * Find the TOP­20 authors who published the most number of papers in
     * "Database" (author published in multiple areas will be counted in all those
     * areas). */
    def q3b(inproceedings: RDD[(Object,BSONObject)]) {
        inproceedings.filter(x=> x._2.get("Area") =="Database").map(x=> x._2.get("authors")).flatMap(x=>toTuple(x.toString)).
        map(x=> (x._2,x._1)).reduceByKey(_+_).takeOrdered(20)(Ordering[Int].reverse.on(x => x._2)).foreach(println)
    }

    /* Q3d.
     * Find the number of authors who wrote more journal papers than conference
     * papers (irrespective of research areas). */
    def q3d(article: RDD[(Object,BSONObject)], inproceedings: RDD[(Object,BSONObject)]) {
        val journal_count=article.map(x=> x._2.get("authors")).flatMap(x=>toTuple(x.toString)).
        map(x=> (x._2,x._1)).reduceByKey(_+_).filter(x=> x._1 != "")
        val inproceeding_count=inproceedings.map(x=> x._2.get("authors")).flatMap(x=>toTuple(x.toString)).
        map(x=> (x._2,x._1)).reduceByKey(_+_).filter(x=> x._1 != "")


       val out = journal_count.leftOuterJoin(inproceeding_count)
       val count = out.filter(x=> x._2._2.isDefined).filter(x=> x._2._2.get< x._2._1).count()+out.filter(x=> x._2._2==None).count
       println(count)
    }

    /* Q4b.
     * Plot a barchart showing how the average number of collaborators varied in
     * these decades for conference papers in each of the four areas in Q3.
     * Again, the decades will be 1950-1959, 1960-1969, ...., 2000-2009, 2010-2015.
     * But for every decade, there will be four bars one for each area (do not
     * consider UNKNOWN), the height of the bars will denote the average number of
     * collaborators. */
    def q4b(article: RDD[(Object,BSONObject)], inproceedings: RDD[(Object,BSONObject)]) {


        val decades= Array(1950,1960,1970,1980,1990,2000,2010)
        val areas = Array("Database","ML-AI","Theory","Systems")
        var results = new Array[String](areas.length*decades.length)
        var k=0
        val base:String = "The avg. number of collobrators at year  "
        for( i <- 0 to decades.length-1){
        for( j <- 0 to areas.length-1){
        val avg=get_avg_collob(article,inproceedings,areas(j),decades(i))
            //results(0)="The average collobrators in year" + decades(i).toString + "in the Area" + areas(j) +" is:  " +avg.toString
             
            results(k)=base+ decades(i)+ " in the area " + areas(j) + " is :   "+avg.toString
            k=k+1
        }    

        }
       results.foreach(println)

         

    }

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










def get_authors(line:String)={
    val y =line.split(",")
    val out = new Array[(String,Int)](y.length)
     for( a <- 0 to y.length-1){
         out(a)=(y(a).replace("[","").replace("]","").trim,1)
      }
      out



}


def toTuple( line: String)={
    val y =line.split(",")
    val out = new Array[(Int,String)](y.length)

   for( a <- 0 to y.length-1){
         out(a)=(1,y(a).replace("[","").replace("]","").trim)
      }
      out
}

def get_collob(line: String): Array[(String,String)] ={
    val y =line.split(",")
    var k =0
    val out = new Array[(String,String)]((y.length*(y.length-1))/2)
    for( i <- 0 to y.length-1){
        for( j <- i+1 to y.length-1){
            var a=y(i).replace("[","").replace("]","").trim
            var b=y(j).replace("[","").replace("]","").trim
         out(k)=(a,b)
         k=k+1
      }
}
 out
}

def getDecade(s: String): Int = {
  try {
    val a= s.toInt
    a-a%10
  } catch {
    case e: Exception => 0
  }
}

def get_avg_collob(article: RDD[(Object,BSONObject)], inproceedings: RDD[(Object,BSONObject)],area:String,decade:Int) :Double={

        val inpro_by_area=inproceedings.filter(x=> x._2.get("Area")!="UNKNOWN").map(x=>(x._2.get("authors"),x._2.get("Area")))
        val authors= inpro_by_area.filter(x=> x._2== area).flatMap(x=> get_authors(x._1.toString))


        val collob_bydecade= article.map(x=> (x._2.get("authors"),getDecade(x._2.get("year").toString))).union(inproceedings.map(x=> (x._2.get("authors"),getDecade(x._2.get("year").toString))))
        .filter(x=> x._2 == decade).flatMap(x=> get_collob(x._1.toString)).distinct.flatMap{case (k,v) => (Array((k,1),(v,1))) }.reduceByKey(_+_)

         val oo=collob_bydecade.subtractByKey(authors)

        oo.values.sum()/oo.count


}




}
