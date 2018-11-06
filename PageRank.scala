import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
// Do NOT use different Spark libraries.

object PageRank {

  def main(args: Array[String]) {
    val inputDir = "sample-input"
    val linksFile = inputDir + "/links-simple-sorted.txt"
    val titlesFile = inputDir + "/titles-sorted.txt"
    val numPartitions = 10
    val numIters = 10

    val conf = new SparkConf()
      .setAppName("PageRank")
      .setMaster("local[*]")
      .set("spark.driver.memory", "1g")
      .set("spark.executor.memory", "2g")

    val sc = new SparkContext(conf)
    

    val links = sc
      .textFile(linksFile, numPartitions).map(x=>createTuple(x))
    

    val titles = sc
      .textFile(titlesFile, numPartitions)
      .zipWithIndex.map{case (k,v) => (v+1,k)}.map(x=>(x._1.toInt,x._2))
    // TODO
    // Setting inital variables : N,d and initial ranks
    val N = titles.count()
    val d= 0.85
    var ranks=titles.map { case (k, v) => (k, 100.0/N) }
    val counts=links.map{case(x,y)=> (x,y.length)}
    val left_ranks=titles.map { case (k, v) => (k, 100*(1.0-d)/N) } // Left side of the equation, will always be the same

       
    
    for (i <- 1 to 10) {
      
    var inlinks = links.flatMap(x=> splittuple(x._1,x._2)).join(ranks.join(counts))
        .map{case (id,(in,(rank,count)))=>(in,((d*rank)/count)) }.reduceByKey((a,b)=> (a+b))
    ranks=left_ranks.union(inlinks).reduceByKey((a,b)=>a+b)
    var norm = ranks.values.sum()
    ranks=ranks.map{case(x,y)=> (x,y*(100/norm))}
    ranks.collect.foreach(println)


    }
    

    println("[ PageRanks ]")
    ranks.collect().foreach(println)
    
    
    // TODO
  }
def createTuple(line: String)= {
   val ind= line.indexOf(":")
   val key= line.substring(0,ind).toInt
   val out =line.substring(ind+1,line.length).split("\\s+").filter(word => word != "").map(x=>x.toInt)
   (key,out)
 }
def splittuple(x:Int, y: Array[Int])={
   val n =y.length
   val out = new Array[(Int,Int)](n)
   for( a <- 0 to n-1){
         out(a)=(x,y(a))
      }
      out
}

}
