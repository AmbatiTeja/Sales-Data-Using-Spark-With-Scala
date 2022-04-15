import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkContext;


object TotaSpent extends App{
    
  Logger.getLogger("org").setLevel(Level.ERROR)

  val sc = new SparkContext("local[*]","sales");
  
  val input = sc.textFile("C:/Users/Akhil/Desktop/Datasets/customerorders.csv");
  
  val mappedInput = input.map(x => (x.split(",")(0),x.split(",")(2).toFloat));
  
  val totalByCustomer = mappedInput.reduceByKey((x,y)=> x+y);
  
  val sortedTotal = totalByCustomer.sortBy(x => x._2);
  
  val result = sortedTotal.collect;
  
  result.foreach(println);
  
  //val data = input.flatMap(x=>x.split(","));
  // data.map();
  
  scala.io.StdIn.readLine();
  
}
