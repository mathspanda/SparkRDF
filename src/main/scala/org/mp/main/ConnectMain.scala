package org.mp.main

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}
import org.mp.jena.sparksql.JenaContext

/**
  * Created by MP on 16/1/29.
  */
object ConnectMain extends Serializable {

  val conf = new SparkConf().setAppName("Jena-SparkSql-Driver").setMaster("local")
  val sc = new SparkContext(conf)
  val sqlContext = new SQLContext(sc)

  def main(args: Array[String]) {
    val service = "http://localhost:3030/tdb/query"
    val query =
      """
        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX ub: <http://www.lehigh.edu/univ-bench.owl#>
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        SELECT ?X ?Y {?X ?Y ub:Professor}
      """
    val resultFrame = sqlContext.queryJena(service, query)
    resultFrame.show
  }
}
