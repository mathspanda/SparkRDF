package org.mp.main

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}
import org.mp.actor.QueryExecutor
import org.mp.jena.sparksql.JenaContext
import org.mp.sparql2sparksql.Parser

/**
  * Created by MP on 16/1/30.
  */
object ParseMain extends Serializable {

  val conf = new SparkConf().setAppName("SPARQL-Parser").setMaster("local")
  val sc = new SparkContext(conf)
  val sqlContext = new SQLContext(sc)

  def main(args: Array[String]) {
    val service = "http://localhost:3030/tdb/query"
    val queryStr =
      """
        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX ub: <http://www.lehigh.edu/univ-bench.owl#>
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        SELECT ?X ?Y ?Z {
        ?X rdf:type ub:GraduateStudent .
        ?Y rdf:type ub:University .
        ?Z rdf:type ub:Department .
        ?X ub:memberOf ?Z .
        ?Z ub:subOrganizationOf ?Y .
        ?X ub:undergraduateDegreeFrom ?Y }
      """

    val parser = Parser(queryStr)
    val queries = parser.queries
    val qexec = new QueryExecutor(service, queries)(sqlContext)
    qexec.start()

    while(!qexec.stop) {}
    val dataFrame = qexec.dataFrameMap
  }
}
