package org.mp.sparql2sparksql

import org.apache.jena.graph.Node
import org.apache.jena.query.QueryFactory
import org.apache.jena.sparql.algebra.{OpWalker, Algebra}
import org.mp.base.SparqlQuery

import scala.collection.JavaConversions._

/**
  * Created by MP on 16/1/30.
  */
class Parser(queryStr: String) {
  private val query = QueryFactory.create(queryStr)
  private val visitor = SparkSQLOpVisitor("TEST")
  OpWalker.walk(Algebra.compile(query), visitor)

  val sql = visitor.generateSQL
  val prefixStr = query.getPrefixMapping()
                       .getNsPrefixMap
                       .toList
                       .map(prefix => String.format("PREFIX %s: %s", prefix._1, prefix._2))
                       .mkString("\n")

  val queries = visitor.queries
}

object Parser {
  def apply(queryStr: String) = new Parser(queryStr)
}
