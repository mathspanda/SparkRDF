package org.mp.sparql2sparksql

import org.apache.jena.graph.Node
import org.apache.jena.query.QueryFactory
import org.apache.jena.sparql.algebra.{OpWalker, Algebra}

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

  val queryVars = visitor.tripleList.map(triple => {
    List(triple.getSubject, triple.getPredicate, triple.getObject)
      .filter(_.isVariable)
      .map(_.getName)
  })

  val queries = visitor.tripleList.map(triple => {
    val itemList = List(node2String(triple.getSubject),
                       node2String(triple.getPredicate),
                       node2String(triple.getObject))
    val tripleStr = itemList.mkString(" ")
    val varStr = itemList.filter(_.startsWith("?")).mkString(" ")

    (varStr, tripleStr)
  }).map(t => "SELECT " + t._1 + " {" + t._2 + "}")

  private def node2String(node: Node): String = {
    if (node.isURI) {
      return "<" + node.toString + ">"
    } else if (node.isLiteral) {
      return "\"" + node.getLiteralValue + "\""
    }
    node.toString
  }
}

object Parser {
  def apply(queryStr: String) = new Parser(queryStr)
}
