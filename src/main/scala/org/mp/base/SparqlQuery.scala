package org.mp.base

import org.apache.jena.graph.{Node, Triple}
import org.mp.utils.HashUtils

/**
  * Created by MP on 16/2/1.
  */
class SparqlQuery(val vars: List[String], val query: String) {
  val queryMD5 = HashUtils.hash2MD5(query)

  override def toString = s"$query : $queryMD5"
}

object SparqlQuery {
  def apply(triple: Triple): SparqlQuery = {
    val nodeList = List(triple.getSubject, triple.getPredicate, triple.getObject)
    val vars = nodeList.filter(_.isVariable).map(_.getName)

    val nodeStrList = nodeList.map(node2String)
    val tripleStr = nodeStrList.mkString(" ")
    val varStr = nodeStrList.filter(_.startsWith("?")).mkString(" ")
    val query = s"SELECT $varStr { $tripleStr }"

    new SparqlQuery(vars, query)
  }

  private def node2String(node: Node): String = {
    if (node.isURI) {
      return "<" + node.toString + ">"
    } else if (node.isLiteral) {
      return "\"" + node.getLiteralValue + "\""
    }
    node.toString
  }
}
