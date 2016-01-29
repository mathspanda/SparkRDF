package org.mp.utils

import org.apache.jena.query.QueryFactory

import scala.collection.JavaConversions._

/**
  * Created by MP on 16/1/29.
  */
object QueryUtils {
  def fetchQueryVariables(queryStr: String): List[String] = {
    val query = QueryFactory.create(queryStr)
    if (query.isConstructType || query.isDescribeType) {
      return List("Subject", "Predicate", "Object")
    } else if (query.isAskType) {
      return List("Ask")
    }
    query.getResultVars.toList
  }
}
