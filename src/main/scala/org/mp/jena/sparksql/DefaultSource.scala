package org.mp.jena.sparksql

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.sources.{DataSourceRegister, SchemaRelationProvider, RelationProvider}
import org.apache.spark.sql.types.StructType

/**
  * Created by MP on 16/1/29.
  */
class DefaultSource extends RelationProvider with SchemaRelationProvider with DataSourceRegister {
  override def shortName(): String = "jena"

  override def createRelation(sqlContext: SQLContext,
                              parameters: Map[String, String]): JenaRelation = {
    createRelation(sqlContext, parameters, null)
  }

  override def createRelation(sqlContext: SQLContext,
                              parameters: Map[String, String],
                              schema: StructType): JenaRelation = {
    JenaRelation(checkService(parameters), checkQuery(parameters), schema)(sqlContext)
  }

  private def checkService(parameters: Map[String, String]): String = {
    parameters.getOrElse("service", null)
  }

  private def checkQuery(parameters: Map[String, String]): String = {
    parameters.getOrElse("query", null)
  }
}
