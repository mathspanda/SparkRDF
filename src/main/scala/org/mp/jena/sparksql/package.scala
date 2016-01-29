package org.mp.jena

import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.types.StructType

/**
  * Created by MP on 16/1/29.
  */
package object sparksql {
  implicit class JenaContext(sqlContext: SQLContext) extends Serializable {
    def queryJena(service: String, query: String, jenaSchema: StructType = null): DataFrame = {
      val jenaRelation = JenaRelation(service, query, jenaSchema)(sqlContext)
      sqlContext.baseRelationToDataFrame(jenaRelation)
    }
  }
}
