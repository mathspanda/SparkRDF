package org.mp.jena.sparksql

import java.sql.{ResultSet, DriverManager, Connection}

import org.apache.jena.jdbc.mem.MemDriver
import org.apache.jena.jdbc.remote.RemoteEndpointDriver
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SQLContext, Row}
import org.apache.spark.sql.sources.{TableScan, BaseRelation}
import org.apache.spark.sql.types._
import org.mp.utils.{QueryUtils => MPQueryUtils}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by MP on 16/1/29.
  */
case class JenaRelation (
    service: String,
    query: String,
    jenaSchema: StructType)(@transient val sqlContext: SQLContext)
  extends BaseRelation with TableScan {

  override val schema: StructType = {
    if (jenaSchema == null) {
      val varNames = MPQueryUtils.fetchQueryVariables(query)
      val schemaFields = varNames.map(StructField(_, StringType, nullable = false))
      StructType(schemaFields)
    } else {
      jenaSchema
    }
  }

  override def buildScan(): RDD[Row] = {
    val schemaFields = schema.fields

    val conn: Connection = if (service.startsWith("mem")) {
      MemDriver.register()
      DriverManager.getConnection(s"jdbc:jena:$service")
    } else {
      RemoteEndpointDriver.register()
      DriverManager.getConnection(s"jdbc:jena:remote:query=$service")
    }

    try {
      val stmt = conn.createStatement()
      try {
        val rs = stmt.executeQuery(query)
        val rows = ArrayBuffer[Row]()

        while (rs.next()) {
          val values = schemaFields.map(extractValueByDataType(rs, _))
          rows += Row.fromSeq(values)
        }

        sqlContext.sparkContext.makeRDD(rows)
      } finally {
        stmt.close()
      }
    } finally {
      conn.close()
    }
  }

  private def extractValueByDataType(rs: ResultSet, field: StructField): AnyRef = {
    val name = field.name
    field.dataType match {
      case ByteType =>
        val v = rs.getByte(name)
        if (rs.wasNull()) null else byte2Byte(v)
      case ShortType =>
        val v = rs.getShort(name)
        if (rs.wasNull()) null else short2Short(v)
      case IntegerType =>
        val v = rs.getInt(name)
        if (rs.wasNull()) null else int2Integer(v)
      case LongType =>
        val v = rs.getLong(name)
        if (rs.wasNull()) null else long2Long(v)
      case FloatType =>
        val v = rs.getFloat(name)
        if (rs.wasNull()) null else float2Float(v)
      case DoubleType =>
        val v = rs.getDouble(name)
        if (rs.wasNull()) null else double2Double(v)
      case StringType =>
        rs.getString(name)
      case BooleanType =>
        val v = rs.getBoolean(name)
        if (rs.wasNull()) null else boolean2Boolean(v)
      case TimestampType =>
        rs.getTimestamp(name)
      case _ =>
        rs.getString(name)
    }
  }
}
