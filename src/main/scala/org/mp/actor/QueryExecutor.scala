package org.mp.actor

import akka.actor.{ActorSystem, Props, ActorRef, Actor}
import akka.routing.RoundRobinRouter
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.mp.base.SparqlQuery
import org.mp.jena.sparksql.JenaContext

import scala.collection.mutable

/**
  * Created by MP on 16/1/30.
  */
sealed trait QueryMessage
case object START extends QueryMessage
case object MASTER_FINISH extends QueryMessage
case object SLAVE_FINISH extends QueryMessage

class QueryExecutor(service: String, queries: List[SparqlQuery])(@transient sqlContext: SQLContext) {

  val dataFrameMap = new mutable.HashMap[String, DataFrame]

  var system: ActorSystem = null
  var stop: Boolean = false

  def start() {
    system = ActorSystem("QueryExecutor")
    val master = system.actorOf(Props(new QueryExecutorMaster))
    master ! START
    system.awaitTermination()
  }

  def compute(): DataFrame = {
    if (!stop) return null

    if (queries.length > 1) {
      queries.sortWith((q1, q2) => q1.vars.length >= q2.vars.length)
             .map(query => dataFrameMap(query.queryMD5))
             .reduce((d1, d2) => {
               val columns = d1.columns intersect d2.columns
               d1.join(d2, columns)
             })
    } else {
      dataFrameMap(queries(0).queryMD5)
    }
  }

  class QueryExecutorMaster extends Actor {

    var finish: Int = 0

    val slaveRouter = context.actorOf(
      Props(new QueryExecutorSlave).withRouter(RoundRobinRouter(queries.size)),
      name = "QueryMaster")

    def receive = {
      case START => {
        for (query <- queries) {
          slaveRouter ! query
        }
      }
      case SLAVE_FINISH => {
        finish += 1
        if (finish == queries.size) {
          stop = true
          context.stop(self)
          system.shutdown()
        }
      }
    }
  }

  class QueryExecutorSlave extends Actor {
    def receive = {
      case query: SparqlQuery => {
        val dataFrame = sqlContext.queryJena(service, query.query)
        dataFrameMap(query.queryMD5) = dataFrame
        dataFrame.cache
        sender ! SLAVE_FINISH
      }
    }
  }

}
