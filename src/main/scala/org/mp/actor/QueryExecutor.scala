package org.mp.actor

import akka.actor.{ActorSystem, Props, ActorRef, Actor}
import akka.routing.RoundRobinRouter
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.mp.jena.sparksql.JenaContext

import scala.collection.mutable

/**
  * Created by MP on 16/1/30.
  */
sealed trait QueryMessage
case object START extends QueryMessage
case object MASTER_FINISH extends QueryMessage
case object SLAVE_FINISH extends QueryMessage

class QueryExecutor(service: String, queries: List[String])(@transient sqlContext: SQLContext) {

  val dataFrameMap = new mutable.HashMap[String, DataFrame]

  var system: ActorSystem = null
  var stop: Boolean = false

  def start() {
    system = ActorSystem("QueryExecutor")
    val master = system.actorOf(Props(new QueryExecutorMaster))
    master ! START
    system.awaitTermination()
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
      case query: String => {
        val dataFrame = sqlContext.queryJena(service, query)
        dataFrameMap(query) = dataFrame
        dataFrame.cache
        sender ! SLAVE_FINISH
      }
    }
  }

}
