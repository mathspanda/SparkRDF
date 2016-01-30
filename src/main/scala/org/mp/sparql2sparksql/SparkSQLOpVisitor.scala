package org.mp.sparql2sparksql

import org.apache.jena.graph.Triple
import org.apache.jena.sparql.algebra.OpVisitor
import org.apache.jena.sparql.algebra.op._

import scala.collection.JavaConversions._

/**
  * Created by MP on 16/1/30.
  */
class SparkSQLOpVisitor(tableName: String) extends OpVisitor {

  var selectClause: String = "select "
  var fromClause: String = s"from $tableName "

  var bgpStarted: Boolean = false
  var tripleList: List[Triple] = _

  override def visit(opTable: OpTable) {}

  override def visit(opNull: OpNull) {}

  override def visit(opProc: OpProcedure) {}

  override def visit(opTop: OpTopN) {}

  override def visit(opGroup: OpGroup) {}

  override def visit(opSlice: OpSlice) {}

  override def visit(opDistinct: OpDistinct) {}

  override def visit(opReduced: OpReduced) {}

  override def visit(opProject: OpProject) {
    val varsStr = opProject.getVars.toList.map(_.getName).mkString(", ")
    selectClause += varsStr

    if (bgpStarted) {
      bgpStarted = false;
    }
  }

  override def visit(opPropFunc: OpPropFunc) {}

  override def visit(opFilter: OpFilter) {}

  override def visit(opGraph: OpGraph) {}

  override def visit(opService: OpService) {}

  override def visit(opPath: OpPath) {}

  override def visit(opQuad: OpQuad) {}

  override def visit(opTriple: OpTriple) {}

  override def visit(quadBlock: OpQuadBlock) {}

  override def visit(quadPattern: OpQuadPattern) {}

  override def visit(opBGP: OpBGP) {
    bgpStarted = true
    tripleList = opBGP.getPattern.getList.toList
  }

  override def visit(dsNames: OpDatasetNames) {}

  override def visit(opLabel: OpLabel) {}

  override def visit(opAssign: OpAssign) {}

  override def visit(opExtend: OpExtend) {}

  override def visit(opJoin: OpJoin) {}

  override def visit(opLeftJoin: OpLeftJoin) {}

  override def visit(opUnion: OpUnion) {}

  override def visit(opDiff: OpDiff) {}

  override def visit(opMinus: OpMinus) {}

  override def visit(opCondition: OpConditional) {}

  override def visit(opSequence: OpSequence) {}

  override def visit(opDisjunction: OpDisjunction) {}

  override def visit(opExt: OpExt) {}

  override def visit(opList: OpList) {}

  override def visit(opOrder: OpOrder) {}

  def generateSQL = s"$selectClause $fromClause"
}

object SparkSQLOpVisitor {
  def apply(tableName: String) = new SparkSQLOpVisitor(tableName)
}
