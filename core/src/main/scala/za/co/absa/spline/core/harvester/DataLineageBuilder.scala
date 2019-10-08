/*
 * Copyright 2017 ABSA Group Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package za.co.absa.spline.core.harvester

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark
import org.apache.spark.SparkContext
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.catalyst.catalog.HiveTableRelation
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.columnar.InMemoryTableScanExec
import org.apache.spark.sql.execution.command.{DataWritingCommand, ExecutedCommandExec}
import org.apache.spark.sql.execution.datasources.{InsertIntoHadoopFsRelationCommand, LogicalRelation}
import org.apache.spark.sql.execution.{FileSourceScanExec, LeafExecNode, LocalTableScanExec, SparkPlan}
import org.apache.spark.sql.hive.execution._
import scalaz.Scalaz._
import za.co.absa.spline.core.harvester.DataLineageBuilder.{Metrics, _}
import za.co.absa.spline.coresparkadapterapi._
import za.co.absa.spline.model._
import org.apache.spark.sql.hive.execution.HiveTableScanExecType

class DataLineageBuilder(logicalPlan: LogicalPlan, executedPlanOpt: Option[SparkPlan], sparkContext: SparkContext)
                        (hadoopConfiguration: Configuration, writeCommandParserFactory: WriteCommandParserFactory) {

  private val componentCreatorFactory: ComponentCreatorFactory = new ComponentCreatorFactory

  private val writeCommandParser = writeCommandParserFactory.writeParser()
  private val clusterUrl: Option[String] = sparkContext.getConf.getOption("spark.master")
  private val tableCommandParser = writeCommandParserFactory.saveAsTableParser(clusterUrl)
  private val jdbcCommandParser = writeCommandParserFactory.jdbcParser()

  def buildLineage(durationNs: Long): Option[DataLineage] = {
    val builders = getOperations(logicalPlan)
    val someRootBuilder = builders.lastOption

    val writeIgnored = someRootBuilder match {
      case Some(rootNode: RootNode) => rootNode.ignoreLineageWrite
      case _ => false
    }

    val operations = builders.map(_.build())

    writeIgnored match {
      case true => None
      case false =>
        val (readMetrics: Map[String,Metrics], writeMetrics: Metrics) = getMetrics()
        val readMap = readMetrics.map{ case (s, metric) => metric.map { case (k, v) => (s"$s.$k", v) } }
        val emptyMap :Metrics = Map.empty
        val read = readMap.foldLeft(emptyMap)((result, metrics) => result |+| metrics)
        //        val read = readMetrics.flatMap { case (s, metric) => metric.map { case (k, v) => (s"$s.$k", v) } }
        val write = writeMetrics.map { case (k, v) => (s"write.$k", v) }
        val duration = Map("durationMs" -> durationNs / 1000000)
        Some(
          DataLineage(
            sparkContext.applicationId,
            sparkContext.appName,
            System.currentTimeMillis(),
            spark.SPARK_VERSION,
            read ++ write ++ duration,
            operations.reverse,
            componentCreatorFactory.metaDatasetConverter.values.reverse,
            componentCreatorFactory.attributeConverter.values,
            componentCreatorFactory.dataTypeConverter.values
          )
        )
    }
  }

  private def getOperations(rootOp: LogicalPlan): Seq[OperationNodeBuilder] = {
    def traverseAndCollect(
                            accBuilders: Seq[OperationNodeBuilder],
                            processedEntries: Map[LogicalPlan, OperationNodeBuilder],
                            enqueuedEntries: Seq[(LogicalPlan, OperationNodeBuilder)]
                          ): Seq[OperationNodeBuilder] = {
      enqueuedEntries match {
        case Nil => accBuilders
        case (curOpNode, parentBuilder) +: restEnqueuedEntries =>
          val maybeExistingBuilder = processedEntries.get(curOpNode)
          val curBuilder = maybeExistingBuilder.getOrElse(createOperationBuilder(curOpNode))

          if (parentBuilder != null) parentBuilder += curBuilder

          if (maybeExistingBuilder.isEmpty) {

            val parsers = Array(jdbcCommandParser, writeCommandParser, tableCommandParser)

            val maybePlan: Option[LogicalPlan] = parsers.
              map(_.asWriteCommandIfPossible(curOpNode)).
              collectFirst {
                case Some(wc) => wc.query
              }

            val newNodesToProcess: Seq[LogicalPlan] =
              maybePlan match {
                case Some(q) => Seq(q)
                case None => curOpNode.children
              }

            traverseAndCollect(
              curBuilder +: accBuilders,
              processedEntries + (curOpNode -> curBuilder),
              newNodesToProcess.map(_ -> curBuilder) ++ restEnqueuedEntries)

          } else {
            traverseAndCollect(accBuilders, processedEntries, restEnqueuedEntries)
          }
      }
    }

    traverseAndCollect(Nil, Map.empty, Seq((rootOp, null)))
  }

  private def createOperationBuilder(op: LogicalPlan): OperationNodeBuilder = {
    implicit val ccf: ComponentCreatorFactory = componentCreatorFactory
    op match {
      case j: Join => new JoinNodeBuilder(j)
      case u: Union => new UnionNodeBuilder(u)
      case p: Project => new ProjectionNodeBuilder(p)
      case f: Filter => new FilterNodeBuilder(f)
      case s: Sort => new SortNodeBuilder(s)
      case s: Aggregate => new AggregateNodeBuilder(s)
      case a: SubqueryAlias => new AliasNodeBuilder(a)
      case hr: HiveTableRelation => new HiveRelationNodeBuilder(hr) with HDFSAwareBuilder
      case lr: LogicalRelation => new ReadNodeBuilder(lr) with HDFSAwareBuilder
      case _: InsertIntoHadoopFsRelationCommand | _: InsertIntoHiveTable =>
        //        val (readMetrics: Metrics, writeMetrics: Metrics) = getMetrics()
        new InsertIntoTableNodeBuilder(op.asInstanceOf[DataWritingCommand]) with HDFSAwareBuilder

      case wc if jdbcCommandParser.matches(op) =>
        //        val (readMetrics: Metrics, writeMetrics: Metrics) = getMetrics()
        val tableCmd = jdbcCommandParser.asWriteCommand(wc).asInstanceOf[SaveJDBCCommand]
        new SaveJDBCCommandNodeBuilder(tableCmd, null, null)
      case wc if writeCommandParser.matches(op) =>
        //        val (readMetrics: Metrics, writeMetrics: Metrics) = getMetrics()
        val writeCmd = writeCommandParser.asWriteCommand(wc).asInstanceOf[WriteCommand]
        new WriteNodeBuilder(writeCmd, null, null) with HDFSAwareBuilder
      case wc if tableCommandParser.matches(op) =>
        //        val (readMetrics: Metrics, writeMetrics: Metrics) = getMetrics()
        val tableCmd = tableCommandParser.asWriteCommand(wc).asInstanceOf[SaveAsTableCommand]
        new SaveAsTableNodeBuilder(tableCmd, null, null)
      case x => new GenericNodeBuilder(x)
    }
  }

  private def getMetrics(): (Map[String,Metrics], Metrics) = {
    executedPlanOpt.
      map(getExecutedReadWriteMetrics).
      getOrElse((Map.empty, Map.empty))
  }

  trait HDFSAwareBuilder extends FSAwareBuilder {
    override protected def getQualifiedPath(path: String): String = {
      val fsPath = new Path(path)
      val fs = FileSystem.get(hadoopConfiguration)
      val absolutePath = fsPath.makeQualified(fs.getUri, fs.getWorkingDirectory)
      absolutePath.toString
    }
  }

}

object DataLineageBuilder {
  private type Metrics = Map[String, Long]

  private def getExecutedReadWriteMetrics(executedPlan: SparkPlan): (Map[String,Metrics], Metrics) = {
    def getNodeMetrics(node: SparkPlan): Metrics = node.metrics.mapValues(_.value)

    val cumulatedReadMetrics: Map[String,Metrics] = {
      def traverseAndCollect(acc: Map[String,Metrics], nodes: Seq[SparkPlan]): Map[String,Metrics] = {
        nodes match {
          case Nil => acc
          case (leaf: LeafExecNode) +: queue =>
            val key = leaf match {
              case f: FileSourceScanExec => {
                f.tableIdentifier match {
                  case Some(x) => x.toString()
                  case None => f.relation.location.rootPaths.mkString(",")
                }
              }
              case h: HiveTableScanExecType => h.relation.tableMeta.identifier.toString()
              case e:ExecutedCommandExec => e.cmd.getClass.getName
              case i: InMemoryTableScanExec => "InMemory." + i.relation.tableName.getOrElse("")
              case _ => "read"
            }
            traverseAndCollect(acc ++ Map(key ->getNodeMetrics(leaf)), queue)
          case (node: SparkPlan) +: queue =>
            traverseAndCollect(acc, node.children ++ queue)
        }
      }

      traverseAndCollect(Map.empty, Seq(executedPlan))
    }

    (cumulatedReadMetrics, getNodeMetrics(executedPlan))
  }
}
