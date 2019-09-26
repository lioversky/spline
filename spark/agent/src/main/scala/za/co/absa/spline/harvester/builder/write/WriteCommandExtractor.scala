/*
 * Copyright 2019 ABSA Group Limited
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

package za.co.absa.spline.harvester.builder.write

import org.apache.spark.sql.SaveMode._
import org.apache.spark.sql.catalyst.catalog.{CatalogStorageFormat, CatalogTable}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.command.{CreateDataSourceTableAsSelectCommand, CreateTableCommand, DropTableCommand}
import org.apache.spark.sql.execution.datasources.jdbc.JdbcRelationProvider
import org.apache.spark.sql.execution.datasources.{InsertIntoHadoopFsRelationCommand, SaveIntoDataSourceCommand}
import org.apache.spark.sql.hive.execution.{CreateHiveTableAsSelectCommand, InsertIntoHiveTable}
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.{SaveMode, SparkSession}
import za.co.absa.spline.common.extractors.{AccessorMethodValueExtractor, SafeTypeMatchingExtractor}
import za.co.absa.spline.harvester.builder.CatalogTableUtils._
import za.co.absa.spline.harvester.builder.SourceIdentifier
import za.co.absa.spline.harvester.builder.write.WriteCommandExtractor.{hasPropertySatisfyingPredicate, _}
import za.co.absa.spline.harvester.qualifier.PathQualifier

import scala.PartialFunction.condOpt
import scala.language.reflectiveCalls

class WriteCommandExtractor(pathQualifier: PathQualifier, session: SparkSession) {

  def asWriteCommand(operation: LogicalPlan): Option[WriteCommand] = condOpt(operation) {
    case cmd: SaveIntoDataSourceCommand =>
      if (hasPropertySatisfyingPredicate[String](cmd)("provider", _ == "jdbc")
        || hasPropertySatisfyingPredicate[Any](cmd)("dataSource", _.isInstanceOf[JdbcRelationProvider])) {
        val jdbcConnectionString = cmd.options("url")
        val tableName = cmd.options("dbtable")
        WriteCommand(cmd.nodeName, SourceIdentifier(Some("jdbc"), Seq(s"$jdbcConnectionString:$tableName")), cmd.mode, cmd.query)
      }
      else {
        val maybeSourceType = DataSourceTypeExtractor.unapply(cmd)
        val maybeFormat = maybeSourceType.map {
          case dsr: DataSourceRegister => dsr.shortName
          case o => o.toString
        }
        val opts = cmd.options
        val uri = opts.get("path").map(pathQualifier.qualify)
          .orElse(opts.get("topic").filter(_ => opts.contains("kafka.bootstrap.servers")).map(topic => s"kafka:$topic"))
          .getOrElse(sys.error(s"Cannot extract source URI from the options: ${opts.keySet mkString ","}"))
        WriteCommand(cmd.nodeName, SourceIdentifier(maybeFormat, Seq(uri)), cmd.mode, cmd.query, opts)
      }

    case cmd: InsertIntoHadoopFsRelationCommand =>
      val path = cmd.outputPath.toString
      val format = cmd.fileFormat.toString
      val qPath = pathQualifier.qualify(path)
      WriteCommand(cmd.nodeName, SourceIdentifier(Some(format), Seq(qPath)), cmd.mode, cmd.query, cmd.options)

    case `_: InsertIntoDataSourceDirCommand`(cmd) =>
      val uri = cmd.storage.locationUri.getOrElse(sys.error(s"Cannot determine the dats source location: ${cmd.storage}"))
      val mode = if (cmd.overwrite) Overwrite else Append
      WriteCommand(cmd.nodeName, SourceIdentifier(Some(cmd.provider), Seq(uri.toString)), mode, cmd.query)

    case `_: InsertIntoHiveDirCommand`(cmd) =>
      val uri = cmd.storage.locationUri.getOrElse(sys.error(s"Cannot determine the dats source location: ${cmd.storage}"))
      val mode = if (cmd.overwrite) Overwrite else Append
      WriteCommand(cmd.nodeName, SourceIdentifier(Some("hive"), Seq(uri.toString)), mode, cmd.query)

    case `_: InsertIntoHiveTable`(cmd) =>
      val mode = if (cmd.overwrite) Overwrite else Append
      asTableWriteCommand(cmd.nodeName, cmd.table, mode, cmd.query)

    case `_: CreateHiveTableAsSelectCommand`(cmd) =>
      val sourceId = toSourceIdentifier(cmd.tableDesc)(pathQualifier, session)
      WriteCommand(cmd.nodeName, sourceId, Overwrite, cmd.query)

    case cmd: CreateDataSourceTableAsSelectCommand =>
      asTableWriteCommand(cmd.nodeName, cmd.table, cmd.mode, cmd.query)

    case dtc: DropTableCommand =>
      val uri = createTablePathURI(dtc.tableName)(session)
      val sourceId = SourceIdentifier(None, Seq(pathQualifier.qualify(uri)))
      WriteCommand(dtc.nodeName, sourceId, Overwrite, dtc)

    case ctc: CreateTableCommand =>
      val sourceId = toSourceIdentifier(ctc.table)(pathQualifier, session)
      WriteCommand(ctc.nodeName, sourceId, Overwrite, ctc)
  }

  private def asTableWriteCommand(name: String, table: CatalogTable, mode: SaveMode, query: LogicalPlan) = {
    val sourceIdentifier = toSourceIdentifier(table)(pathQualifier, session)
    WriteCommand(name, sourceIdentifier, mode, query, Map("table" -> table))
  }
}

object WriteCommandExtractor {

  private object `_: InsertIntoHiveTable` extends SafeTypeMatchingExtractor(classOf[InsertIntoHiveTable])

  private object `_: CreateHiveTableAsSelectCommand` extends SafeTypeMatchingExtractor(classOf[CreateHiveTableAsSelectCommand])

  private object `_: InsertIntoHiveDirCommand` extends SafeTypeMatchingExtractor[ {
    def storage: CatalogStorageFormat
    def query: LogicalPlan
    def overwrite: Boolean
    def nodeName: String
  }]("org.apache.spark.sql.hive.execution.InsertIntoHiveDirCommand")

  private object `_: InsertIntoDataSourceDirCommand` extends SafeTypeMatchingExtractor[ {
    def storage: CatalogStorageFormat
    def provider: String
    def query: LogicalPlan
    def overwrite: Boolean
    def nodeName: String
  }]("org.apache.spark.sql.execution.command.InsertIntoDataSourceDirCommand")

  private object DataSourceTypeExtractor extends AccessorMethodValueExtractor[AnyRef]("provider", "dataSource")

  private def hasPropertySatisfyingPredicate[A: Manifest](o: AnyRef)(key: String, predicate: A => Boolean): Boolean =
    AccessorMethodValueExtractor[A](key).apply(o).exists(predicate)
}