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

import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.command.CreateDataSourceTableAsSelectCommand
import org.apache.spark.sql.execution.datasources.jdbc.JdbcRelationProvider
import org.apache.spark.sql.execution.datasources.{InsertIntoHadoopFsRelationCommand, SaveIntoDataSourceCommand}
import org.apache.spark.sql.hive.execution.InsertIntoHiveTable
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.{SaveMode, SparkSession}
import za.co.absa.spline.common.extractors.{AccessorMethodValueExtractor, SafeTypeMatchingExtractor}
import za.co.absa.spline.harvester.builder.write.WriteCommandExtractor._
import za.co.absa.spline.harvester.builder.{CatalogTableUtils, SourceIdentifier}
import za.co.absa.spline.harvester.qualifier.PathQualifier

import scala.PartialFunction.condOpt
import scala.language.implicitConversions

class WriteCommandExtractor(pathQualifier: PathQualifier, session: SparkSession) {

  def asWriteCommand(operation: LogicalPlan): Option[WriteCommand] = condOpt(operation) {
    case cmd: SaveIntoDataSourceCommand
      if hasPropertySatisfyingPredicate[String](cmd)("provider", _ == "jdbc")
        || hasPropertySatisfyingPredicate[Any](cmd)("dataSource", _.isInstanceOf[JdbcRelationProvider]) =>
      val jdbcConnectionString = cmd.options("url")
      val tableName = cmd.options("dbtable")
      asJDBCWriteCommand(jdbcConnectionString, tableName, cmd.mode, cmd.query)

    case cmd: CreateDataSourceTableAsSelectCommand =>
      asTableWriteCommand(cmd.table, cmd.mode, cmd.query)

    case `_: InsertIntoHiveTable`(cmd) =>
      val mode = if (cmd.overwrite) SaveMode.Overwrite else SaveMode.Append
      asTableWriteCommand(cmd.table, mode, cmd.query)

    case cmd: InsertIntoHadoopFsRelationCommand =>
      val path = cmd.outputPath.toString
      val format = cmd.fileFormat.toString
      val qPath = pathQualifier.qualify(path)
      WriteCommand(SourceIdentifier(Some(format), Seq(qPath)), cmd.mode, cmd.query, cmd.options)

    case cmd: SaveIntoDataSourceCommand =>
      val maybeSourceType = DataSourceTypeExtractor.unapply(cmd)
      val maybeFormat = maybeSourceType.map {
        case dsr: DataSourceRegister => dsr.shortName
        case o => o.toString
      }
      val opts = cmd.options
      val uri = opts.get("path").map(pathQualifier.qualify)
        .orElse(opts.get("topic").filter(_ => opts.contains("kafka.bootstrap.servers")).map(topic => s"kafka:$topic"))
        .getOrElse(sys.error(s"Cannot extract source URI from the options: ${opts.keySet mkString ","}"))
      WriteCommand(SourceIdentifier(maybeFormat, Seq(uri)), cmd.mode, cmd.query, opts)
  }

  private def asJDBCWriteCommand(jdbcConnStr: String, tableName: String, mode: SaveMode, query: LogicalPlan) = {
    WriteCommand(SourceIdentifier(Some("jdbc"), Seq(s"$jdbcConnStr:$tableName")), mode, query)
  }

  private def asTableWriteCommand(table: CatalogTable, mode: SaveMode, query: LogicalPlan) = {
    val sourceIdentifier = CatalogTableUtils.toSourceIdentifier(table)(pathQualifier, session)
    WriteCommand(sourceIdentifier, mode, query, Map("table" -> table))
  }
}

object WriteCommandExtractor {

  private object `_: InsertIntoHiveTable` extends SafeTypeMatchingExtractor(classOf[InsertIntoHiveTable])

  private object DataSourceTypeExtractor extends AccessorMethodValueExtractor[AnyRef]("provider", "dataSource")

  private def hasPropertySatisfyingPredicate[A: Manifest](o: AnyRef)(key: String, predicate: A => Boolean): Boolean =
    AccessorMethodValueExtractor[A](key).apply(o).exists(predicate)
}