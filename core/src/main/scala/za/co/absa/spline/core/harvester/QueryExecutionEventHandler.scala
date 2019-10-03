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

import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.execution.command._
import org.apache.spark.sql.execution.datasources.SaveIntoDataSourceCommand
import org.slf4s.Logging
import za.co.absa.spline.core.SparkLineageProcessor

import scala.language.postfixOps

class QueryExecutionEventHandler(harvesterFactory: DataLineageBuilderFactory, lineageProcessor: SparkLineageProcessor)
  extends Logging {

  /**
   * The method is executed when an action execution is successful.
   *
   * @param funcName   A name of the executed action.
   * @param qe         A Spark object holding lineage information (logical, optimized, physical plan)
   * @param durationNs Duration of the action execution [nanoseconds]
   */
  def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long): Unit = {
    log debug s"Action '$funcName' execution succeeded"

    val maybeLineage = if (funcName == "save" || funcName == "saveAsTable" || funcName == "insertInto") {
      log debug s"Start tracking lineage for action '$funcName'"


      log debug s"Lineage tracking for action '$funcName' is done."
      harvesterFactory.
        createBuilder(qe.analyzed, Some(qe.executedPlan), qe.sparkSession.sparkContext).
        buildLineage(durationNs)
    }
    else if (funcName == "command") {
      qe.sparkPlan match {
        case r: ExecutedCommandExec => {
          r.cmd match {
            case c: LoadDataCommand => log.info("LoadDataCommand")
            case c: CreateViewCommand => log.info("CreateViewCommand")
            case c: SaveIntoDataSourceCommand => log.info("SaveIntoDataSourceCommand")
            case c: CreateTableCommand => log.info("CreateTableCommand")

            case c: CreateDataSourceTableCommand => log.info("CreateDataSourceTableCommand")
            case ct:CacheTableCommand => log.info("CacheTableCommand")
            case _ => log.info("_")
          }
          None
        }
        case r: DataWritingCommandExec => {
          harvesterFactory.
            createBuilder(qe.analyzed, Some(qe.executedPlan), qe.sparkSession.sparkContext).
            buildLineage(durationNs)
        }

        case _ => {
          None
        }

      }
    }
    else {
      log debug s"Skipping lineage tracking for action '$funcName'"
      None
    }
    maybeLineage match {
      case None => log debug s"The write result was ignored. Skipping lineage."
      case Some(lineage) => lineageProcessor.process(lineage)
    }

  }

  /**
   * The method is executed when an error occurs during an action execution.
   *
   * @param funcName  A name of the executed action.
   * @param qe        A Spark object holding lineage information (logical, optimized, physical plan)
   * @param exception An exception describing the reason of the error
   */
  def onFailure(funcName: String, qe: QueryExecution, exception: Exception): Unit = {
    log.error(s"Action '$funcName' execution failed", exception)
  }
}
