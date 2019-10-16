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

package za.co.absa.spline.persistence.atlas.conversion

import za.co.absa.spline.persistence.atlas.util.AtlasUtil._
import org.apache.atlas.model.instance.{AtlasObjectId => Id}
import za.co.absa.spline.model.op.InsertIntoTable
import za.co.absa.spline.model.{DataLineage, op}
import za.co.absa.spline.persistence.atlas.model._

/**
 * The object is responsible for extraction of [[za.co.absa.spline.persistence.atlas.model.Dataset Atlas data sets]] from [[za.co.absa.spline.model.DataLineage Spline lineage]].
 */
object DatasetConverter {
  val datasetSuffix = "_Dataset"

  /**
   * The method extracts [[za.co.absa.spline.persistence.atlas.model.Dataset Atlas data sets]] from [[za.co.absa.spline.model.DataLineage Spline linage]].
   *
   * @param lineage        An input lineage
   * @param dataTypeIdMap  A mapping from Spline data type ids to ids assigned by Atlas API.
   * @param attributeIdMap A mapping from Spline attribute ids to ids assigned by Atlas API.
   * @return Extracted data sets
   */
  def convert(lineage: DataLineage, dataTypeIdMap: Map[String, Id], attributeIdMap: Map[String, Id], clusterName: String): Seq[Dataset] = {
    for {
      operation <- lineage.operations
      dataset <- lineage.datasets if dataset.id == operation.mainProps.output
    } yield {
      val name = operation.mainProps.name + datasetSuffix
      val qualifiedName = dataset.id
      val attributes = dataset.schema.attrs.map(u=>attributeIdMap(u.toString))
      operation match {
        case op.Read(_, st, paths) =>
          val path = paths.map(_.path) mkString ", "
          new EndpointDataset(name, qualifiedName.toString, attributes, new FileEndpoint(path, path), EndpointType.file, EndpointDirection.input, st)
        case op.HiveRelation(_, st, table) => {
          val tableEntity = getTableEntity(table, clusterName)
          new EndpointDataset(name, qualifiedName.toString, attributes, tableEntity, EndpointType.hive_table, EndpointDirection.input, st)
        }
        case op.Write(_, dt, path, _, _, _) =>
          new EndpointDataset(name, qualifiedName.toString, attributes, new FileEndpoint(path, path), EndpointType.file, EndpointDirection.output, dt)
        case InsertIntoTable(_, dt, path, _, table) =>
          dt match {
            case "table" =>
              val tableEntity = getTableEntity(table, clusterName)
              new EndpointDataset(name, qualifiedName.toString, attributes, tableEntity, EndpointType.hive_table, EndpointDirection.output, dt)
            case _ =>
              new EndpointDataset(name, qualifiedName.toString, attributes, new FileEndpoint(path, path), EndpointType.file, EndpointDirection.output, dt)
          }
        case _ => new Dataset(name, qualifiedName.toString, attributes)
      }
    }
  }

  def getTableEntity(table: za.co.absa.spline.model.HiveTable, clusterName: String): HiveTable = {
    val dbEntity = new HiveDatabase(table.db.name, clusterName)

    val tableEntity = new HiveTable(table.id.toString, table.name, table.db.name, table.owner, table.comment, table.tableType, clusterName, dbEntity)
    val columnEntities = table.columns.map(col =>
      new HiveColumn(col.name, col.dataType, table.owner, table.db.name, table.name, getAtlasObjectId(tableEntity), clusterName))
    val sdEntity = new HiveStorage(table.sd.location, table.sd.compressed,
      table.sd.inputFormat, table.sd.outputFormat, table.db.name, table.name, getAtlasObjectId(tableEntity), clusterName
    )
    tableEntity.setIds(sdEntity, columnEntities)
    tableEntity
  }
}
