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

import java.text.SimpleDateFormat
import java.util.{Date, UUID}

import org.apache.atlas.`type`.AtlasTypeUtil
import org.apache.atlas.model.instance.{AtlasEntity, AtlasObjectId => Id}
import org.apache.commons.configuration.Configuration
import za.co.absa.spline.model.DataLineage
import za.co.absa.spline.persistence.atlas.model.{EndpointDataset, HasReferredEntities, _}

/**
 * The object is responsible for conversion of a [[za.co.absa.spline.model.DataLineage Spline lineage model]] to Atlas entities.
 */
object DataLineageToTypeSystemConverter {
  val ATLAS_CLUSTER_NAME = "atlas.cluster.name"

  /**
   * The method converts a [[za.co.absa.spline.model.DataLineage Spline lineage model]] to Atlas entities.
   *
   * @param lineage An input Spline lineage model
   * @return Atlas entities
   */
  def convert(lineage: DataLineage, atlasProperties: Configuration): Seq[AtlasEntity] = {
    val dataTypes = DataTypeConverter.convert(lineage.dataTypes)
    val dataTypeIdMap = toIdMap(dataTypes)
    val dataTypeIdAndNameMap = dataTypes.map(i => i.qualifiedName -> (AtlasTypeUtil.getAtlasObjectId(i), i.name)).toMap
    val attributes = AttributeConverter.convert(lineage.attributes, dataTypeIdAndNameMap)
    val attributesIdMap = toIdMap(attributes)
    val clusterName = atlasProperties.getString(ATLAS_CLUSTER_NAME, "primary")
    val dataSets = DatasetConverter.convert(lineage, dataTypeIdMap, attributesIdMap, clusterName)

    val dataSetIdMap = toIdMap(dataSets)
    val splineAttributesMap = lineage.attributes.map(i => i.id.toString -> i).toMap
    val expressionConverter = new ExpressionConverter(splineAttributesMap, dataTypeIdMap)
    //    val (dbEntitySeq, tableEntitySeq, sdEntitySeq, columnEntitySeq) = HiveEntityConverter.convert(lineage)

    //    val hiveTableIdMap = tableEntitySeq.map(t=> t.asInstanceOf[HiveTable].uuid -> t.getId).toMap
    val operations = new OperationConverter(expressionConverter).convert(lineage, dataSetIdMap, attributesIdMap, dataTypeIdMap)
    val processes = createProcess(lineage, operations, dataSets)
    val ret = dataTypes ++ attributes ++ dataSets ++ operations ++ processes
    ret ++ ret.flatMap {
      case e: EndpointDataset =>
        val endpoint = e.asInstanceOf[EndpointDataset].endpoint
        endpoint.asInstanceOf[HasReferredEntities].getReferredEntities :+ endpoint
      case h: HasReferredEntities => h.getReferredEntities
      case _ => None
    }
  }

  private def toIdMap(collection: Seq[AtlasEntity with QualifiedEntity]): Map[String, Id] =
    collection.map(i => i.qualifiedName -> AtlasTypeUtil.getAtlasObjectId(i)).toMap

  /**
   * Spark unique lineage for process, Execution information for each job
   * @param lineage An input Spline lineage model
   * @param operations Spark job operations
   * @param datasets Spark job datasets
   * @return Process and job entities
   */
  private def createProcess(lineage: DataLineage, operations: Seq[Operation], datasets: Seq[AtlasEntity]): Seq[AtlasEntity] = {
    val (inputDatasets, outputDatasets) = datasets
      .filter(_.isInstanceOf[EndpointDataset])
      .partition(_.asInstanceOf[EndpointDataset].direction == EndpointDirection.input)

    val inputs = inputDatasets.map(_.asInstanceOf[EndpointDataset].endpoint)
    val outputs = outputDatasets.map(_.asInstanceOf[EndpointDataset].endpoint)
    val inputStr = inputs.map(entity => entity.getAttribute("qualifiedName")).toSet.mkString("+")
    val outputStr = outputs.map(entity => entity.getAttribute("qualifiedName")).mkString("+")
    val processName = s"$inputStr=>$outputStr"
    val process = new SparkProcess(
      processName,
      processName,
      null,
      null,
      inputDatasets.map(i => AtlasTypeUtil.getAtlasObjectId(i.asInstanceOf[EndpointDataset].endpoint)),
      outputDatasets.map(i => AtlasTypeUtil.getAtlasObjectId(i.asInstanceOf[EndpointDataset].endpoint))
    )

    val job = new Job(
      lineage.appId,
      lineage.appName + ":" + new SimpleDateFormat("yyyy-MM-dd'T'H:mm:ss").format(new Date(lineage.timestamp)),
      lineage.id,
      lineage.timestamp,
      lineage.durationMs,
      lineage.metrics,
      operations.map(AtlasTypeUtil.getAtlasObjectId),
      datasets.map(AtlasTypeUtil.getAtlasObjectId),
      inputDatasets.map(AtlasTypeUtil.getAtlasObjectId),
      outputDatasets.map(AtlasTypeUtil.getAtlasObjectId),
      Seq(),
      Seq(),
      AtlasTypeUtil.getAtlasObjectId(process)
    )
    Seq(process, job)
  }
}
