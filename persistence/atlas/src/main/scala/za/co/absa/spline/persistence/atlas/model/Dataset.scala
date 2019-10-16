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

package za.co.absa.spline.persistence.atlas.model

import za.co.absa.spline.persistence.atlas.util.AtlasUtil._
import org.apache.atlas.model.instance.{AtlasEntity, AtlasObjectId => Id}

import scala.collection.JavaConverters._

/**
  * The class represents a state of data within a Spark job
  * @param name A name
  * @param qualifiedName An unique identifier
  * @param attributes A sequence of attributes
  * @param datasetType An Atlas entity type name
  * @param childProperties Properties that are specific for derived classes
  */
class Dataset(
  val name : String,
  val qualifiedName: String,
  attributes: Seq[Id],
  datasetType: String = SparkDataTypes.Dataset,
  childProperties: Map[String, AnyRef] = Map.empty
) extends AtlasEntity(
  datasetType,
  new java.util.HashMap[String, Object]{
    put("name", name)
    put("qualifiedName", qualifiedName.toString)
    put("attributes", attributes.asJava)
    childProperties.foreach(i => put(i._1,i._2))
  }
) with QualifiedEntity


import za.co.absa.spline.persistence.atlas.model.EndpointDirection._
import za.co.absa.spline.persistence.atlas.model.EndpointType._

/**
  * The class represents an initial or final data set.
  * @param name A name
  * @param qualifiedName An unique identifier
  * @param attributes A sequence of attributes
  * @param endpoint An endpoint where the attribute comes from or where ends up
  * @param endpointType An endpoint type (file, topic, table, etc.)
  * @param direction A flag saying whether the endpoint is a source or a destination of the data set.
  * @param format A format in which date are represented within the endpoint (csv, xml, parquet, etc.)
  */
class EndpointDataset(
  name : String,
  qualifiedName: String,
  attributes: Seq[Id],
  val endpoint : AtlasEntity,
  endpointType : EndpointType,
  val direction : EndpointDirection,
  format : String
) extends Dataset(
  name,
  qualifiedName,
  attributes,
  SparkDataTypes.EndpointDataset,
  Map(
    "endpoint" -> getAtlasObjectId(endpoint),
    "endpointType" -> endpointType.toString,
    "direction" -> direction.toString,
    "format" -> format
  )
) with QualifiedEntity{

}
