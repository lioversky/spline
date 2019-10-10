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

import org.apache.atlas.model.instance.{AtlasEntity, AtlasObjectId}

import scala.collection.JavaConverters._

/**
  * The object represents an enumeration of endpoint directions.
  */
trait DataType extends QualifiedEntity{

  /**
    * A name
    */
  val name : String

  /**
    * An unique identifier
    */
  val qualifiedName : String

  /**
    * A flag describing whether the type is nullable or not
    */
  val nullable: Boolean

  def resolveIds(splineToAtlasIdAndNameMapping: Map[String, (AtlasObjectId, String)]): Unit = {}
}

/**
  * The class represents atomic types such as boolean, integer, string, etc.
  * @param name A name
  * @param qualifiedName An unique identifier
  * @param nullable A flag describing whether the type is nullable or not
  */
class SimpleDataType(val name : String, val qualifiedName : String, val nullable: Boolean) extends AtlasEntity  (
  SparkDataTypes.SimpleDataType,
  new java.util.HashMap[String, Object]{
    put("name", name)
    put("qualifiedName", s"name=$name;nullable=${nullable}" )
    put("nullable", Boolean.box(nullable))
  }
) with DataType

/**
  * The class represents custom structured types.
  * @param fields A sequence of fields that the type is compound from
  * @param qualifiedName An unique identifier
  * @param nullable A flag describing whether the type is nullable or not
  */
class StructDataType(fields: Seq[StructField], val qualifiedName : String, val nullable: Boolean) extends AtlasEntity(
  SparkDataTypes.StructDataType,
  new java.util.HashMap[String, Object]{
    put("name", "struct")
    put("qualifiedName", s"fields=${fields.mkString(",")};nullable=${nullable}")
    put("nullable", Boolean.box(nullable))
    put("fields", fields.asJava)
  }
)with DataType
{
  val name = "struct"

  override def resolveIds(splineToAtlasIdAndNameMapping: Map[String, (AtlasObjectId, String)]): Unit = fields.foreach(_.resolveIds(splineToAtlasIdAndNameMapping))
}

/**
  * The class represents one sub-attribute (element) of a [[za.co.absa.spline.persistence.atlas.model.StructDataType StructDataType]]
  * @param name A name of the sub-attribute (element)
  * @param qualifiedName An unique identifier
  * @param dataType A data type of the sub-attribute (element)
  */
class StructField(name: String, qualifiedName: String, dataType: String) extends AtlasEntity(
  SparkDataTypes.StructField,
  new java.util.HashMap[String, Object]{
    put("name", name)
    put("qualifiedName", qualifiedName)
  }
){
  def resolveIds(splineToAtlasIdAndNameMapping: Map[String, (AtlasObjectId, String)]): Unit = {
    val (id, name) = splineToAtlasIdAndNameMapping(dataType)
    setAttribute("type", name)
    setAttribute("typeRef", id)
  }
}

/**
  * The class represents a spacial data type for arrays.
  * @param elementDataType A data type of any element from the array
  * @param qualifiedName An unique identifier
  * @param nullable A flag describing whether the type is nullable or not
  */
class ArrayDataType(elementDataType: String, val qualifiedName : String, val nullable: Boolean) extends AtlasEntity(
  SparkDataTypes.ArrayDataType,
  new java.util.HashMap[String, Object]{
    put("name", "array")
    put("qualifiedName", qualifiedName.toString)
    put("nullable", Boolean.box(nullable))
  }
) with DataType
{
  val name = "array"

  override def resolveIds(splineToAtlasIdAndNameMapping: Map[String, (AtlasObjectId, String)]): Unit = {
    val (id, _) = splineToAtlasIdAndNameMapping(elementDataType)
    setAttribute("elementType", id)
  }
}
