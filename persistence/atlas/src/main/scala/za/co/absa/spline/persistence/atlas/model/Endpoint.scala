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

import org.apache.atlas.`type`.AtlasTypeUtil
import org.apache.atlas.model.instance.{AtlasEntity, AtlasObjectId => Id}

import scala.collection.JavaConverters._

/**
 * The object represents an enumeration of endpoint directions.
 */
object EndpointDirection extends Enumeration {
  type EndpointDirection = Value
  val input, output = Value
}

/**
 * The object represents an enumeration of endpoint types.
 */
object EndpointType extends Enumeration {
  type EndpointType = Value
  val file, hive_table = Value
}


/**
 * The class represents a file endpoint.
 *
 * @param path A relative path to the file
 * @param uri  An absolute path to the file including cluster name, etc.
 */
class FileEndpoint(val path: String, uri: String) extends AtlasEntity(
  SparkDataTypes.FileEndpoint,
  new java.util.HashMap[String, Object] {
    put("name", path)
    put("qualifiedName", uri)
    put("path", path)
  }
) with HasReferredEntities {
  override def getReferredEntities: List[AtlasEntity] = Nil
}

class HiveDatabase(name: String, clusterName: String) extends AtlasEntity(
  "hive_db", new java.util.HashMap[String, Object] {
    put("name", name)
    put("clusterName", clusterName)
    put("qualifiedName", s"$name@$clusterName")
  }
)

class HiveTable(val uuid: String,
                name: String,
                db: String,
                owner: String,
                comment: String,
                tableType: String,
                clusterName: String,
                var database: HiveDatabase,
                var storage: HiveStorage = null,
                var columns: Seq[HiveColumn] = null
               ) extends AtlasEntity(
  "hive_table",
  new java.util.HashMap[String, Object] {
    put("name", name)
    put("qualifiedName", s"$db.$name@$clusterName")
    put("comment", comment)
    put("owner", owner)
    put("tableType", tableType)
    put("temporary", "false")
    put("db", AtlasTypeUtil.getAtlasObjectId(database))
  }
) with HasReferredEntities {


  def setIds(sd: HiveStorage, columns: Seq[HiveColumn]): Unit = {
    this.storage = sd
    this.columns = columns
    this.getAttributes.put("sd", AtlasTypeUtil.getAtlasObjectId(sd))
    this.getAttributes.put("columns", AtlasTypeUtil.getAtlasObjectIds(columns.map(_.asInstanceOf[AtlasEntity]).asJava))

  }

  override def getReferredEntities: List[AtlasEntity] = {
    List(database, storage) ++ columns
  }

}

class HiveColumn(name: String, dataType: String, owner: String, db: String, table: String, tableId: Id, clusterName: String) extends AtlasEntity(
  "hive_column", new java.util.HashMap[String, Object] {
    put("name", name)
    put("qualifiedName", s"$db.$table.$name@$clusterName")
    put("type", dataType)
    put("owner", owner)
    put("table", tableId)
  }
)

class HiveStorage(location: String, compressed: Boolean,
                  inputFormat: String, outputFormat: String,
                  db: String, table: String, tableId: Id,
                  clusterName: String) extends AtlasEntity(
  "hive_storagedesc", new java.util.HashMap[String, Object] {
    put("qualifiedName", s"$db.$table@${clusterName}_storage")
    put("location", location)
    put("compressed", compressed.toString)
    put("inputFormat", inputFormat)
    put("outputFormat", outputFormat)
    put("table", tableId)
  }
)

