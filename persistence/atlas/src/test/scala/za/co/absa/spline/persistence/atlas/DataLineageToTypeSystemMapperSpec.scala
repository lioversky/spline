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

package za.co.absa.spline.persistence.atlas

import java.util.UUID.randomUUID

import org.apache.atlas.ApplicationProperties
import org.apache.atlas.`type`.AtlasType
import org.scalatest.{FlatSpec, Matchers}
import za.co.absa.spline.model.{op, _}
import za.co.absa.spline.model.dt.Simple
import za.co.absa.spline.model.op._
import za.co.absa.spline.persistence.atlas.conversion.DataLineageToTypeSystemConverter

class DataLineageToTypeSystemMapperSpec extends FlatSpec with Matchers {

  "A simple lineage graph with several nodes" should "be serializable to JSON via Atlas API" in {
    // Arrange
    val types = Seq(Simple("StringType", nullable = true))
    val attributes = Seq(
      Attribute(randomUUID, "_1", types(0).id),
      Attribute(randomUUID, "_2", types(0).id),
      Attribute(randomUUID, "_3", types(0).id)
    )

    val schema = Schema(attributes.map(_.id))

    val datasets = Seq(
      MetaDataset(randomUUID, schema),
      MetaDataset(randomUUID, schema),
      MetaDataset(randomUUID, schema),
      MetaDataset(randomUUID, schema),
      MetaDataset(randomUUID, schema),
      MetaDataset(randomUUID, schema)
    )

    val columns = attributes.map(s => HiveColumn(randomUUID, s.name, "String", "user"))
    val sd = HiveStorage(randomUUID(), "/tmp/hive", false, "", "", "default", "test")
    val db = HiveDatabase(randomUUID(), "default")
    val hiveTable = HiveTable(randomUUID, "test", "", "comment", "MANAGED", db, sd, columns)

    val operations: Seq[Operation] = Seq(
      InsertIntoTable(OperationProps(randomUUID, "InsertIntoTable", Seq(datasets(1).id), datasets(0).id), "table", null, true, hiveTable),
      Generic(OperationProps(randomUUID, "Union", Seq(datasets(3).id, datasets(2).id), datasets(1).id), "generic1"),
      Generic(OperationProps(randomUUID, "Filter", Seq(datasets(5).id), datasets(3).id), "generic2"),
      Generic(OperationProps(randomUUID, "Filter", Seq(datasets(4).id), datasets(2).id), "generic4"),
      Generic(OperationProps(randomUUID, "LogicalRDD", Seq.empty, datasets(4).id), "generic3"),
      HiveRelation(OperationProps(randomUUID, "HiveRelation", Seq.empty, datasets(5).id), "HiveTable", hiveTable)
    )
    val lineage = DataLineage(
      randomUUID.toString,
      "TestApp",
      1L,
      "SparkVersion",
      Map.empty,
      operations,
      datasets,
      attributes,
      types
    )

    // Act
    val entities = DataLineageToTypeSystemConverter.convert(lineage, ApplicationProperties.get)
    val jsonObjects = entities.map(AtlasType.toJson)

    // Assert
    entities.length shouldEqual jsonObjects.length
    jsonObjects.length shouldEqual jsonObjects.count(o => o != null && o.length > 0)
  }
}
