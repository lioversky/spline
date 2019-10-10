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

import org.apache.atlas.model.instance.AtlasObjectId
import za.co.absa.spline.model.{DataLineage, op}
import za.co.absa.spline.persistence.atlas.model._

/**
  * The class is responsible for extraction of [[za.co.absa.spline.persistence.atlas.model.Operation Atlas operations]] from a [[za.co.absa.spline.model.DataLineage Spline lineage instance]].
  */
class OperationConverter(expressionConverter: ExpressionConverter) {

  /**
    * The method extracts [[za.co.absa.spline.persistence.atlas.model.Operation Atlas operations]] from a [[za.co.absa.spline.model.DataLineage Spline lineage instance]].
    * @param lineage A lineage instance used for extraction of operations
    * @param dataSetIdMap A mapping from Spline data set ids to ids assigned by Atlas API.
    * @param attributeIdMap  A mapping from Spline attribute ids to ids assigned by Atlas API.
    * @param dataTypeIdMap  A mapping from Spline data type ids to ids assigned by Atlas API.
    * @return Atlas operations
    */
  def convert(lineage: DataLineage, dataSetIdMap : Map[String, AtlasObjectId], attributeIdMap: Map[String, AtlasObjectId], dataTypeIdMap: Map[String, AtlasObjectId]) : Seq[Operation] = {
    lineage.operations.map{ o =>
      val commonProperties = OperationCommonProperties(
        o.mainProps.name,
        o.mainProps.id.toString,
        o.mainProps.inputs.map(i => dataSetIdMap(i.toString)),
        Seq(dataSetIdMap(o.mainProps.output.toString))
      )
      o match {
        case op.Write(_, _, _, append, _, _) => new WriteOperation(commonProperties, append)
        case op.InsertIntoTable(_, dt, path, append, table) =>
          new InsertIntoTableOperation(commonProperties, append)
        case op.Sort(_, orders) =>
          val atlasOrders = orders.zipWithIndex.map{
            case (op.SortOrder(expression, direction, nullOrder), i) =>
              val qualifiedName = s"${commonProperties.qualifiedName}_order@$i"
              new SortOrder(
                qualifiedName,
                expressionConverter.convert(qualifiedName, expression),
                direction,
                nullOrder
              )
          }
          new SortOperation(commonProperties, atlasOrders)
        case op.Aggregate(_, groupings, aggregations) =>
          val atlasGroupings = groupings.map(e => expressionConverter.convert(commonProperties.qualifiedName + "_grouping", e))
          val atlasAggregations = aggregations.values.map(a => expressionConverter.convert(commonProperties.qualifiedName + "_aggregation", a)).toSeq
          new AggregateOperation(commonProperties, atlasGroupings, atlasAggregations)
        case op.Join(_, c, t) => new JoinOperation(commonProperties, t, c.map(j => expressionConverter.convert(commonProperties.qualifiedName, j)))
        case op.Filter(_, c) => new FilterOperation(commonProperties, expressionConverter.convert(commonProperties.qualifiedName, c))
        case op.Projection(_, t) => new ProjectOperation(commonProperties, t.zipWithIndex.map(j => expressionConverter.convert(commonProperties.qualifiedName + "@" + j._2, j._1)))
        case op.Alias(_, a) => new AliasOperation(commonProperties, a)
        case op.Generic(_, r) => new GenericOperation(commonProperties, r)
        case op.HiveRelation(_, _, _)=> new HiveRelationOperation(commonProperties)
        case _ => new Operation(commonProperties)
      }
    }
  }
}
