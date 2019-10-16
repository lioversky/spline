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

import org.apache.atlas.hook.AtlasHook
import org.apache.atlas.model.instance.AtlasEntity
import org.apache.atlas.model.notification.HookNotification
import org.apache.commons.configuration.Configuration
import org.apache.hadoop.security.UserGroupInformation
import org.slf4s.Logging
import za.co.absa.spline.model.DataLineage
import za.co.absa.spline.persistence.api.DataLineageWriter
import za.co.absa.spline.persistence.atlas.conversion.DataLineageToTypeSystemConverter
import za.co.absa.spline.persistence.atlas.model.SparkDataTypes._

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer
import scala.concurrent.{ExecutionContext, Future, blocking}


/**
 * The class represents Atlas persistence layer for the [[za.co.absa.spline.model.DataLineage DataLineage]] entity.
 */
class AtlasDataLineageWriter extends AtlasHook with DataLineageWriter with Logging {

  //  override def getNumberOfRetriesPropertyKey: String = "atlas.hook.spline.numRetries"

  def getConf(): Configuration = AtlasHook.atlasProperties

  /**
   * The method stores a particular data lineage to the persistence layer.
   *
   * @param lineage A data lineage that will be stored
   */


  override def store(lineage: DataLineage)(implicit ec: ExecutionContext): Future[Unit] = Future {

    val entityCollections = DataLineageToTypeSystemConverter.convert(lineage, getConf())
    blocking {
      log debug s"Sending lineage entries (${entityCollections.length})"

      val atlasUser = AtlasHook.getUser()
      val user = if (atlasUser == null || atlasUser.isEmpty) "Anonymous" else atlasUser
      //      this.notifyEntities(s"$user via Spline", entityCollections.asJava)
      //      val hookNotifications:List[HookNotification] = new java.util.ArrayList[HookNotification]
      //      hookNotifications.add(new HookNotificationV1.EntityCreateRequest(user, entityCollections.asJava))
      val l0: ListBuffer[AtlasEntity] = new ListBuffer()
      val l1: ListBuffer[AtlasEntity] = new ListBuffer()
      val l2: ListBuffer[AtlasEntity] = new ListBuffer()
      val l3: ListBuffer[AtlasEntity] = new ListBuffer()
      val l4: ListBuffer[AtlasEntity] = new ListBuffer()
      entityCollections.foreach(entity => {
        entity.getTypeName match {
          case Attribute
               | SimpleDataType
               | StructDataType
               | StructField
               | ArrayDataType
          => l0 += entity
          case Dataset
               | EndpointDataset
               | FileEndpoint
               | HiveDB
               | HiveTable
               | HiveColumn
               | HiveSD => l1 += entity

          case Expression
               | AliasExpression
               | BinaryExpression
               | AttributeReferenceExpression
               | UDFExpression => l2 += entity

          case Operation
               | GenericOperation
               | JoinOperation
               | FilterOperation
               | ProjectOperation
               | AliasOperation
               | SortOperation
               | SortOrder
               | AggregateOperation
               | WriteOperation
          => l3 += entity
          case Job | SparkProcess => l4 += entity
        }

      })

      val list: List[HookNotification] = List(
        new HookNotification.EntityCreateRequestV2(user, new AtlasEntity.AtlasEntitiesWithExtInfo(l0.asJava)),
        new HookNotification.EntityCreateRequestV2(user, new AtlasEntity.AtlasEntitiesWithExtInfo(l1.asJava)),
        new HookNotification.EntityCreateRequestV2(user, new AtlasEntity.AtlasEntitiesWithExtInfo(l2.asJava)),
        new HookNotification.EntityCreateRequestV2(user, new AtlasEntity.AtlasEntitiesWithExtInfo(l3.asJava)),
        new HookNotification.EntityCreateRequestV2(user, new AtlasEntity.AtlasEntitiesWithExtInfo(l4.asJava))
      )

      this.notifyEntities(list.asJava, UserGroupInformation.getCurrentUser)
    }
  }


}
