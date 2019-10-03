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

import java.util.Collections

import org.apache.atlas.hook.AtlasHook
import org.apache.atlas.model.instance.AtlasEntity
import org.apache.atlas.model.notification.HookNotification
import org.apache.commons.configuration.Configuration
import org.apache.hadoop.security.UserGroupInformation
import org.slf4s.Logging
import za.co.absa.spline.model.DataLineage
import za.co.absa.spline.persistence.api.DataLineageWriter
import za.co.absa.spline.persistence.atlas.conversion.DataLineageToTypeSystemConverter

import scala.collection.JavaConverters._
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

    val entityCollections = DataLineageToTypeSystemConverter.convert(lineage,getConf())
    blocking {
      log debug s"Sending lineage entries (${entityCollections.length})"

      val atlasUser = AtlasHook.getUser()
      val user = if(atlasUser == null || atlasUser.isEmpty) "Anonymous" else atlasUser
//      this.notifyEntities(s"$user via Spline", entityCollections.asJava)
//      val hookNotifications:List[HookNotification] = new java.util.ArrayList[HookNotification]
//      hookNotifications.add(new HookNotificationV1.EntityCreateRequest(user, entityCollections.asJava))
      val entitiesWithExtInfo = new AtlasEntity.AtlasEntitiesWithExtInfo(entityCollections.asJava)
      this.notifyEntities(Collections.singletonList(new HookNotification.EntityCreateRequestV2(user, entitiesWithExtInfo)),
        UserGroupInformation.getCurrentUser)
    }
  }


}
