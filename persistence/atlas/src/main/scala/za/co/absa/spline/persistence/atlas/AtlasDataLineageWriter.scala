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

import com.izettle.metrics.influxdb.data.InfluxDbPoint
import org.apache.atlas.hook.AtlasHook
import org.apache.atlas.model.instance.AtlasEntity
import org.apache.atlas.model.notification.HookNotification
import org.apache.commons.configuration.Configuration
import org.apache.hadoop.security.UserGroupInformation
import org.slf4s.Logging
import za.co.absa.spline.model.DataLineage
import za.co.absa.spline.persistence.api.DataLineageWriter
import za.co.absa.spline.persistence.atlas.conversion.DataLineageToTypeSystemConverter
import za.co.absa.spline.persistence.atlas.model.{EndpointDataset, Expression, HasReferredEntities, Operation, SparkDataTypes, SparkProcess}

import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContext, Future, blocking}
import com.izettle.metrics.influxdb.{Configuration => InfluxdbConf, _}
/**
 * The class represents Atlas persistence layer for the [[za.co.absa.spline.model.DataLineage DataLineage]] entity.
 */
class AtlasDataLineageWriter extends AtlasHook with DataLineageWriter with Logging {

  val PERSIST_ALL = "atlas.notification.persist.all"
  val PROCESS_ONLY = "atlas.notification.persist.processOnly"
  //  override def getNumberOfRetriesPropertyKey: String = "atlas.hook.spline.numRetries"

  def getConf(): Configuration = AtlasHook.atlasProperties

  /**
   * The method stores a particular data lineage to the persistence layer.
   *
   * @param lineage A data lineage that will be stored
   */


  override def store(lineage: DataLineage)(implicit ec: ExecutionContext): Future[Unit] = Future {
    val conf = getConf()
    val persistAll = conf.getBoolean(PERSIST_ALL, true)
    val processOnly = conf.getBoolean(PROCESS_ONLY, false)
    val entityCollections = DataLineageToTypeSystemConverter.convert(lineage, getConf())
    blocking {
      log info s"Sending lineage entries (${entityCollections.length})"

      val atlasUser = AtlasHook.getUser()
      val user = if (atlasUser == null || atlasUser.isEmpty) "Anonymous" else atlasUser
      //      this.notifyEntities(s"$user via Spline", entityCollections.asJava)
      //      val hookNotifications:List[HookNotification] = new java.util.ArrayList[HookNotification]
      //      hookNotifications.add(new HookNotificationV1.EntityCreateRequest(user, entityCollections.asJava))

      val entitiesWithExtInfo = if (persistAll) {
        new AtlasEntity.AtlasEntitiesWithExtInfo(entityCollections.asJava)
      } else {
        if (!processOnly) {
          val reduceEntities = entityCollections.flatMap {
            case _: Expression => None
            case o: Operation =>
              o.clearExpression()
              Some(o)
            case e => Some(e)
          }
          new AtlasEntity.AtlasEntitiesWithExtInfo(reduceEntities.asJava)
        } else {
          val processEntities = entityCollections.flatMap {
            case p: SparkProcess => Some(p)
            case e: EndpointDataset =>
              val endpoint = e.endpoint
              endpoint.asInstanceOf[HasReferredEntities].getReferredEntities :+ endpoint
            case _ => None
          }
          val process = processEntities.filter(_.isInstanceOf[SparkProcess]).head.asInstanceOf[SparkProcess]
          sendMetrics(lineage,process.name)
          new AtlasEntity.AtlasEntitiesWithExtInfo(processEntities.asJava)
        }
      }
      this.notifyEntities(Collections.singletonList(new HookNotification.EntityCreateRequestV2(user, entitiesWithExtInfo)),
        UserGroupInformation.getCurrentUser)
    }
  }

  def sendMetrics(lineage: DataLineage,processName:String): Unit ={
    val host = getConf().getString("atlas.notification.influxdb.host")
    val port = getConf().getInt("atlas.notification.influxdb.port",8086)
    val database = getConf().getString("atlas.notification.influxdb.database","process-quality")

    val conf = new InfluxdbConf("http",host,port,database)
    val tagMap:Map[String,String] = Map("appId"->lineage.appId,"appName"->lineage.appName,"process"->processName)
    val fieldMap:java.util.Map[String,Object] =
      (Map("durationMs"->lineage.durationMs) ++ lineage.writeMetrics ++ lineage.readMetrics)
      .map(t=> (t._1,t._2.asInstanceOf[Object])).asJava

    val sender = new InfluxDbHttpSender(conf)
    sender.appendPoints(new InfluxDbPoint(SparkDataTypes.Job, tagMap.asJava, lineage.timestamp, fieldMap) )
    sender.writeData()
  }


}
