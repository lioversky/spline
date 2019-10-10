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

import org.apache.atlas.AtlasClient
import org.apache.atlas.model.instance.{AtlasEntity, AtlasObjectId => Id}

import scala.collection.JavaConverters._

/**
 * Create by hongxun on 2019/9/24
 */
class SparkProcess(
               name: String,
               qualifiedName: String,
               currUser: String,
               details: String,
               inputs: Seq[Id],
               outputs: Seq[Id]
             ) extends AtlasEntity(
  SparkDataTypes.SparkProcess,
  new java.util.HashMap[String, Object] {
    put("name", name)
    put("qualifiedName", qualifiedName)
    put("details", details)
    put("inputs", inputs.asJava)
    put("outputs", outputs.asJava)
  }
)
