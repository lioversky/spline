/*
 * Copyright 2017 ABSA Africa Group Limited
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

import org.apache.atlas.`type`.AtlasTypeUtil
import org.apache.atlas.model.instance.{AtlasEntity, AtlasObjectId => Id}
import za.co.absa.spline.persistence.atlas.{model => atlasModel}
import za.co.absa.spline.{model => splineModel}
/**
 * The object is responsible for conversion of [[za.co.absa.spline.model.Attribute Spline attributes]] to [[za.co.absa.spline.persistence.atlas.model.Attribute Atlas attributes]].
 */
object AttributeConverter {

  /**
   * The method converts [[za.co.absa.spline.model.Attribute Spline attributes]] to unique [[za.co.absa.spline.persistence.atlas.model.Attribute Atlas attributes]].
   *
   * @param splineAttributes    Spline attributes that will be converted
   * @param dataTypeIdAnNameMap A mapping from Spline data type ids to ids assigned by Atlas API and attribute names.
   * @return Atlas attributes and origin uuid with attribute mapping
   */
  def convert(splineAttributes: Seq[splineModel.Attribute], dataTypeIdAnNameMap: Map[String, (Id, String)]): (Seq[atlasModel.Attribute], Map[String, Id]) = {
    //将attributes按照属性名与属性类型保留唯一值，并获取原uuid与唯一属性对应关系，foldLeft入参：map的key为确定唯一属性的属性名称和属性类型，
    val (attrs, idMap) = splineAttributes.foldLeft(Map[(String, String), atlasModel.Attribute](), Map[String, Id]())((b, attr) => {
      //属性类型
      val datatype = dataTypeIdAnNameMap(attr.dataTypeId.toString)._2
      //属性名称
      val name = attr.name
      // 如果唯一属性已存在，增加原uuid与已存在属性的映射，否则在唯一列表中增加此属性，并增加映射关系
      val (reAttrs, reMap) = b._1.get((name, datatype)) match {
        case Some(a) =>
          (b._1, b._2 + (attr.id.toString -> AtlasTypeUtil.getAtlasObjectId(a.asInstanceOf[AtlasEntity])))
        case None =>
          val attribute = new atlasModel.Attribute(name, attr.id.toString, dataTypeIdAnNameMap(attr.dataTypeId.toString))
          (b._1 + ((name, datatype) -> attribute), b._2 + (attr.id.toString -> AtlasTypeUtil.getAtlasObjectId(attribute)))
      }
      (reAttrs, reMap)
    })

    (attrs.values.toSeq, idMap)
  }
}
