package za.co.absa.spline.persistence.atlas.util

import java.util.Collections

import org.apache.atlas.model.instance.{AtlasEntity, AtlasObjectId}
import scala.collection.JavaConverters._

/**
 * Create by hongxun on 2019/10/16
 */
object AtlasUtil {
  def getAtlasObjectId(entity: AtlasEntity): AtlasObjectId = {
    val qualifiedName = entity.getAttribute("qualifiedName").asInstanceOf[String]
//    val attrMap :java.util.Map[java.lang.String,java.lang.Object] = Map("qualifiedName"-> qualifiedName.asInstanceOf[Object]).asJava
    val ret = new AtlasObjectId(entity.getTypeName,"qualifiedName", qualifiedName)

    ret
  }

  def getAtlasObjectIds(entities: java.util.List[AtlasEntity]): java.util.List[AtlasObjectId] = entities.asScala.map(getAtlasObjectId(_)).asJava


}
