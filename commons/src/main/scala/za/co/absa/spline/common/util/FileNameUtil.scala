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

package za.co.absa.spline.common.util

/**
 * file path util.
 */
object FileNameUtil {

  // (yyyy/MM?  yyyyMM/dd? yyyyMMdd/HH? yyyy-MM/dd? yyyy-MM-dd/HH? yyyy_MM_dd/HH?)
  private val timeFormat = "^.*(/\\d{4,8}|/\\d{4}-\\d{2}|/\\d{4}-\\d{2}-\\d{2}|/\\d{4}_\\d{2}_\\d{2})(/\\d{1,4})?$".r

  //({}=(yyyyMMddHH yyyy-MM-dd yyyy_MM_dd)) {}=HH?
  private val partitionFormat = "^.*(/\\w+=\\d{4,8}|/\\w+=\\d{4}-\\d{2}|/\\w+=\\d{4}-\\d{2}-\\d{2}|/\\w+=\\d{4}_\\d{2}_\\d{2})(/\\w+=\\d{1,4})?$".r

  def findUniqueParent(paths: Seq[String]): Option[String] = {
    if (paths == null || paths.length == 0) None
    else if (paths.length == 1) Some(paths(0))
    else {
      val pathArray = paths.map(path => path.split("/"))
      val minLength = pathArray.map(arr => arr.length).min
      var minIndex = -1;
      var findFlag = false
      for (i <- (0 until minLength).reverse if !findFlag) {
        val v = pathArray(0)(i)
        if (pathArray.forall(arr => v.equals(arr(i)))) {
          minIndex = i + 1
          findFlag = true
        }
      }
      if (minIndex > 0)
        Some(pathArray(0).slice(0, minIndex).mkString("/"))
      else None
    }

  }


  def reducePathWithoutTime(path: String): String = {
    val (y, h) = path match {
      case timeFormat(y, h) => (y, h)
      case partitionFormat(y, h) => (y, h)
      case _ => (null, null)
    }
    val result = if (y != null)
      path.replace(y, "")
    else path
    if (h != null)
      result.replace(h, "")
    else result
  }

  def main(args: Array[String]): Unit = {
    Seq(
      "/text/12341210",
      "/text/12341210/33",
      "/text/1234-12",
      "/text/1234-12/33",
      "/text/1234-12-10",
      "/text/1234-12-10/33",
      "/text/1234_12_10",
      "/text/1234_12_10/33",
      "/text/fday=1234",
      "/text/fday=1234/f=33",
      "/text/fday=1234-12-10",
      "/text/fday=1234-12-10/f=33",
      "/text/fday=1234_12_10",
      "/text/fday=1234_12_10/f=33",
      "/t/t/t"
    ).foreach(p => println(reducePathWithoutTime(p)))
  }

}
