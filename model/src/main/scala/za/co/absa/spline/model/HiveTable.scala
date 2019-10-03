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

package za.co.absa.spline.model

import java.util.UUID

/**
 * Create by hongxun on 2019/9/19
 */
case class HiveTable(id: UUID, name: String, owner: String,
                     comment: String, tableType: String,
                     db: HiveDatabase, sd: HiveStorage, columns: Seq[HiveColumn])

case class HiveDatabase(id: UUID, name: String)

case class HiveColumn(id: UUID, name: String, dataType: String, owner: String)

case class HiveStorage(id: UUID, location: String, compressed: Boolean,
                       inputFormat: String, outputFormat: String,
                       db: String, table: String)