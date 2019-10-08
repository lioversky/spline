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

/**
 * The case class represents a hive table metadata descriptor
 * @param id An unique identifier
 * @param name table name
 * @param owner table owner
 * @param comment table comment
 * @param tableType table type
 * @param db belong to database
 * @param sd storage descriptor
 * @param columns table columns
 */
case class HiveTable(id: UUID, name: String, owner: String,
                     comment: String, tableType: String,
                     db: HiveDatabase, sd: HiveStorage, columns: Seq[HiveColumn])

/**
 * The case class represents Database descriptor
 * @param id An unique identifier
 * @param name Database name
 */
case class HiveDatabase(id: UUID, name: String)

/**
 *
 * @param id An unique identifier
 * @param name column name
 * @param dataType column data type
 * @param owner table owner
 */
case class HiveColumn(id: UUID, name: String, dataType: String, owner: String)

/**
 * The case class represents a column descriptor
 * @param id An unique identifier
 * @param location table storage location
 * @param compressed table is compressed
 * @param inputFormat table input format
 * @param outputFormat table output format
 * @param databaseName Database name
 * @param tableName table name
 */
case class HiveStorage(id: UUID, location: String, compressed: Boolean,
                       inputFormat: String, outputFormat: String,
                       databaseName: String, tableName: String)