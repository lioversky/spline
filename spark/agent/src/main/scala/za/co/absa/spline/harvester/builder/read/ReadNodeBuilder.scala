/*
 * Copyright 2019 ABSA Group Limited
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

package za.co.absa.spline.harvester.builder.read

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import za.co.absa.spline.harvester.ComponentCreatorFactory
import za.co.absa.spline.harvester.ModelConstants.OperationParams
import za.co.absa.spline.harvester.builder.OperationNodeBuilder
import za.co.absa.spline.producer.rest.model.ReadOperation

class ReadNodeBuilder
(val command: ReadCommand)
  (implicit val componentCreatorFactory: ComponentCreatorFactory)
  extends OperationNodeBuilder {

  override protected type R = ReadOperation
  override val operation: LogicalPlan = command.operation

  override def build(): ReadOperation = ReadOperation(
    inputSources = command.sourceIdentifier.uris,
    id = id,
    schema = Some(outputSchema),
    params = command.params ++ Map(
      OperationParams.Name -> operation.nodeName,
      OperationParams.SourceType -> command.sourceIdentifier.format
    ))
}
