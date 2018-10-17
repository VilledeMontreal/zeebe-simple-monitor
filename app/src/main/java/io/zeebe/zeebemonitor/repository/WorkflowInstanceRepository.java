/*
 * Copyright © 2017 camunda services GmbH (info@camunda.com)
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
package io.zeebe.zeebemonitor.repository;

import io.zeebe.zeebemonitor.entity.WorkflowInstanceEntity;
import org.springframework.data.mongodb.repository.Query;
import org.springframework.data.repository.CrudRepository;

public interface WorkflowInstanceRepository extends CrudRepository<WorkflowInstanceEntity, String>
{
    @Query(value = "{ 'workflowKey' : ?0, 'ended' : false }", count = true)
    long countRunningInstances(long workfloKey);

    @Query(value = "{ 'workflowKey' : ?0, 'ended' : true }", count = true)
    long countEndedInstances(long workflowKey);

    WorkflowInstanceEntity findByWorkflowInstanceKeyAndPartitionId(long workflowInstanceKeys, int partitionId);

}
