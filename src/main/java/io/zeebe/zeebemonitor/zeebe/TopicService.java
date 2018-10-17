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
package io.zeebe.zeebemonitor.zeebe;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

import io.zeebe.gateway.api.commands.PartitionInfo;
import io.zeebe.zeebemonitor.entity.PartitionEntity;
import io.zeebe.zeebemonitor.repository.PartitionRepository;

@Component
public class TopicService
{
    @Autowired
    private PartitionRepository partitionRepository;

    @Autowired
    private ZeebeConnectionService connectionService;

    @Autowired
    private ZeebeSubscriber subscriber;

    @Async
    public void synchronizeAsync() throws InterruptedException, ExecutionException
    {
        synchronizeWithBroker();
    }

    public void synchronizeWithBroker() throws InterruptedException, ExecutionException
    {
        // yes - this will totally break in some scnearios! Quick hack for the moment until
        // tools are migrated to 0.12
        final List<PartitionInfo> partitions =
            connectionService.getClient()
                .newTopologyRequest().send().get().getBrokers().get(0).getPartitions();

        final List<Integer> availablePartitions = new ArrayList<>();
        for (PartitionEntity partitionEntity : partitionRepository.findAll())
        {
            availablePartitions.add(partitionEntity.getId());
        }

        partitions.removeIf(p -> availablePartitions.contains(p.getPartitionId()));

        partitions.forEach(p ->
        {
            final PartitionEntity partitionEntity = new PartitionEntity();
            partitionEntity.setId(p.getPartitionId());

            partitionRepository.save(partitionEntity);
        });

        subscriber.openSubscription();
    }

}
