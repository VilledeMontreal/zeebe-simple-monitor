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
package io.zeebe.zeebemonitor.rest;

import io.zeebe.client.api.clients.WorkflowClient;
import io.zeebe.client.api.commands.DeployWorkflowCommandStep1.DeployWorkflowCommandBuilderStep2;
import io.zeebe.zeebemonitor.entity.WorkflowEntity;
import io.zeebe.zeebemonitor.entity.WorkflowInstanceEntity;
import io.zeebe.zeebemonitor.repository.WorkflowInstanceRepository;
import io.zeebe.zeebemonitor.repository.WorkflowRepository;
import io.zeebe.zeebemonitor.zeebe.ZeebeConnectionService;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping(path = "/api/workflows")
public class WorkflowResource {
  @Autowired private ZeebeConnectionService connections;

  @Autowired private WorkflowRepository workflowRepository;

  @Autowired private WorkflowInstanceRepository workflowInstanceRepository;

  @RequestMapping("/")
  public List<WorkflowDto> getWorkflows() {
    final List<WorkflowDto> dtos = new ArrayList<>();
    for (WorkflowEntity workflowEntity : workflowRepository.findAll()) {
      final WorkflowDto dto = toDto(workflowEntity);
      dtos.add(dto);
    }

    return dtos;
  }

  @RequestMapping(path = "/{workflowKey}")
  public WorkflowDto findWorkflow(@PathVariable("workflowKey") long workflowKey) {
    return workflowRepository.findById(workflowKey).map(this::toDto).orElse(null);
  }

  private WorkflowDto toDto(WorkflowEntity workflowEntity) {
    final long workflowKey = workflowEntity.getKey();

    final Map<Long, List<WorkflowInstanceEntity>> instanceEventyByKey =
        StreamSupport.stream(
                workflowInstanceRepository.findByWorkflowKey(workflowKey).spliterator(), false)
            .collect(Collectors.groupingBy(WorkflowInstanceEntity::getWorkflowInstanceKey));

    final AtomicInteger countRunning = new AtomicInteger(0);
    final AtomicInteger countEnded = new AtomicInteger(0);

    instanceEventyByKey.forEach(
        (workflowInstanceKey, events) -> {
          final boolean isCompleted =
              events
                  .stream()
                  .anyMatch(
                      e ->
                          e.getKey() == workflowInstanceKey
                              && e.getIntent().equals("ELEMENT_COMPLETED"));

          if (isCompleted) {
            countEnded.incrementAndGet();
          } else {
            countRunning.incrementAndGet();
          }
        });

    final WorkflowDto dto = WorkflowDto.from(workflowEntity, countRunning.get(), countEnded.get());
    return dto;
  }

  @RequestMapping(path = "/{workflowKey}", method = RequestMethod.POST)
  public void createWorkflowInstance(
      @PathVariable("workflowKey") long workflowKey, @RequestBody String payload) {

    connections
        .getClient()
        .workflowClient()
        .newCreateInstanceCommand()
        .workflowKey(workflowKey)
        .payload(payload)
        .send()
        .join();
  }

  @RequestMapping(path = "/", method = RequestMethod.POST)
  public void uploadModel(@RequestBody DeploymentDto deployment)
      throws UnsupportedEncodingException {

    final WorkflowClient workflowClient = connections.getClient().workflowClient();

    final List<FileDto> files = deployment.getFiles();
    final FileDto firstFile = files.get(0);

    final DeployWorkflowCommandBuilderStep2 cmd =
        workflowClient
            .newDeployCommand()
            .addResourceBytes(firstFile.getContent(), firstFile.getFilename());

    for (FileDto file : files.subList(1, files.size())) {
      cmd.addResourceBytes(file.getContent(), file.getFilename());
    }

    cmd.send().join();
  }
}
