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
import io.zeebe.zeebemonitor.entity.WorkflowEntity;
import io.zeebe.zeebemonitor.repository.WorkflowInstanceRepository;
import io.zeebe.zeebemonitor.repository.WorkflowRepository;
import io.zeebe.zeebemonitor.zeebe.ZeebeConnectionService;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;
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
    final long workflowKey = workflowEntity.getWorkflowKey();

    // TODO count the instances
    final long countRunning = 0; // workflowInstanceRepository.countRunningInstances(workflowKey);
    final long countEnded = 0; // workflowInstanceRepository.countEndedInstances(workflowKey);

    final WorkflowDto dto = WorkflowDto.from(workflowEntity, countRunning, countEnded);
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

    for (FileDto file : deployment.getFiles()) {
      workflowClient
          .newDeployCommand()
          .addResourceBytes(file.getContent(), file.getFilename())
          .send()
          .join();
    }
  }
}
