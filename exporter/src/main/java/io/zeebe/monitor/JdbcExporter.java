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
package io.zeebe.monitor;

import io.zeebe.exporter.context.Context;
import io.zeebe.exporter.context.Controller;
import io.zeebe.exporter.record.Record;
import io.zeebe.exporter.record.value.DeploymentRecordValue;
import io.zeebe.exporter.record.value.IncidentRecordValue;
import io.zeebe.exporter.record.value.WorkflowInstanceRecordValue;
import io.zeebe.exporter.record.value.deployment.DeployedWorkflow;
import io.zeebe.exporter.record.value.deployment.DeploymentResource;
import io.zeebe.exporter.spi.Exporter;
import io.zeebe.protocol.clientapi.RecordType;
import io.zeebe.protocol.clientapi.ValueType;
import org.slf4j.Logger;

import java.net.URI;
import java.net.URL;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.*;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class JdbcExporter implements Exporter {

  private static final String INSERT_WORKFLOW =
      "INSERT INTO WORKFLOW (ID_, KEY_, BPMN_PROCESS_ID_, VERSION_, RESOURCE_, TIMESTAMP_) VALUES ('%s', %d, '%s', %d, '%s', %d);";

  private static final String INSERT_WORKFLOW_INSTANCE =
      "INSERT INTO WORKFLOW_INSTANCE"
          + " (ID_, PARTITION_ID_, KEY_, INTENT_, WORKFLOW_INSTANCE_KEY_, ACTIVITY_ID_, SCOPE_INSTANCE_KEY_, PAYLOAD_, WORKFLOW_KEY_, TIMESTAMP_)"
          + " VALUES "
          + "('%s', %d, %d, '%s', %d, '%s', %d, '%s', %d, %d);";

  private static final String INSERT_INCIDENT =
      "INSERT INTO INCIDENT"
          + " (ID_, KEY_, WORKFLOW_INSTANCE_KEY_, ACTIVITY_INSTANCE_KEY_, JOB_KEY_, ERROR_TYPE_, ERROR_MSG_, TIMESTAMP_);"
          + " VALUES "
          + "('%s', %d, %d, %d, %d, '%s', '%s', %d)";

  public static final int BATCH_SIZE = 100;
  public static final Duration COMMIT_TIMER = Duration.ofSeconds(15);

  private final Map<ValueType, Consumer<Record>> insertCreatorPerType = new HashMap<>();
  private final List<String> insertStatements;

  private Logger log;
  private JdbcExporterConfiguration configuration;
  private Connection connection;

  public JdbcExporter() {
    insertCreatorPerType.put(ValueType.DEPLOYMENT, this::createWorkflowTableInserts);
    insertCreatorPerType.put(ValueType.WORKFLOW_INSTANCE, this::createWorkflowInstanceTableInsert);
    insertCreatorPerType.put(ValueType.INCIDENT, this::createIncidentTableInsert);

    insertStatements = new ArrayList<>();
  }

  @Override
  public void configure(final Context context) {
    log = context.getLogger();
    configuration = context.getConfiguration().instantiate(JdbcExporterConfiguration.class);

    log.debug("Exporter configured with {}", configuration);
    try {
      Class.forName(configuration.driverName);
    } catch (final ClassNotFoundException e) {
      throw new RuntimeException("Driver not found in class path", e);
    }
  }

  @Override
  public void open(final Controller controller) {
    try {
      connection =
          DriverManager.getConnection(
              configuration.jdbcUrl, configuration.userName, configuration.password);
      connection.setAutoCommit(true);
    } catch (final SQLException e) {
      throw new RuntimeException("Error on opening database.", e);
    }

    createTables();
    log.info("Start exporting to {}.", configuration.jdbcUrl);

    controller.scheduleTask(COMMIT_TIMER, this::tryToExecuteInsertBatch);
  }

  private void createTables() {
    try (final Statement statement = connection.createStatement()) {

      final URL resource = JdbcExporter.class.getResource("/CREATE_SCHEMA.sql");
      final URI uri = resource.toURI();
      FileSystems.newFileSystem(uri, Collections.EMPTY_MAP);
      final Path path = Paths.get(uri);
      final byte[] bytes = Files.readAllBytes(path);
      final String sql = new String(bytes);

      log.info("Create tables:\n{}", sql);
      statement.executeUpdate(sql);
    } catch (final Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void close() {
    try {
      connection.close();
    } catch (final Exception e) {
      log.warn("Failed to close jdbc connection", e);
    }
    log.info("Exporter closed");
  }

  @Override
  public void export(final Record record) {
    if (record.getMetadata().getRecordType() != RecordType.EVENT) {
      return;
    }

    final Consumer<Record> recordConsumer =
        insertCreatorPerType.get(record.getMetadata().getValueType());
    if (recordConsumer != null) {
      recordConsumer.accept(record);

      if (insertStatements.size() > BATCH_SIZE) {
        tryToExecuteInsertBatch();
      }
    }
  }

  private void tryToExecuteInsertBatch() {
    try (final Statement statement = connection.createStatement()) {
      for (final String insert : insertStatements) {
        statement.addBatch(insert);
      }
      statement.executeBatch();
      insertStatements.clear();
    } catch (final Exception e) {
      log.error("Batch insert failed!", e);
    }
  }

  private void createWorkflowTableInserts(final Record record) {
    final long key = record.getKey();
    final long timestamp = record.getTimestamp().toEpochMilli();
    final DeploymentRecordValue deploymentRecordValue = (DeploymentRecordValue) record.getValue();

    final List<DeploymentResource> resources = deploymentRecordValue.getResources();
    for (final DeploymentResource resource : resources) {
      final List<DeployedWorkflow> deployedWorkflows =
          deploymentRecordValue
              .getDeployedWorkflows()
              .stream()
              .filter(w -> w.getResourceName().equals(resource.getResourceName()))
              .collect(Collectors.toList());
      for (final DeployedWorkflow deployedWorkflow : deployedWorkflows) {
        final String insertStatement =
            String.format(
                INSERT_WORKFLOW,
                createId(),
                key,
                deployedWorkflow.getBpmnProcessId(),
                deployedWorkflow.getVersion(),
                new String(resource.getResource()),
                timestamp);
        insertStatements.add(insertStatement);
      }
    }
  }

  private void createWorkflowInstanceTableInsert(final Record record) {
    final long key = record.getKey();
    final int partitionId = record.getMetadata().getPartitionId();
    final String intent = record.getMetadata().getIntent().name();
    final long timestamp = record.getTimestamp().toEpochMilli();

    final WorkflowInstanceRecordValue workflowInstanceRecordValue =
        (WorkflowInstanceRecordValue) record.getValue();
    final long workflowInstanceKey = workflowInstanceRecordValue.getWorkflowInstanceKey();
    final String activityId = workflowInstanceRecordValue.getActivityId();
    final long scopeInstanceKey = workflowInstanceRecordValue.getScopeInstanceKey();
    final String payload = workflowInstanceRecordValue.getPayload();
    final long workflowKey = workflowInstanceRecordValue.getWorkflowKey();

    final String insertStatement =
        String.format(
            INSERT_WORKFLOW_INSTANCE,
            createId(),
            partitionId,
            key,
            intent,
            workflowInstanceKey,
            activityId,
            scopeInstanceKey,
            payload,
            workflowKey,
            timestamp);
    insertStatements.add(insertStatement);
  }

  private void createIncidentTableInsert(final Record record) {
    final long key = record.getKey();
    final long timestamp = record.getTimestamp().toEpochMilli();

    final IncidentRecordValue incidentRecordValue = (IncidentRecordValue) record.getValue();
    final long workflowInstanceKey = incidentRecordValue.getWorkflowInstanceKey();
    final long activityInstanceKey = incidentRecordValue.getActivityInstanceKey();
    final long jobKey = incidentRecordValue.getJobKey();
    final String errorType = incidentRecordValue.getErrorType();
    final String errorMessage = incidentRecordValue.getErrorMessage();

    final String insertStatement =
        String.format(
            INSERT_INCIDENT,
            createId(),
            key,
            workflowInstanceKey,
            activityInstanceKey,
            jobKey,
            errorType,
            errorMessage,
            timestamp);
    insertStatements.add(insertStatement);
  }

  private String createId() {
    return UUID.randomUUID().toString();
  }
}
