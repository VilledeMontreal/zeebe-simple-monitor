package io.zeebe.monitor;

import io.zeebe.exporter.context.Configuration;
import io.zeebe.exporter.context.Context;
import io.zeebe.exporter.context.Controller;
import io.zeebe.exporter.record.Record;
import io.zeebe.exporter.record.RecordMetadata;
import io.zeebe.exporter.record.value.DeploymentRecordValue;
import io.zeebe.exporter.record.value.deployment.DeployedWorkflow;
import io.zeebe.exporter.record.value.deployment.DeploymentResource;
import io.zeebe.exporter.record.value.deployment.ResourceType;
import io.zeebe.protocol.clientapi.RecordType;
import io.zeebe.protocol.clientapi.ValueType;
import io.zeebe.protocol.intent.DeploymentIntent;
import io.zeebe.protocol.intent.Intent;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class JdbcExporterTest {

  private JdbcExporter exporter;
  private JdbcExporterConfiguration configuration;

  @Before
  public void setup() {
    exporter = new JdbcExporter();
    configuration = new JdbcExporterConfiguration();
    configuration.jdbcUrl = "jdbc:h2:mem:zeebe-monitor";
    configuration.batchSize = 0;

    final Context contextMock = mock(Context.class);
    final Configuration configMock = mock(Configuration.class);
    when(configMock.instantiate(JdbcExporterConfiguration.class)).thenReturn(configuration);
    when(contextMock.getConfiguration()).thenReturn(configMock);

    final Logger logger = LoggerFactory.getLogger("simple-monitor");
    when(contextMock.getLogger()).thenReturn(logger);

    exporter.configure(contextMock);

    final Controller controllerMock = mock(Controller.class);
    exporter.open(controllerMock);
  }

  @Test
  public void shouldCreateTables() throws Exception {
    // given

    // when
    try (final Connection connection =
        DriverManager.getConnection(
            configuration.jdbcUrl, configuration.userName, configuration.password)) {
      try (final Statement statement = connection.createStatement()) {
        statement.execute("SELECT * FROM WORKFLOW;");
        statement.execute("SELECT * FROM WORKFLOW_INSTANCE;");
        statement.execute("SELECT * FROM INCIDENT;");
      }
    }

    // then no error should happen
  }

  @Test
  public void shouldInsertWorkflow() throws Exception {
    // given
    final Record deploymentRecord =
        createRecordMockForIntent(ValueType.DEPLOYMENT, DeploymentIntent.CREATED);
    final DeploymentRecordValue deploymentRecordValueMock = createDeploymentRecordValueMock();
    when(deploymentRecord.getValue()).thenReturn(deploymentRecordValueMock);

    // when
    exporter.export(deploymentRecord);

    // then
    try (final Connection connection =
        DriverManager.getConnection(
            configuration.jdbcUrl, configuration.userName, configuration.password)) {
      try (final Statement statement = connection.createStatement()) {
        statement.execute("SELECT * FROM WORKFLOW;");
        final ResultSet resultSet = statement.getResultSet();
        resultSet.beforeFirst();
        resultSet.next();

        // (ID_, KEY_, BPMN_PROCESS_ID_, VERSION_, RESOURCE_, TIMESTAMP_)
        final String uuid = resultSet.getString(1);
        UUID.fromString(uuid); // should not thrown an exception

        final long key = resultSet.getLong(2);
        assertThat(key).isEqualTo(1);

        final String bpmnProcessId = resultSet.getString(3);
        assertThat(bpmnProcessId).isEqualTo("process");

        final int version = resultSet.getInt(4);
        assertThat(version).isEqualTo(0);

        final String resource = resultSet.getString(5);
        assertThat(resource).isEqualTo("ThisistheResource");

        final long timeStamp = resultSet.getLong(6);
        assertThat(timeStamp).isGreaterThan(0).isLessThan(Instant.now().toEpochMilli());
      }
    }
  }

  private Record createRecordMockForIntent(final ValueType valueType, final Intent intent) {
    final Record recordMock = mock(Record.class);
    when(recordMock.getKey()).thenReturn(1L);
    when(recordMock.getTimestamp()).thenReturn(Instant.now());

    final RecordMetadata metadataMock = mock(RecordMetadata.class);
    when(metadataMock.getRecordType()).thenReturn(RecordType.EVENT);
    when(metadataMock.getPartitionId()).thenReturn(0);
    when(metadataMock.getValueType()).thenReturn(valueType);
    when(metadataMock.getIntent()).thenReturn(intent);

    when(recordMock.getMetadata()).thenReturn(metadataMock);
    return recordMock;
  }

  private DeploymentRecordValue createDeploymentRecordValueMock() {
    final DeploymentRecordValue deploymentRecordValueMock = mock(DeploymentRecordValue.class);

    final DeploymentResource resourceMock = mock(DeploymentResource.class);
    when(resourceMock.getResource()).thenReturn("ThisistheResource".getBytes());
    when(resourceMock.getResourceName()).thenReturn("process.bpmn");
    when(resourceMock.getResourceType()).thenReturn(ResourceType.BPMN_XML);

    final List<DeploymentResource> resourceList = new ArrayList<>();
    resourceList.add(resourceMock);
    when(deploymentRecordValueMock.getResources()).thenReturn(resourceList);

    final DeployedWorkflow deployedWorkflowMock = mock(DeployedWorkflow.class);
    when(deployedWorkflowMock.getResourceName()).thenReturn("process.bpmn");
    when(deployedWorkflowMock.getBpmnProcessId()).thenReturn("process");
    when(deployedWorkflowMock.getVersion()).thenReturn(0);
    when(deployedWorkflowMock.getWorkflowKey()).thenReturn(1L);

    final List<DeployedWorkflow> deployedWorkflows = new ArrayList<>();
    deployedWorkflows.add(deployedWorkflowMock);
    when(deploymentRecordValueMock.getDeployedWorkflows()).thenReturn(deployedWorkflows);

    return deploymentRecordValueMock;
  }
}
