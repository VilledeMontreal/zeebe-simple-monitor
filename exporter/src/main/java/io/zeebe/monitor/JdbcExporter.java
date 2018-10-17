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
import io.zeebe.exporter.spi.Exporter;
import org.slf4j.Logger;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class JdbcExporter implements Exporter {

  private Logger log;
  private Controller controller;
  private JdbcExporterConfiguration configuration;

  private long lastPosition = -1;

  private Connection connection;

  @Override
  public void configure(Context context) {
    log = context.getLogger();
    configuration = context.getConfiguration().instantiate(JdbcExporterConfiguration.class);

    log.debug("Exporter configured with {}", configuration);
    try {
      Class.forName(configuration.driverName);
    } catch (ClassNotFoundException e) {
      throw new RuntimeException("Driver not found in class path", e);
    }
  }

  @Override
  public void open(Controller controller) {
    this.controller = controller;

    try {
      connection =
          DriverManager.getConnection(
              configuration.jdbcUrl, configuration.userName, configuration.password);
    } catch (SQLException e) {
      throw new RuntimeException("Error on opening database.", e);
    }

    createTables();
    log.info("Exporter opened");
  }

  private void createTables() {
    try {
      final Path createTablesPath =
          Paths.get(JdbcExporter.class.getResource("CREATE_SCHEMA.sql").toURI());
      final String sql = new String(Files.readAllBytes(createTablesPath));

      final Statement statement = connection.createStatement();
      statement.execute(sql);

    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void close() {
    try {
      connection.close();
    } catch (Exception e) {
      log.warn("Failed to close jdbc connection", e);
    }
    log.info("Exporter closed");
  }

  @Override
  public void export(Record record) {
    lastPosition = record.getPosition();
  }
}
