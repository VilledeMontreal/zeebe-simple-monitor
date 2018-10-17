package io.zeebe.monitor;

public class JdbcExporterConfiguration {

  String jdbcUrl = "jdbc:sqlite:/tmp/zeebe-monitor";
  String driverName = "org.sqlite.JDBC";

  String userName = "sa";
  String password = "";
}
