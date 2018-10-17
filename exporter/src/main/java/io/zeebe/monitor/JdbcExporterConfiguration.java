package io.zeebe.monitor;

public class JdbcExporterConfiguration {

  String jdbcUrl = "jdbc:h2:mem:zeebe-monitor:DB_CLOSE_ON_EXIT=false";
  String driverName = "org.h2.Driver";

  String userName = "sa";
  String password = "";
}
