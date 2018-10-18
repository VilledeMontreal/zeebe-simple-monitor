package io.zeebe.monitor;

public class JdbcExporterConfiguration {

  String jdbcUrl = "jdbc:h2:~/zeebe-monitor;AUTO_SERVER=TRUE";
  String driverName = "org.h2.Driver";

  String userName = "sa";
  String password = "";

  int batchSize = 100;
}
