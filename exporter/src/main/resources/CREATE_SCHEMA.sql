CREATE TABLE IF NOT EXISTS WORKFLOW
(
  id VARCHAR PRIMARY KEY,
 	key_ BIGINT,
  bpmnProcessId VARCHAR,
  version INT,
  resource VARCHAR
);

CREATE TABLE IF NOT EXISTS WORKFLOW_INSTANCE
(
	id VARCHAR PRIMARY KEY,
	partitionId INT,
	key_ BIGINT,
	intent VARCHAR,
	workflowInstanceKey BIGINT,
	activityId VARCHAR,
	scopeInstanceKey BIGINT,
	payload BLOB,
	workflowKey BIGINT
);

CREATE TABLE IF NOT EXISTS INCIDENTS
(
	id VARCHAR PRIMARY KEY,
	key_ BIGINT,
	workflowInstanceKey BIGINT,
	activityInstanceKey BIGINT,
	errorType VARCHAR,
	errorMsg VARCHAR
);
