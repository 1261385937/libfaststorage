#include "meta_describe.hpp"

struct storage_context {
	uint64_t first_id = 7156904296829767975;
	uint64_t second_id;
	uint32_t asset_id;
	uint16_t log_version = 3;
	uint32_t session_id = 2307938197;
	std::string client_mac = "20-0B-C7-2A-F1-C6";
	std::string client_ip = "172.16.194.45";
	uint16_t client_port = 59138;
	std::string server_mac = "00-0C-29-3E-FB-7B";
	std::string server_ip = "172.16.37.82";
	uint16_t server_port = 3306;
	uint32_t request_time{ 1669011233 };
	uint32_t request_time_usec = 994899;
	uint64_t execute_time = 146;
	uint16_t request_status = 1803;
	std::string account = "root";
	std::string os_user_name = "wangwei";
	std::string risk_type = "middle";
	std::set<std::string> matched_rules = { "white", "black", "user_define" };
	uint16_t risk_level = 1002;
	uint16_t protect_operate = 1401;
	uint8_t reviewed = 0;
	std::string comment = "this is a test insert";
	uint8_t sent = 0;
	std::string instance_name = "MYSQL";
	std::string client_app = "libmysql";
	std::string client_host = "www.mysql.com";
	std::string operation_statement =
		"SHOW VARIABLES LIKE 'lower_case_%'; SHOW VARIABLES LIKE 'sql_mode'; "
		"SELECT COUNT(*) AS support_ndb FROM information_schema.ENGINES WHERE "
		"Engine = 'ndbcluster'";
	std::deque<std::deque<std::string>> binding_variables = {
		{"name", "sex", "home"}, {"wangwei", "man", "hangzhou"}, {"xiaoming", "man", "shanghai"} };
	// std::deque<std::deque<std::string>> binding_variables;
	std::string statement_pattern =
		"SHOW VARIABLES LIKE 'LOWER_CASE_%'; SHOW VARIABLES LIKE 'SQL_MODE'; "
		"SELECT COUNT(*) AS SUPPORT_NDB FROM INFORMATION_SCHEMA.ENGINES WHERE "
		"ENGINE = 'NDBCLUSTER'";
	std::deque<std::deque<std::string>> reply = {
		{"Host, Db, User, Select_priv, Insert_priv, Update_priv, Delete_priv, "
		 "Create_priv, Drop_priv, Grant_priv, References_priv, Index_priv, "
		 "Alter_priv, Create_tmp_table_priv, Lock_tables_priv, "
		 "Create_view_priv, "
		 "Show_view_priv, Create_routine_priv, Alter_routine_priv, "
		 "Execute_priv, "
		 "Event_priv, Trigger_priv"} };
	std::string error_reply = "Empty set";
	int32_t rows_affected = 10000;
	uint8_t operation_type = 4;
	std::string operation_command = "SELECT";
	std::string operand_type = "TABLE";
	std::vector<std::string> operand_name = { "DB", "hhhh", "rrrrr" };
	std::vector<std::string> second_operand_name = { "*", "name", "id", "ip" };
	std::string web_user_name = "admin";
	std::string web_url = "http://192.168.3.7/local";
	std::string web_ip = "192.168.3.7";
	std::string web_session_id = "CE60:4ACC:75EA2A:8432B7:637B1BE2";

	META_DESCRIBE_INTRUSIVE(storage_context, first_id, second_id, asset_id, log_version, session_id,
							client_mac, client_ip, client_port, server_mac, server_ip, server_port,
							request_time, request_time_usec, execute_time, request_status, account,
							os_user_name, risk_type, matched_rules, risk_level, protect_operate,
							reviewed, comment, sent, instance_name, client_app, client_host,
							operation_statement, binding_variables, statement_pattern, reply,
							error_reply, rows_affected, operation_type, operation_command,
							operand_type, operand_name, second_operand_name, web_user_name, web_url,
							web_ip, web_session_id);
};

constexpr auto sql = R"(
CREATE TABLE IF NOT EXISTS default.sql_log
(
    `first_id` UInt64,
    `second_id` UInt64,
    `asset_id` UInt32,
    `log_version` UInt16,
    `session_id` UInt32,
    `client_mac` String,
    `client_ip` String,
    `client_port` UInt16,
    `server_mac` String,
    `server_ip` String,
    `server_port` UInt16,
    `request_time` UInt32,
    `request_time_usec` UInt32,
    `minute_time` UInt32 MATERIALIZED request_time / 60,
    `request_microsecond` Int64 MATERIALIZED (request_time * 1000000) + request_time_usec,
    `execute_time` UInt64,
    `request_status` UInt16,
    `account` String,
    `os_user_name` String,
    `risk_type` String,
    `matched_rules` Array(String),
    `risk_level` UInt16,
    `protect_operate` UInt16,
    `reviewed` UInt8 DEFAULT 0,
    `comment` String DEFAULT '',
    `sent` UInt8 DEFAULT 0,
    `instance_name` String,
    `client_app` String,
    `client_host` String,
    `operation_statement` String,
    `binding_variables` Array(Array(String)),
    `statement_pattern` String,
    `reply` Array(Array(String)),
    `error_reply` String,
    `rows_affected` Int32,
    `operation_type` UInt8,
    `operation_command` String,
    `operand_type` String,
    `operand_name` Array(String),
    `second_operand_name` Array(String),
    `web_user_name` String,
    `web_url` String,
    `web_ip` String,
    `web_session_id` String
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(toDateTime(request_time))
ORDER BY (minute_time,
 asset_id,
 account,
 risk_level,
 request_status,
 session_id,
 client_ip)
SETTINGS index_granularity = 8192;
)";
