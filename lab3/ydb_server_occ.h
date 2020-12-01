#ifndef ydb_server_occ_h
#define ydb_server_occ_h

#include <string>
#include <map>
#include "extent_client.h"
#include "lock_protocol.h"
#include "lock_client.h"
#include "lock_client_cache.h"
#include "ydb_protocol.h"
#include "ydb_server.h"

typedef std::map<std::string, op_entry> entry_map;
typedef std::map<std::string, std::string> string_map;

class transaction_occ
{
public:
	entry_map entrymap;
	string_map readset;
};

class ydb_server_occ : public ydb_server
{
private:
	int transaction_count = 0;
	std::map<int, transaction_occ> transaction_cache;

	bool validation(string_map readset);
	ydb_protocol::status find_transaction(ydb_protocol::transaction_id id, transaction_occ &transaction);
	ydb_protocol::status remove_transaction(ydb_protocol::transaction_id id);

public:
	ydb_server_occ(std::string, std::string);
	~ydb_server_occ();
	ydb_protocol::status transaction_begin(int, ydb_protocol::transaction_id &);
	ydb_protocol::status transaction_commit(ydb_protocol::transaction_id, int &);
	ydb_protocol::status transaction_abort(ydb_protocol::transaction_id, int &);
	ydb_protocol::status get(ydb_protocol::transaction_id, const std::string, std::string &);
	ydb_protocol::status set(ydb_protocol::transaction_id, const std::string, const std::string, int &);
	ydb_protocol::status del(ydb_protocol::transaction_id, const std::string, int &);
};

#endif
