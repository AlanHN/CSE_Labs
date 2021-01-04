#ifndef ydb_server_2pl_h
#define ydb_server_2pl_h

#include <string>
#include <map>
#include <queue>
#include "extent_client.h"
#include "lock_protocol.h"
#include "lock_client.h"
#include "lock_client_cache.h"
#include "ydb_protocol.h"
#include "ydb_server.h"

#define GRAPH_SIZE 1024

class transaction_entry
{
public:
 	entry_map entrymap;
	string_set lockmap; 
};
	
class ydb_server_2pl: public ydb_server {
private:
    int transaction_count;
	std::map<int, transaction_entry> transaction_cache;
	std::map<std::string,int> global_lock;
	bool waiting_graph[GRAPH_SIZE][GRAPH_SIZE];

    bool detect_cycle();
	ydb_protocol::status find_transaction(ydb_protocol::transaction_id id,transaction_entry &transaction);
	ydb_protocol::status remove_transaction(ydb_protocol::transaction_id id);
	ydb_protocol::status acquire(ydb_protocol::transaction_id id,std::string key);

public:
	ydb_server_2pl(std::string, std::string);
	~ydb_server_2pl();
	ydb_protocol::status transaction_begin(int, ydb_protocol::transaction_id &);
	ydb_protocol::status transaction_commit(ydb_protocol::transaction_id, int &);
	ydb_protocol::status transaction_abort(ydb_protocol::transaction_id, int &);
	ydb_protocol::status get(ydb_protocol::transaction_id, const std::string, std::string &);
	ydb_protocol::status set(ydb_protocol::transaction_id, const std::string, const std::string, int &);
	ydb_protocol::status del(ydb_protocol::transaction_id, const std::string, int &);
};

#endif

