#ifndef ydb_server_h
#define ydb_server_h

#include <string>
#include <map>
#include <vector>
#include <set>
#include "extent_client.h"
#include "lock_protocol.h"
#include "lock_client.h"
#include "lock_client_cache.h"
#include "ydb_protocol.h"

enum op_kind{EDIT,DELETE};
class op_entry
{
public:
	op_kind kind;
	std::string value;

	op_entry():kind(EDIT),value(""){};
	op_entry(op_kind k,const std::string v = ""):kind(k),value(v){};
};
typedef std::map<std::string, op_entry> entry_map;
typedef std::set<std::string> string_set;

class ydb_server {
protected:
	extent_client *ec;
	lock_client *lc;
	std::map<std::string,extent_protocol::extentid_t> keymap;
	virtual extent_protocol::extentid_t key2id(const std::string key);

public:
	ydb_server(std::string, std::string);
	virtual ~ydb_server();
	virtual ydb_protocol::status transaction_begin(int, ydb_protocol::transaction_id &);
	virtual ydb_protocol::status transaction_commit(ydb_protocol::transaction_id, int &);
	virtual ydb_protocol::status transaction_abort(ydb_protocol::transaction_id, int &);
	virtual ydb_protocol::status get(ydb_protocol::transaction_id, const std::string, std::string &);
	virtual ydb_protocol::status set(ydb_protocol::transaction_id, const std::string, const std::string, int &);
	virtual ydb_protocol::status del(ydb_protocol::transaction_id, const std::string, int &);
};

#endif

