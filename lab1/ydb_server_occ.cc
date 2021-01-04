#include "ydb_server_occ.h"
#include "extent_client.h"

//#define DEBUG 1
# define COMMIT_LOCK 1025
# define TRANSCACTION_ID_LOCK 1026
ydb_server_occ::ydb_server_occ(std::string extent_dst, std::string lock_dst) : ydb_server(extent_dst, lock_dst)
{
	transaction_count = 0;
}

ydb_server_occ::~ydb_server_occ()
{
}

ydb_protocol::status ydb_server_occ::transaction_begin(int, ydb_protocol::transaction_id &out_id)
{ // the first arg is not used, it is just a hack to the rpc lib
	// lab3: your code here
	lc->acquire(TRANSCACTION_ID_LOCK);
	out_id = transaction_count++;
	printf("transaction_begin: %d\n", out_id);
	transaction_cache[out_id] = transaction_occ();
	lc->release(TRANSCACTION_ID_LOCK);
	return ydb_protocol::OK;
}

ydb_protocol::status ydb_server_occ::find_transaction(ydb_protocol::transaction_id id, transaction_occ &transaction)
{
	std::map<int, transaction_occ>::iterator iter = transaction_cache.find(id);
	if (iter == transaction_cache.end())
	{
		printf("find_transaction: no transaction: %d\n", id);
		return ydb_protocol::TRANSIDINV;
	}
	transaction = iter->second;
	return ydb_protocol::OK;
}

ydb_protocol::status ydb_server_occ::remove_transaction(ydb_protocol::transaction_id id)
{
	std::map<int, transaction_occ>::iterator iter = transaction_cache.find(id);
	if (iter == transaction_cache.end())
	{
		printf("remove_transaction: no transaction: %d\n", id);
		return ydb_protocol::TRANSIDINV;
	}
	transaction_cache.erase(iter);
	return ydb_protocol::OK;
}

bool ydb_server_occ::validation(string_map readset)
{
	string_map::iterator iter = readset.begin();
	for (; iter != readset.end(); iter++)
	{
		std::string value;
		std::string key = iter->first;
		ec->get(key2id(key), value);
		if (value != iter->second)
		{
			return false;
		}
	}
	return true;
}
ydb_protocol::status ydb_server_occ::transaction_commit(ydb_protocol::transaction_id id, int &)
{
	lc->acquire(COMMIT_LOCK);
	// lab3: your code here
	printf("transaction_commit: %d\n", id);
	transaction_occ transaction;
	if (find_transaction(id, transaction) == ydb_protocol::TRANSIDINV)
	{
		printf("transaction_commit: no transaction: %d\n", id);
		return ydb_protocol::TRANSIDINV;
	}

	entry_map entrymap = transaction.entrymap;
	string_map readset = transaction.readset;

	if (validation(readset))
	{
		entry_map::iterator iter = entrymap.begin();
		for (; iter != entrymap.end(); iter++)
		{
			std::string key = iter->first;
			if (iter->second.kind == EDIT)
			{
				ec->put(key2id(key), iter->second.value);
			}
			else
			{
				ec->put(key2id(key),"");
			}
		}
	}
	else
	{
		printf("validation false, abort transaction:%d\n", id);
		int a;
		if(transaction_abort(id, a)==ydb_protocol::OK)
		{
			lc->release(COMMIT_LOCK);
			return ydb_protocol::ABORT;
		}
		else{
            printf("abort error");
		}
	}

	if (remove_transaction(id) == ydb_protocol::TRANSIDINV)
	{
		printf("transaction_commit: no transaction: %d\n", id);
		lc->release(COMMIT_LOCK);
		return ydb_protocol::TRANSIDINV;
	}
	lc->release(COMMIT_LOCK);
	return ydb_protocol::OK;
}

ydb_protocol::status ydb_server_occ::transaction_abort(ydb_protocol::transaction_id id, int &)
{
	// lab3: your code here
	printf("transaction_abort: %d\n", id);

	if (remove_transaction(id) == ydb_protocol::TRANSIDINV)
	{
		printf("transaction_abort: no transaction: %d\n", id);
		return ydb_protocol::TRANSIDINV;
	}

	return ydb_protocol::OK;
}

ydb_protocol::status ydb_server_occ::get(ydb_protocol::transaction_id id, const std::string key, std::string &out_value)
{
	// lab3: your code here
	ydb_protocol::status ret = ydb_protocol::OK;
	
	transaction_occ transaction;
	if (find_transaction(id, transaction) == ydb_protocol::TRANSIDINV)
	{
		printf("get: no transaction: %d\n", id);
		return ydb_protocol::TRANSIDINV;
	}
	entry_map entrymap = transaction.entrymap;
	entry_map::iterator iter = entrymap.find(key);
	if (iter != entrymap.end())
	{
		if (iter->second.kind == EDIT)
		{
			printf("edit");
			out_value = iter->second.value;
		}
		else
		{
			printf("delete");
			out_value = "";
		}
	}
	else
	{
		std::map<int, transaction_occ>::iterator iter = transaction_cache.find(id);
		string_map readset = iter->second.readset;
		string_map::iterator iter2 = readset.find(key);
		if (iter2 != readset.end())
		{
			out_value = iter2->second;
		}
		else{
			ec->get(key2id(key), out_value);
			iter->second.readset[key] = out_value;
		}
	}
	printf("get: transaction:%d key:%s value:%s\n", id, key.c_str(), out_value.c_str());
	return ret;
	return ydb_protocol::OK;
}

ydb_protocol::status ydb_server_occ::set(ydb_protocol::transaction_id id, const std::string key, const std::string value, int &)
{
	// lab3: your code here
	printf("set: trans:%d key:%s value:%s\n", id, key.c_str(), value.c_str());

	std::map<int, transaction_occ>::iterator iter = transaction_cache.find(id);
	iter->second.entrymap[key] = op_entry(EDIT, value);
	return ydb_protocol::OK;
}

ydb_protocol::status ydb_server_occ::del(ydb_protocol::transaction_id id, const std::string key, int &)
{
	// lab3: your code here
	printf("del: trans:%d key:%s\n", id, key.c_str());

	std::map<int, transaction_occ>::iterator iter = transaction_cache.find(id);
	iter->second.entrymap[key] = op_entry(DELETE);
	return ydb_protocol::OK;
}
