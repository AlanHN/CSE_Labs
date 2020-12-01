#include "ydb_server_2pl.h"
#include "extent_client.h"

//#define DEBUG 1
#define WAITING_GRAPH_LOCK 1025
#define GLOBAL_LOCK_LOCK 1025
#define TRANSACTION_ID_LOCK 1025
ydb_server_2pl::ydb_server_2pl(std::string extent_dst, std::string lock_dst) : ydb_server(extent_dst, lock_dst)
{
	transaction_count = 0;
	for(int i = 0; i <GRAPH_SIZE;i++)
	{
		for(int j=0;j<GRAPH_SIZE;j++)
		{
			waiting_graph[i][j]=0;
		}
	}
}

ydb_server_2pl::~ydb_server_2pl()
{
}

ydb_protocol::status ydb_server_2pl::transaction_begin(int, ydb_protocol::transaction_id &out_id)
{	// the first arg is not used, it is just a hack to the rpc lib
	// lab3: your code here
	lc->acquire(TRANSACTION_ID_LOCK);
	out_id = transaction_count++;
	printf("transaction_begin: %d\n", out_id);
	transaction_cache[out_id] = transaction_entry();
	lc->release(TRANSACTION_ID_LOCK);
	return ydb_protocol::OK;
}

ydb_protocol::status ydb_server_2pl::find_transaction(ydb_protocol::transaction_id id, transaction_entry &transaction)
{
	std::map<int, transaction_entry>::iterator iter = transaction_cache.find(id);
	if (iter == transaction_cache.end())
	{
		printf("no transaction: %d\n", id);
		return ydb_protocol::TRANSIDINV;
	}
	transaction = iter->second;
	return ydb_protocol::OK;
}

ydb_protocol::status ydb_server_2pl::remove_transaction(ydb_protocol::transaction_id id)
{
	std::map<int, transaction_entry>::iterator iter = transaction_cache.find(id);
	if (iter == transaction_cache.end())
	{
		printf("no transaction: %d\n", id);
		return ydb_protocol::TRANSIDINV;
	}
	transaction_cache.erase(iter);
	return ydb_protocol::OK;
}

ydb_protocol::status ydb_server_2pl::transaction_commit(ydb_protocol::transaction_id id, int &)
{
	// lab3: your code here
	printf("transaction_commit: %d\n", id);
	transaction_entry transaction;
	if (find_transaction(id, transaction) == ydb_protocol::TRANSIDINV)
	{
		printf("transaction_commit: no transaction: %d\n", id);
		return ydb_protocol::TRANSIDINV;
	}

	entry_map entrymap = transaction.entrymap;
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
			ec->put(key2id(key), "");
		}
	}
	string_set lockmap = transaction.lockmap;
	string_set::iterator iter2 = lockmap.begin();

	lc->acquire(GLOBAL_LOCK_LOCK);
	for (; iter2 != lockmap.end(); iter2++)
	{
		lc->release(key2id(*iter2));
		global_lock.erase(*iter2);
		printf("release: transaction:%d key:%s\n",id,(*iter2).c_str());
	}
	remove_transaction(id);
	lc->release(GLOBAL_LOCK_LOCK);
	return ydb_protocol::OK;
}

ydb_protocol::status ydb_server_2pl::transaction_abort(ydb_protocol::transaction_id id, int &)
{
	// lab3: your code here
	printf("transaction_abort: %d\n", id);

	transaction_entry transaction;
	if (find_transaction(id, transaction) == ydb_protocol::TRANSIDINV)
	{
		return ydb_protocol::TRANSIDINV;
	}

	string_set lockmap = transaction.lockmap;
	string_set::iterator iter = lockmap.begin();
	lc->acquire(GLOBAL_LOCK_LOCK);
	for (; iter != lockmap.end(); iter++)
	{
		printf("release: transaction:%d key:%s\n",id,(*iter).c_str());
		lc->release(key2id(*iter));
		global_lock.erase(*iter);
	}
	remove_transaction(id);

	lc->release(GLOBAL_LOCK_LOCK);
	return ydb_protocol::OK;
}

bool ydb_server_2pl::detect_cycle()
{
	std::vector<int> in(GRAPH_SIZE, 0);
	std::queue<int> q;
	for (int i = 0; i < GRAPH_SIZE; i++)
	{
		for (int j = 0; j < GRAPH_SIZE; j++)
		{
			if (waiting_graph[j][i] == 1)
			{
				in[i]++;
			}
		}
	}
	for (int i = 0; i < GRAPH_SIZE; i++)
	{
		if (in[i] == 0)
		{
			q.push(i);
		}
	}
	int NumOfList = 0;
	while (!q.empty())
	{
		int p = q.front();
		q.pop();
		NumOfList++;
		for (int i = 0; i < GRAPH_SIZE; i++)
		{
			if (waiting_graph[p][i] == 1)
			{
				in[i]--;
				if (in[i] == 0)
				{
					q.push(i);
				}
			}
		}
	}
	return NumOfList != GRAPH_SIZE;
}

ydb_protocol::status ydb_server_2pl::acquire(ydb_protocol::transaction_id id, std::string key)
{
	printf("acquire: transaction:%d key:%s\n",id,key.c_str());
	lc->acquire(GLOBAL_LOCK_LOCK);
    
	std::map<int, transaction_entry>::iterator iter = transaction_cache.find(id);
	if (iter == transaction_cache.end())
	{
		printf("no transaction: %d\n", id);
		lc->release(GLOBAL_LOCK_LOCK);
		return ydb_protocol::TRANSIDINV;
	}
	transaction_entry transaction = iter->second;
	string_set local_lock = transaction.lockmap;
	// the transaction doesn't have lock now
	if (local_lock.find(key) == local_lock.end())
	{
		// others don't have lock too
		if (global_lock.find(key) == global_lock.end())
		{
			printf("transaction %d need acquire lock %s\n",id,key.c_str());
			lc->release(GLOBAL_LOCK_LOCK);
			lc->acquire(key2id(key));
			lc->acquire(GLOBAL_LOCK_LOCK);
			printf("transaction %d acquire lock %s successfully\n",id,key.c_str());
			iter->second.lockmap.insert(key);
			global_lock[key] = id;
		}
		//others has lock
		else
		{
			int depend_id = global_lock.find(key)->second;
			waiting_graph[id][depend_id] = 1;
			if (detect_cycle())
			{
				printf("cycle, abort transaction:%d\n", id);
				int a;
				lc->release(GLOBAL_LOCK_LOCK);
				transaction_abort(id, a);
				lc->acquire(GLOBAL_LOCK_LOCK);
				for(int i = 0;i<GRAPH_SIZE;i++)
				{
					waiting_graph[id][i]=0;
					waiting_graph[i][id]=0;
				}
				lc->release(GLOBAL_LOCK_LOCK);
				return ydb_protocol::ABORT;
			}
			else
			{
				printf("transaction %d need wait for lock %s in transaction %d\n",id,key.c_str(),depend_id);
				lc->release(GLOBAL_LOCK_LOCK);
				lc->acquire(key2id(key));
				lc->acquire(GLOBAL_LOCK_LOCK);
				waiting_graph[id][depend_id] = 0;
				printf("transaction %d acquire lock %s successfully\n",id,key.c_str());
				iter->second.lockmap.insert(key);
				global_lock[key] = id;
			}
		}
	}
	else{
		printf("transaction %d has lock %s \n", id, key.c_str());
	}
    lc->release(GLOBAL_LOCK_LOCK);
	return ydb_protocol::OK;
}

ydb_protocol::status ydb_server_2pl::get(ydb_protocol::transaction_id id, const std::string key, std::string &out_value)
{
	// lab3: your code here
	ydb_protocol::status ret = ydb_protocol::OK;
	if ((ret = acquire(id, key)) != ydb_protocol::OK)
	{
		return ret;
	}
	transaction_entry transaction;
	find_transaction(id, transaction);
	entry_map entrymap = transaction.entrymap;
	entry_map::iterator iter = entrymap.find(key);
	if (iter != entrymap.end())
	{
		if (iter->second.kind == EDIT)
		{
			printf("edit\n");
			out_value = iter->second.value;
		}
		//TODO:delete
		else
		{
			printf("delete\n");
			out_value = "";
		}
	}
	else
	{
		ec->get(key2id(key), out_value);
	}
	printf("get: transaction:%d key:%s value:%s\n", id, key.c_str(), out_value.c_str());
	return ret;
}

ydb_protocol::status ydb_server_2pl::set(ydb_protocol::transaction_id id, const std::string key, const std::string value, int &)
{
	// lab3: your code here
	printf("set: transaction:%d key:%s value:%s \n", id, key.c_str(), value.c_str());

	ydb_protocol::status ret = ydb_protocol::OK;
	if ((ret = acquire(id, key)) != ydb_protocol::OK)
	{
		return ret;
	}

	std::map<int, transaction_entry>::iterator iter = transaction_cache.find(id);
	iter->second.entrymap[key] = op_entry(EDIT, value);
	return ret;
}

ydb_protocol::status ydb_server_2pl::del(ydb_protocol::transaction_id id, const std::string key, int &)
{
	// lab3: your code here
	printf("del: transaction:%d key:%s \n", id, key.c_str());

	ydb_protocol::status ret = ydb_protocol::OK;
	if ((ret = acquire(id, key)) != ydb_protocol::OK)
	{
		return ret;
	}

	std::map<int, transaction_entry>::iterator iter = transaction_cache.find(id);
	iter->second.entrymap[key] = op_entry(DELETE);
	return ret;
}
