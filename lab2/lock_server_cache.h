#ifndef lock_server_cache_h
#define lock_server_cache_h

#include <string>
#include <map>
#include <set>
#include "lock_protocol.h"
#include "rpc.h"
#include "lock_server.h"

class lock_server_cache
{
private:
  int nacquire;
  enum lock_server_cache_status
  {
    FREE,
    LOCKED,
    WAITING
  };
  struct status_entry
  {
  public:
    status_entry(){};
    ~status_entry(){};
    std::string clientId;
    std::string retryId;
    std::set<std::string> clientSet;
    lock_server_cache_status status;
  };
  std::map<lock_protocol::lockid_t, status_entry> lock_cache_status;

  static pthread_mutex_t mutex;

public:
  lock_server_cache();
  lock_protocol::status stat(lock_protocol::lockid_t, int &);
  int acquire(lock_protocol::lockid_t, std::string id, int &);
  int release(lock_protocol::lockid_t, std::string id, int &);
};

#endif
