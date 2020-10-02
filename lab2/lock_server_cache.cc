// the caching lock server implementation

#include "lock_server_cache.h"
#include <sstream>
#include <stdio.h>
#include <unistd.h>
#include <arpa/inet.h>
#include "lang/verify.h"
#include "handle.h"
#include "tprintf.h"

pthread_mutex_t lock_server_cache::mutex = PTHREAD_MUTEX_INITIALIZER;

lock_server_cache::lock_server_cache() : nacquire(0)
{
}

int lock_server_cache::acquire(lock_protocol::lockid_t lid, std::string id, int &)
{
  lock_protocol::status ret = lock_protocol::OK;
  // Your lab2 part3 code goes here
  pthread_mutex_lock(&mutex);
  bool dorevoke = false;

  if (lock_cache_status.find(lid) == lock_cache_status.end())
  {
    status_entry entry;
    entry.clientId = id;
    entry.retryId = "";
    entry.status = LOCKED;
    lock_cache_status[lid] = entry;
  }
  else
  {
    switch (lock_cache_status[lid].status)
    {
    case FREE:
    {
      lock_cache_status[lid].status = LOCKED;
      lock_cache_status[lid].clientId = id;
      break;
    }
    case LOCKED:
    {

      lock_cache_status[lid].status = WAITING;
      lock_cache_status[lid].clientSet.insert(id);
      lock_cache_status[lid].retryId = id;
      ret = lock_protocol::RETRY;
      dorevoke = true;
      break;
    }
    case WAITING:
    {

      if (lock_cache_status[lid].clientSet.find(lock_cache_status[lid].retryId) == lock_cache_status[lid].clientSet.end())
      {
        ret = lock_protocol::RETRY;
        lock_cache_status[lid].clientSet.insert(id);
      }
      else
      {
        if (lock_cache_status[lid].retryId == id)
        {
          lock_cache_status[lid].clientId = id;
          lock_cache_status[lid].clientSet.erase(lock_cache_status[lid].clientSet.find(lock_cache_status[lid].retryId));
          if (lock_cache_status[lid].clientSet.empty() == true)
          {
            lock_cache_status[lid].retryId = "";
            lock_cache_status[lid].status = LOCKED;
          }
          else
          {
            lock_cache_status[lid].retryId = *(lock_cache_status[lid].clientSet.begin());
            lock_cache_status[lid].status = WAITING;
            dorevoke = true;
          }
        }
        else
        {
          ret = lock_protocol::RETRY;
          lock_cache_status[lid].clientSet.insert(id);
        }
      }
      break;
    }
    default:
      break;
    }
  }
  if (dorevoke)
  {
    handle h(lock_cache_status[lid].clientId);
    rpcc *cl = h.safebind();
    if (cl)
    {
      pthread_mutex_unlock(&mutex);
      int r = cl->call(rlock_protocol::revoke, lid, r);
      pthread_mutex_lock(&mutex);
      if (r != rlock_protocol::OK)
  		{
  			printf("rlock error\n");
  		}
    }
    else
    {
      printf("Bind error.\n");
    }
  }
  pthread_mutex_unlock(&mutex);
  return ret;
}

int lock_server_cache::release(lock_protocol::lockid_t lid, std::string id, int &r)
{
  lock_protocol::status ret = lock_protocol::OK;
  // Your lab2 part3 code goes here
  pthread_mutex_lock(&mutex);
  bool doretry = false;

  if (lock_cache_status.find(lid) == lock_cache_status.end())
  {
    pthread_mutex_unlock(&mutex);
    return ret;
  }
  else if (lock_cache_status[lid].clientId != id)
  {
    pthread_mutex_unlock(&mutex);
    return ret;
  }
  else
  {
    switch (lock_cache_status[lid].status)
    {
    case FREE:
      pthread_mutex_unlock(&mutex);
      return ret;
    case LOCKED:
      lock_cache_status[lid].status = FREE;
      lock_cache_status[lid].clientId = "";
      break;
    case WAITING:
      lock_cache_status[lid].clientId = "";
      doretry = true;
      break;
    default:
      break;
    }
  }
  if (doretry)
  {
    handle h(lock_cache_status[lid].retryId);
    rpcc *cl = h.safebind();
    if (cl)
    {
      pthread_mutex_unlock(&mutex);
      int r = cl->call(rlock_protocol::retry, lid, r);
      pthread_mutex_lock(&mutex);
      if (r != rlock_protocol::OK)
  		{
  			printf("rlock error\n");
  		}
    }
    else
    {
      printf("Bind error.\n");
    }
  }
  pthread_mutex_unlock(&mutex);
  return ret;
}

lock_protocol::status
lock_server_cache::stat(lock_protocol::lockid_t lid, int &r)
{
  tprintf("stat request\n");
  r = nacquire;
  return lock_protocol::OK;
}
