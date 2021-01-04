// RPC stubs for clients to talk to lock_server, and cache the locks
// see lock_client.cache.h for protocol details.

#include "lock_client_cache.h"
#include "rpc.h"
#include <sstream>
#include <iostream>
#include <stdio.h>
#include "tprintf.h"

pthread_mutex_t lock_client_cache::mutex = PTHREAD_MUTEX_INITIALIZER;

int lock_client_cache::last_port = 0;

lock_client_cache::lock_client_cache(std::string xdst,
                                     class lock_release_user *_lu)
    : lock_client(xdst), lu(_lu)
{
  srand(time(NULL) ^ last_port);
  rlock_port = ((rand() % 32000) | (0x1 << 10));
  const char *hname;
  // VERIFY(gethostname(hname, 100) == 0);
  hname = "127.0.0.1";
  std::ostringstream host;
  host << hname << ":" << rlock_port;
  id = host.str();
  last_port = rlock_port;
  rpcs *rlsrpc = new rpcs(rlock_port);
  rlsrpc->reg(rlock_protocol::revoke, this, &lock_client_cache::revoke_handler);
  rlsrpc->reg(rlock_protocol::retry, this, &lock_client_cache::retry_handler);
}

lock_protocol::status
lock_client_cache::acquire(lock_protocol::lockid_t lid)
{
  int ret = lock_protocol::OK;
  // Your lab2 part3 code goes here
  // printf("acquire %lld\n", lid);
  pthread_mutex_lock(&mutex);
  bool doacquire = false;

  // Not cached
  if (lock_cache_status.find(lid) == lock_cache_status.end())
  {
    doacquire = true;
    status_entry entry;
    entry.status = NONE;
    entry.cond = PTHREAD_COND_INITIALIZER;
    entry.condForRetry = PTHREAD_COND_INITIALIZER;
    entry.revoke = false;
    entry.retry = false;
    lock_cache_status[lid] = entry;
  }

  switch (lock_cache_status[lid].status)
  {
  case NONE:
  {
    doacquire = true;
    lock_cache_status[lid].status = ACQUIRING;
    break;
  }
  case FREE:
  {
    lock_cache_status[lid].status = LOCKED;
    break;
  }
  default:
    while (lock_cache_status[lid].status == LOCKED ||
           lock_cache_status[lid].status == ACQUIRING ||
           lock_cache_status[lid].status == RELEASING)
    {
      pthread_cond_wait(&lock_cache_status[lid].cond, &mutex);
    }
    switch (lock_cache_status[lid].status)
    {
    case NONE:
    {
      doacquire = true;
      lock_cache_status[lid].status = ACQUIRING;
      break;
    }
    case FREE:
    {
      lock_cache_status[lid].status = LOCKED;
      break;
    }
    default:
      break;
    }
    break;
  }

  if (doacquire == true)
  {
    while (lock_cache_status[lid].retry == false)
    {
      int r = -1;
      pthread_mutex_unlock(&mutex);
      ret = cl->call(lock_protocol::acquire, lid, id, r);
      pthread_mutex_lock(&mutex);
      // need RETRY
      if (ret == lock_protocol::RETRY)
      {
        while (lock_cache_status[lid].retry == false)
        {
          pthread_cond_wait(&lock_cache_status[lid].condForRetry, &mutex);
        }
        lock_cache_status[lid].retry = false;
      }
      //get lock
      else if (ret == lock_protocol::OK)
      {
        lock_cache_status[lid].status = LOCKED;
        break;
      }
    }
  }
  pthread_mutex_unlock(&mutex);
  return ret;
}

lock_protocol::status
lock_client_cache::release(lock_protocol::lockid_t lid)
{
  int ret = lock_protocol::OK;
  // Your lab2 part3 code goes here
  // printf("release %lld\n", lid);
  pthread_mutex_lock(&mutex);
  //Not cached
  if (lock_cache_status.find(lid) == lock_cache_status.end())
  {
    // printf("No lock\n");
  }

  // revoke
  else if (lock_cache_status[lid].revoke == true)
  {
    lock_cache_status[lid].revoke = false;
    int r = -1;
    pthread_mutex_unlock(&mutex);
    ret = cl->call(lock_protocol::release, lid, id, r);
    pthread_mutex_lock(&mutex);
    lock_cache_status[lid].status = NONE;
    pthread_cond_signal(&lock_cache_status[lid].cond);
  }
  else
  {
    switch (lock_cache_status[lid].status)
    {
    case LOCKED:
      lock_cache_status[lid].status = FREE;
      pthread_cond_signal(&lock_cache_status[lid].cond);
      break;
    case FREE:
      break;
    case RELEASING:
    {
      int r = -1;
      pthread_mutex_unlock(&mutex);
      ret = cl->call(lock_protocol::release, lid, id, r);
      pthread_mutex_lock(&mutex);
      lock_cache_status[lid].status = NONE;
      pthread_cond_signal(&lock_cache_status[lid].cond);
      break;
    }
    default:
      break;
    }
  }
  pthread_mutex_unlock(&mutex);
  return ret;
}

rlock_protocol::status
lock_client_cache::revoke_handler(lock_protocol::lockid_t lid,
                                  int &)
{
  int ret = rlock_protocol::OK;
  // Your lab2 part3 code goes here
  pthread_mutex_lock(&mutex);
  if (lock_cache_status.find(lid) == lock_cache_status.end())
  {
    // printf("nothing to revoke");
  }
  switch (lock_cache_status[lid].status)
  {
  case NONE:
  case RELEASING:
  case ACQUIRING:
    lock_cache_status[lid].revoke = true;
    break;
  case FREE:
  {
    lock_cache_status[lid].status = RELEASING;
    int r = -1;
    pthread_mutex_unlock(&mutex);
    ret = cl->call(lock_protocol::release, lid, id, r);
    pthread_mutex_lock(&mutex);
    lock_cache_status[lid].status = NONE;
    pthread_cond_signal(&lock_cache_status[lid].cond);
    break;
  }
  case LOCKED:
    lock_cache_status[lid].status = RELEASING;
    break;
  default:
    break;
  }
  pthread_mutex_unlock(&mutex);
  return ret;
}

rlock_protocol::status
lock_client_cache::retry_handler(lock_protocol::lockid_t lid,
                                 int &)
{
  int ret = rlock_protocol::OK;
  // Your lab2 part3 code goes here
  pthread_mutex_lock(&mutex);
  pthread_cond_signal(&lock_cache_status[lid].condForRetry);
  lock_cache_status[lid].retry = true;
  pthread_mutex_unlock(&mutex);
  return ret;
}
