// the extent server implementation

#include "extent_server.h"
#include <sstream>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include "handle.h"

#ifndef USE_EXTENT_CACHE
extent_server::extent_server()
{
  im = new inode_manager();
}

int extent_server::create(uint32_t type, extent_protocol::extentid_t &id)
{
  // alloc a new inode and return inum
  printf("extent_server: create inode\n");
  id = im->alloc_inode(type);

  return extent_protocol::OK;
}

int extent_server::put(extent_protocol::extentid_t id, std::string buf, int &)
{
  id &= 0x7fffffff;

  const char *cbuf = buf.c_str();
  int size = buf.size();
  im->write_file(id, cbuf, size);

  return extent_protocol::OK;
}

int extent_server::get(extent_protocol::extentid_t id, std::string &buf)
{
  printf("extent_server: get %lld\n", id);

  id &= 0x7fffffff;

  int size = 0;
  char *cbuf = NULL;

  im->read_file(id, &cbuf, &size);
  if (size == 0)
    buf = "";
  else
  {
    buf.assign(cbuf, size);
    free(cbuf);
  }

  return extent_protocol::OK;
}

int extent_server::getattr(extent_protocol::extentid_t id, extent_protocol::attr &a)
{
  printf("extent_server: getattr %lld\n", id);

  id &= 0x7fffffff;

  extent_protocol::attr attr;
  memset(&attr, 0, sizeof(attr));
  im->getattr(id, attr);
  a = attr;

  return extent_protocol::OK;
}

int extent_server::remove(extent_protocol::extentid_t id, int &)
{
  printf("extent_server: write %lld\n", id);

  id &= 0x7fffffff;
  im->remove_file(id);

  return extent_protocol::OK;
}

#else

extent_server::extent_server()
{
  // printf("initial extent_server\n");
  im = new inode_manager();
  filemap[1] = new fileinfo();
}

extent_server::~extent_server()
{
  for (std::map<extent_protocol::extentid_t, fileinfo *>::iterator it = filemap.begin(); it != filemap.end(); it++)
    delete it->second;
}

int extent_server::create(uint32_t type, std::string cid, extent_protocol::extentid_t &id)
{
  // printf("extent_server: create\n");
  // alloc a new inode and return inum
  id = im->alloc_inode(type);

  filemap[id] = new fileinfo();
  filemap[id]->cached_cids.insert(cid);
  return extent_protocol::OK;
}

int extent_server::put(extent_protocol::extentid_t id, std::string cid, std::string buf, int &)
{
  // printf("extent_server: put\n");
  id &= 0x7fffffff;

  const char *cbuf = buf.c_str();
  int size = buf.size();
  im->write_file(id, cbuf, size);

  filemap[id]->cached_cids.insert(cid);
  filemap[id]->working_cid = cid;
  notify(id, cid);
  return extent_protocol::OK;
}

int extent_server::get(extent_protocol::extentid_t id, std::string cid, std::string &buf)
{
  // printf("extent_server: get\n");
  id &= 0x7fffffff;

  int size = 0;
  char *cbuf = NULL;

  require(id, cid);
  im->read_file(id, &cbuf, &size);
  if (size == 0)
    buf = "";
  else
  {
    buf.assign(cbuf, size);
    free(cbuf);
  }

  filemap[id]->cached_cids.insert(cid);
  if (!filemap[id]->working_cid.empty())
    notify(id, cid);
  return extent_protocol::OK;
}

int extent_server::getattr(extent_protocol::extentid_t id, std::string cid, extent_protocol::attr &a)
{
  // printf("extent_server: getattr\n");
  id &= 0x7fffffff;
  extent_protocol::attr attr;
  memset(&attr, 0, sizeof(attr));

  require(id, cid);
  im->getattr(id, attr);
  a = attr;

  filemap[id]->cached_cids.insert(cid);
  if (!filemap[id]->working_cid.empty())
    notify(id, cid);
  return extent_protocol::OK;
}

int extent_server::remove(extent_protocol::extentid_t id, std::string cid, int &)
{
  // printf("extent_server: remove\n");
  id &= 0x7fffffff;

  im->remove_file(id);

  notify(id, cid);
  delete filemap[id];
  filemap.erase(filemap.find(id));
  return extent_protocol::OK;
}

void extent_server::notify(extent_protocol::extentid_t id, std::string cid)
{
  int r;

  if (!filemap[id])
    return;
  if (filemap[id]->cached_cids.empty())
    return;

  for (std::set<std::string>::iterator it = filemap[id]->cached_cids.begin(); it != filemap[id]->cached_cids.end(); it++)
    if (cid != *it)
    {
      if (*it == filemap[id]->working_cid)
        filemap[id]->working_cid.clear();
      handle(*it).safebind()->call(extent_protocol::pull, id, r);
    }
}

void extent_server::require(extent_protocol::extentid_t id, std::string cid)
{
  int r;

  if (!filemap[id])
    return;
  if (filemap[id]->working_cid.empty())
    return;

  handle(filemap[id]->working_cid).safebind()->call(extent_protocol::push, id, r);
}
#endif