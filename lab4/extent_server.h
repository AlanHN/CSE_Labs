// this is the extent server

#ifndef extent_server_h
#define extent_server_h

#include <string>
#include <map>
#include <set>
#include "extent_protocol.h"
#include "inode_manager.h"

#ifndef USE_EXTENT_CACHE 
class extent_server
{
protected:
#if 0
  typedef struct extent {
    std::string data;
    struct extent_protocol::attr attr;
  } extent_t;
  std::map <extent_protocol::extentid_t, extent_t> extents;
#endif
  inode_manager *im;

public:
  extent_server();

  int create(uint32_t type, extent_protocol::extentid_t &id);
  int put(extent_protocol::extentid_t id, std::string, int &);
  int get(extent_protocol::extentid_t id, std::string &);
  int getattr(extent_protocol::extentid_t id, extent_protocol::attr &);
  int remove(extent_protocol::extentid_t id, int &);
};



#else
class extent_server
{
private:
  inode_manager *im;  
  struct fileinfo {
    std::string working_cid;
    std::set<std::string> cached_cids;
    fileinfo(){}
  };

  std::map<extent_protocol::extentid_t, fileinfo *> filemap; 
  void notify(extent_protocol::extentid_t id, std::string cid);
  void require(extent_protocol::extentid_t id, std::string cid);

public:
  extent_server();
  ~extent_server();
  int create(uint32_t type,std::string cid, extent_protocol::extentid_t &id);
  int put(extent_protocol::extentid_t id, std::string cid, std::string, int &);
  int get(extent_protocol::extentid_t id, std::string cid, std::string &);
  int getattr(extent_protocol::extentid_t id, std::string cid, extent_protocol::attr &);
  int remove(extent_protocol::extentid_t id, std::string cid, int &);
};

#endif
#endif
