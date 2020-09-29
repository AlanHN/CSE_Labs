// yfs client.  implements FS operations using extent and lock server
#include "yfs_client.h"
#include "extent_client.h"
#include <sstream>
#include <iostream>
#include <stdio.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

yfs_client::yfs_client()
{
    ec = new extent_client();
}

yfs_client::yfs_client(std::string extent_dst, std::string lock_dst)
{
    ec = new extent_client();
    if (ec->put(1, "") != extent_protocol::OK)
        printf("error init root dir\n"); // XYB: init root dir
}

yfs_client::inum
yfs_client::n2i(std::string n)
{
    std::istringstream ist(n);
    unsigned long long finum;
    ist >> finum;
    return finum;
}

std::string
yfs_client::filename(inum inum)
{
    std::ostringstream ost;
    ost << inum;
    return ost.str();
}

bool yfs_client::isfile(inum inum)
{
    extent_protocol::attr a;

    if (ec->getattr(inum, a) != extent_protocol::OK)
    {
        printf("error getting attr\n");
        return false;
    }

    if (a.type == extent_protocol::T_FILE)
    {
        printf("isfile: %lld is a file\n", inum);
        return true;
    }
    printf("isfile: %lld is not a file\n", inum);
    return false;
}
/** Your code here for Lab...
 * You may need to add routines such as
 * readlink, issymlink here to implement symbolic link.
 * 
 * */

bool yfs_client::isdir(inum inum)
{
    // Oops! is this still correct when you implement symlink?
    // return !isfile(inum);
    extent_protocol::attr a;

    if (ec->getattr(inum, a) != extent_protocol::OK)
    {
        printf("error getting attr\n");
        return false;
    }

    if (a.type == extent_protocol::T_DIR)
    {
        printf("isfile: %lld is a dir\n", inum);
        return true;
    }
    printf("isfile: %lld is not a dir\n", inum);
    return false;
}

bool yfs_client::issymlink(inum inum)
{
    extent_protocol::attr a;

    if (ec->getattr(inum, a) != extent_protocol::OK)
    {
        printf("error getting attr\n");
        return false;
    }

    if (a.type == extent_protocol::T_SYMLINK)
    {
        printf("isfile: %lld is a symlink\n", inum);
        return true;
    }
    printf("isfile: %lld is not a symlink\n", inum);
    return false;
}

int yfs_client::getfile(inum inum, fileinfo &fin)
{
    int r = OK;

    printf("getfile %016llx\n", inum);
    extent_protocol::attr a;
    if (ec->getattr(inum, a) != extent_protocol::OK)
    {
        r = IOERR;
        goto release;
    }

    fin.atime = a.atime;
    fin.mtime = a.mtime;
    fin.ctime = a.ctime;
    fin.size = a.size;
    printf("getfile %016llx -> sz %llu\n", inum, fin.size);

release:
    return r;
}

int yfs_client::getdir(inum inum, dirinfo &din)
{
    int r = OK;

    printf("getdir %016llx\n", inum);
    extent_protocol::attr a;
    if (ec->getattr(inum, a) != extent_protocol::OK)
    {
        r = IOERR;
        goto release;
    }
    din.atime = a.atime;
    din.mtime = a.mtime;
    din.ctime = a.ctime;

release:
    return r;
}

#define EXT_RPC(xx)                                                \
    do                                                             \
    {                                                              \
        if ((xx) != extent_protocol::OK)                           \
        {                                                          \
            printf("EXT_RPC Error: %s:%d \n", __FILE__, __LINE__); \
            r = IOERR;                                             \
            goto release;                                          \
        }                                                          \
    } while (0)

// Only support set size of attr
int yfs_client::setattr(inum ino, size_t size)
{
    int r = OK;

    /*
     * your code goes here.
     * note: get the content of inode ino, and modify its content
     * according to the size (<, =, or >) content length.
     */

    std::string buf;

    extent_protocol::attr a;
    printf("setattr %016llx\n", ino);

    if (extent_protocol::OK != ec->getattr(ino, a))
    {
        return IOERR;
    }
    if (extent_protocol::OK != ec->get(ino, buf))
    {
        return IOERR;
    }
    buf.resize(size);
    if (extent_protocol::OK != ec->put(ino, buf))
    {
        return IOERR;
    }

    return r;
}

int yfs_client::create(inum parent, const char *name, mode_t mode, inum &ino_out)
{
    int r = OK;

    /*
     * your code goes here.
     * note: lookup is what you need to check if file exist;
     * after create file or dir, you must remember to modify the parent infomation.
     */

    bool found = false;
    std::string buf;
    struct dirent ent;

    printf("create %016llx %s\n", parent, name);

    if (extent_protocol::OK == lookup(parent, name, found, ino_out) && found)
    {
        printf("file exist\n");
        return EXIST;
    }

    if (extent_protocol::OK != ec->create(extent_protocol::T_FILE, ino_out))
    {
        return IOERR;
    }

    if (extent_protocol::OK != ec->get(parent, buf))
    {
        return IOERR;
    }

    buf.append(std::string(name) + ":" + filename(ino_out) + "/");
    if (extent_protocol::OK != ec->put(parent, buf))
    {
        return IOERR;
    }

    return r;
}

int yfs_client::mkdir(inum parent, const char *name, mode_t mode, inum &ino_out)
{
    int r = OK;

    /*
     * your code goes here.
     * note: lookup is what you need to check if directory exist;
     * after create file or dir, you must remember to modify the parent infomation.
     */
    bool found = false;
    std::string buf;
    struct dirent ent;

    printf("create %016llx %s\n", parent, name);

    if (extent_protocol::OK == lookup(parent, name, found, ino_out) && found)
    {
        printf("dir exist\n");
        return EXIST;
    }

    if (extent_protocol::OK != ec->create(extent_protocol::T_DIR, ino_out))
    {
        return IOERR;
    }

    if (extent_protocol::OK != ec->get(parent, buf))
    {
        return IOERR;
    }

    buf.append(std::string(name) + ":" + filename(ino_out) + "/");
    if (extent_protocol::OK != ec->put(parent, buf))
    {
        return IOERR;
    }

    return r;
}

int yfs_client::lookup(inum parent, const char *name, bool &found, inum &ino_out)
{
    int r = OK;

    /*
     * your code goes here.
     * note: lookup file from parent dir according to name;
     * you should design the format of directory content.
     */

    /*
     * directory content:
     * name:inum/name:inum/.../
     */

    std::list<dirent> list;
    extent_protocol::attr a;

    printf("lookup %016llx %s\n", parent, name);
    if (extent_protocol::OK != ec->getattr(parent, a))
    {
        return IOERR;
    }

    if (a.type != extent_protocol::T_DIR)
    {
        printf("not a dir %016llx %s\n", parent, name);
        return NOENT;
    }

    readdir(parent, list);

    std::list<dirent>::iterator it;
    for (it = list.begin(); it != list.end(); it++)
    {
        dirent ent = *it;
        if (ent.name == std::string(name))
        {
            found = true;
            ino_out = ent.inum;
            return r;
        }
    }

    found = false;

    return r;
}

int yfs_client::readdir(inum dir, std::list<dirent> &list)
{
    int r = OK;

    /*
     * your code goes here.
     * note: you should parse the dirctory content using your defined format,
     * and push the dirents to the list.
     */

    /*
     * directory content:
     * name:inum/name:inum/.../
     */

    std::string buf;
    extent_protocol::attr a;
    struct dirent ent_buf;

    printf("readdir %016llx\n", dir);
    if (extent_protocol::OK != ec->getattr(dir, a))
    {
        return IOERR;
    }
    if (extent_protocol::T_DIR != a.type)
    {
        printf("not a dir %016llx\n", dir);
        return NOENT;
    }
    if (extent_protocol::OK != ec->get(dir, buf))
    {
        return IOERR;
    }

    size_t start = 0;
    size_t end = buf.find(':');
    while (end != std::string::npos)
    {
        std::string name = buf.substr(start, end - start);
        start = end + 1;
        end = buf.find('/', start);
        std::string inum = buf.substr(start, end - start);

        struct dirent entry;
        entry.name = name;
        entry.inum = n2i(inum);

        list.push_back(entry);

        start = end + 1;
        end = buf.find(':', start);
    }
    return r;
}

int yfs_client::read(inum ino, size_t size, off_t off, std::string &data)
{
    int r = OK;

    /*
     * your code goes here.
     * note: read using ec->get().
     */
    std::string buf;

    printf("read %016llx sz %ld off %ld\n", ino, size, off);
    if (extent_protocol::OK != ec->get(ino, buf))
    {
        return IOERR;
    }

    if ((uint32_t)off >= buf.size())
        return r;

    if (size > buf.size())
        size = buf.size();
    data.assign(buf, off, size);
    return r;
}

int yfs_client::write(inum ino, size_t size, off_t off, const char *data,
                      size_t &bytes_written)
{
    int r = OK;

    /*
     * your code goes here.
     * note: write using ec->put().
     * when off > length of original file, fill the holes with '\0'.
     */

    std::string buf;
    printf("write %016llx sz %ld off %ld\n", ino, size, off);
    if (size == 0 || off < 0)
    {
        return IOERR;
    }
    if (extent_protocol::OK != ec->get(ino, buf))
    {
        return IOERR;
    }

    if (off + size < buf.size())
    {
        for (size_t i = 0; i < size; i++)
        {
            buf[off + i] = data[i];
        }
    }
    else
    {
        buf.resize(off + size);
        for (size_t i = 0; i < size; i++)
        {
            buf[off + i] = data[i];
        }
    }
    bytes_written = size;
    ec->put(ino, buf);
    return r;
}

int yfs_client::unlink(inum parent, const char *name)
{
    int r = OK;

    /*
     * your code goes here.
     * note: you should remove the file using ec->remove,
     * and update the parent directory content.
     */

    inum ino;
    std::string buf;
    bool found = false;
    if (extent_protocol::OK != lookup(parent, name, found, ino) || !found)
    {
        printf("dir not exist\n");
        return NOENT;
    }
    if (extent_protocol::OK != ec->remove(ino))
    {
        printf("can't remove\n");
        return IOERR;
    }

    if (extent_protocol::OK != ec->get(parent, buf))
    {
        return IOERR;
    }
    uint32_t start = buf.find(name);
    uint32_t end = buf.find('/', start);
    buf.erase(start, end - start + 1);
    if (extent_protocol::OK != ec->put(parent, buf))
    {
        return IOERR;
    }

    return r;
}

int yfs_client::symlink(inum parent, const char *name, const char *link, inum &ino_out)
{
    int r = OK;
    inum ino;
    bool found = false;
    std::string buf;
    struct dirent ent;

    printf("create %016llx %s\n", parent, name);

    if (extent_protocol::OK == lookup(parent, name, found, ino) && found)
    {
        printf("symlink exist\n");
        return EXIST;
    }

    if (extent_protocol::OK != ec->create(extent_protocol::T_SYMLINK, ino_out))
    {
        return IOERR;
    }

    if (extent_protocol::OK != ec->put(ino_out, std::string(link)))
    {
        return IOERR;
    }

    if (extent_protocol::OK != ec->get(parent, buf))
    {
        return IOERR;
    }
    buf.append(std::string(name) + ":" + filename(ino_out) + "/");
    if (extent_protocol::OK != ec->put(parent, buf))
    {
        return IOERR;
    }

    return r;
}

int yfs_client::readlink(inum ino, std::string &data)
{
    int r = OK;

    if (extent_protocol::OK != ec->get(ino, data))
    {
        return IOERR;
    }

    return r;
}