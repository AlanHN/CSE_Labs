#include "inode_manager.h"

// disk layer -----------------------------------------

disk::disk()
{
  bzero(blocks, sizeof(blocks));
}

void disk::read_block(blockid_t id, char *buf)
{
  memcpy(buf, blocks[id], BLOCK_SIZE);
}

void disk::write_block(blockid_t id, const char *buf)
{
  memcpy(blocks[id], buf, BLOCK_SIZE);
}

// block layer -----------------------------------------

// Allocate a free disk block.
blockid_t
block_manager::alloc_block()
{
  /*
   * your code goes here.
   * note: you should mark the corresponding bit in block bitmap when alloc.
   * you need to think about which block you can start to be allocated.
   */
  for (int i = BITMAP_START; i < BLOCK_NUM; i++)
  {
    if (!using_blocks[i])
    {
      using_blocks[i] = 1;
      return i;
    }
  }
  printf("\tbm[alloc_block]:no free block\n");
  return 0;
}

void block_manager::free_block(uint32_t id)
{
  /* 
   * your code goes here.
   * note: you should unmark the corresponding bit in the block bitmap when free.
   */
  using_blocks[id] = 0;
  return;
}

// The layout of disk should be like this:
// |<-sb->|<-free block bitmap->|<-inode table->|<-data->|
block_manager::block_manager()
{
  d = new disk();

  sb.size = BLOCK_SIZE * BLOCK_NUM;
  sb.nblocks = BLOCK_NUM;
  sb.ninodes = INODE_NUM;
  // format the disk
  for (int i = 0; i <BITMAP_START; i++)
  {
    using_blocks[i] = 1;
  }
  d->write_block(1, (char *)&sb);
}

void block_manager::read_block(uint32_t id, char *buf)
{
  d->read_block(id, buf);
}

void block_manager::write_block(uint32_t id, const char *buf)
{
  d->write_block(id, buf);
}

// inode layer -----------------------------------------

inode_manager::inode_manager()
{
  bm = new block_manager();
  uint32_t root_dir = alloc_inode(extent_protocol::T_DIR);
  if (root_dir != 1)
  {
    printf("\tim: error! alloc first inode %d, should be 1\n", root_dir);
    exit(0);
  }
  using_inodes[0]= using_inodes[1] = 1;
  bm->sb.next_free_inode=2;
}

/* Create a new file.
 * Return its inum. */
uint32_t
inode_manager::alloc_inode(uint32_t type)
{
  /* 
   * your code goes here.
   * note: the normal inode block should begin from the 2nd inode block.
   * the 1st is used for root_dir, see inode_manager::inode_manager().
   */
  for (int i = 1; i < INODE_NUM; i++)
  {
    if (using_inodes[i] == 0)
    {
      struct inode *ino = (struct inode *)malloc(sizeof(struct inode));
      bzero(ino, sizeof(struct inode));
      ino->type = type;
      ino->size = 0;
      ino->atime = time(0);
      ino->mtime = time(0);
      ino->ctime = time(0);
      put_inode(i, ino);
      free(ino);
      using_inodes[i] = 1;
      return i;
    }
  }
  printf("\t im: no inode available");
  return 1;
}

void inode_manager::free_inode(uint32_t inum)
{
  /* 
   * your code goes here.
   * note: you need to check if the inode is already a freed one;
   * if not, clear it, and remember to write back to disk.
   */
   
  if (using_inodes[inum])
  {
    struct inode *ino = get_inode(inum);
    // bzero((char *)ino, sizeof(struct inode));
    ino->type = ino->size = ino->atime = ino->ctime = ino->mtime = 0;
    put_inode(inum, ino);
    free(ino);
    using_inodes[inum] = 0;
  }
  return;
}

/* Return an inode structure by inum, NULL otherwise.
 * Caller should release the memory. */
struct inode *
inode_manager::get_inode(uint32_t inum)
{
  struct inode *ino, *ino_disk;
  char buf[BLOCK_SIZE];

  //printf("\tim: get_inode %d\n", inum);

  if (inum < 0 || inum >= INODE_NUM)
  {
    printf("\tim: inum out of range\n");
    return NULL;
  }

  bm->read_block(IBLOCK(inum, bm->sb.nblocks), buf);
  // printf("%s:%d\n", __FILE__, __LINE__);

  ino_disk = (struct inode *)buf + inum % IPB;
  if (ino_disk->type == 0)
  {
    printf("\tim: inode not exist\n");
    return NULL;
  }

  ino = (struct inode *)malloc(sizeof(struct inode));
  *ino = *ino_disk;

  return ino;
}

void inode_manager::put_inode(uint32_t inum, struct inode *ino)
{
  char buf[BLOCK_SIZE];
  struct inode *ino_disk;

  //printf("\tim: put_inode %d\n", inum);
  if (ino == NULL)
    return;

  bm->read_block(IBLOCK(inum, bm->sb.nblocks), buf);
  ino_disk = (struct inode *)buf + inum % IPB;
  *ino_disk = *ino;
  bm->write_block(IBLOCK(inum, bm->sb.nblocks), buf);
}

#define MIN(a, b) ((a) < (b) ? (a) : (b))

//获得inode中第n个block的id,返回block的索引
blockid_t
inode_manager::get_n_block(inode_t *ino, uint32_t n)
{

  char buf[BLOCK_SIZE];
  blockid_t index;

  if (n < NDIRECT)
    index = ino->blocks[n];
  else if (n < MAXFILE)
  {
    if (!ino->blocks[NDIRECT])
    {
      printf("\tim:none NINDIRECT!\n");
    }
    bm->read_block(ino->blocks[NDIRECT], buf);

    index = ((blockid_t *)buf)[n - NDIRECT];
  }
  else
  {
    printf("\tim: out of range\n");
    exit(0);
  }

  return index;
}

void inode_manager::alloc_n_block(inode_t *ino, uint32_t n)
{
  char buf[BLOCK_SIZE];

  if (n < NDIRECT)
    ino->blocks[n] = bm->alloc_block();
  else if (n < MAXFILE)
  {
    if (!ino->blocks[NDIRECT])
    {
      printf("\tim: new NINDIRECT!\n");
      ino->blocks[NDIRECT] = bm->alloc_block();
    }
    bm->read_block(ino->blocks[NDIRECT], buf);
    ((blockid_t *)buf)[n - NDIRECT] = bm->alloc_block();
    bm->write_block(ino->blocks[NDIRECT], buf);
  }
  else
  {
    printf("\tim:out of range\n");
    exit(0);
  }
}

/* Get all the data of a file by inum. 
 * Return alloced data, should be freed by caller. */
void inode_manager::read_file(uint32_t inum, char **buf_out, int *size)
{
  /*
   * your code goes here.
   * note: read blocks related to inode number inum,
   * and copy them to buf_Out
   */
  int block_num = 0;
  int remain_size = 0;
  char buf[BLOCK_SIZE];
  int i = 0;

  // printf("\tim: read_file %d\n", inum);
  inode_t *ino = get_inode(inum);
  if (ino)
  {
    *size = ino->size;
    *buf_out = (char *)malloc(*size);

    block_num = *size / BLOCK_SIZE;
    remain_size = *size % BLOCK_SIZE;
    while (i < block_num)
    {
      bm->read_block(get_n_block(ino, i), buf);
      memcpy(*buf_out + i * BLOCK_SIZE, buf, BLOCK_SIZE);
      i++;
    }
    if (remain_size)
    {
      bm->read_block(get_n_block(ino, i), buf);
      memcpy(*buf_out + i * BLOCK_SIZE, buf, remain_size);
    }
    free(ino);
  }

  return;
}

/* alloc/free blocks if needed */
void inode_manager::write_file(uint32_t inum, const char *buf, int size)
{
  /*
   * your code goes here.
   * note: write buf to blocks of inode inum.
   * you need to consider the situation when the size of buf 
   * is larger or smaller than the size of original inode
   */
  int block_num = 0;
  int remain_size = 0;
  int old_blocks_num, new_blocks_num;
  char temp[BLOCK_SIZE];
  int i = 0;
  inode_t *ino = get_inode(inum);
  if (ino)
  {
    old_blocks_num = ino->size == 0 ? 0 : (ino->size - 1) / BLOCK_SIZE + 1;
    new_blocks_num = size == 0 ? 0 : (size - 1) / BLOCK_SIZE + 1;

    if (old_blocks_num < new_blocks_num)
    {
      for (int j = old_blocks_num; j < new_blocks_num; j++)
      {
        alloc_n_block(ino, j);
      }
    }
    else if (old_blocks_num > new_blocks_num)
    {
      for (int j = new_blocks_num; j < old_blocks_num; j++)
      {
        bm->free_block(get_n_block(ino, j));
      }
    }

    block_num = size / BLOCK_SIZE;
    remain_size = size % BLOCK_SIZE;

    while (i < block_num)
    {
      bm->write_block(get_n_block(ino, i), buf + i * BLOCK_SIZE);
      i++;
    }

    if (remain_size)
    {
      memcpy(temp, buf + i * BLOCK_SIZE, remain_size);
      bm->write_block(get_n_block(ino, i), temp);
    }
    ino->size = size;
    ino->atime = time(0);
    ino->mtime = time(0);
    ino->ctime = time(0);
    put_inode(inum, ino);
    free(ino);
  }

  return;
}

void inode_manager::getattr(uint32_t inum, extent_protocol::attr &a)
{
  /*
   * your code goes here.
   * note: get the attributes of inode inum.
   * you can refer to "struct attr" in extent_protocol.h
   */
  // printf("\tim: getattr %d\n", inum);
  inode_t *ino = get_inode(inum);
  if (!ino)
  {
    return;
  }
  a.type = ino->type;
  a.atime = ino->atime;
  a.mtime = ino->mtime;
  a.ctime = ino->ctime;
  a.size = ino->size;
  free(ino);
  return;
}

void inode_manager::remove_file(uint32_t inum)
{
  /*
   * your code goes here
   * note: you need to consider about both the data block and inode of the file
   */

  printf("\tim: remove_file %d\n", inum);
  inode_t *ino = get_inode(inum);
  size_t size = ino->size;
  int block_num = size == 0 ? 0 : (size - 1) / BLOCK_SIZE + 1;

  for (int i = 0; i < block_num; i++)
  {
    bm->free_block(get_n_block(ino, i));
  }
  if (block_num > NDIRECT)
  {
    bm->free_block(ino->blocks[NDIRECT]);
  }
  bzero(ino, sizeof(inode_t));
  free_inode(inum);
  free(ino);

  return;
}
