#include "inode_manager.h"

// disk layer -----------------------------------------

disk::disk()
{
  bzero(blocks, sizeof(blocks));
}

void disk::read_block(blockid_t id, char *buf)
{
  if (id < 0 || id >= BLOCK_NUM || buf == NULL)
    return;

  memcpy(buf, blocks[id], BLOCK_SIZE);
}

void disk::write_block(blockid_t id, const char *buf)
{
  if (id < 0 || id >= BLOCK_NUM || buf == NULL)
    return;

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
  uint32_t data_block_id = IBLOCK(sb.ninodes, sb.nblocks) + 1;
  for (uint32_t i = data_block_id; i < BLOCK_NUM; i++)
  {
    if (using_blocks[i] != 1)
    {
      using_blocks[i] = 1;

      // Write to disk
      // char buf[BLOCK_SIZE];
      // read_block(BBLOCK(i), buf);
      // uint32_t bit = i % BPB;
      // char *bits = &(buf)[bit / 8];
      // *bits |= (1 << (bit % 8));
      // d->write_block(BBLOCK(i), buf);
      return i;
    }
  }
  printf("\tbm[alloc_block]: no free block\n");
  return 0;
}

void block_manager::free_block(uint32_t id)
{
  /* 
   * your code goes here.
   * note: you should unmark the corresponding bit in the block bitmap when free.
   */
  if (id < 0 || id >= BLOCK_NUM)
  {
    printf("\tbm[free_block]: blockid out of range %d\n", id);
    return;
  }
  if (using_blocks[id] == 0)
  {
    printf("\tbm[free_block]: free again %d\n", id);
    return;
  }
  using_blocks[id] = 0;

  // Write to disk
  // char buf[BLOCK_SIZE];
  // read_block(BBLOCK(id), buf);
  // uint32_t bit = id % BPB;
  // char *bits = &(buf)[bit / 8];
  // *bits &= ~(1 << (bit % 8));
  // d->write_block(BBLOCK(id), buf);
  return;
}

// The layout of disk should be like this:
// |<-sb->|<-free block bitmap->|<-inode table->|<-data->|
block_manager::block_manager()
{
  d = new disk();

  // format the disk
  uint32_t data_block_id = IBLOCK(sb.ninodes, sb.nblocks) + 1;
  for (uint32_t i = 0; i < data_block_id; i++)
  {
    using_blocks[i] = 1;
  }
  sb.size = DISK_SIZE;
  sb.nblocks = BLOCK_NUM;
  sb.ninodes = INODE_NUM;
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
  using_inodes[1]=1;
  if (root_dir != 1)
  {
    printf("\tim: error! alloc first inode %d, should be 1\n", root_dir);
    exit(0);
  }
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
  for (uint32_t i = 1; i < INODE_NUM; i++)
  {
    if (using_inodes[i]==0)
    {
      struct inode * ino = (struct inode *)malloc(sizeof(struct inode));
      bzero(ino, sizeof(struct inode));
      ino->type = type;
      ino->size = 0;
      ino->atime = time(0);
      ino->mtime = time(0);
      ino->ctime = time(0);
      put_inode(i, ino);
      using_inodes[i]=1;
      return i;
    }
  }
  printf("\tim: error! no inode available");
  return 1;
}

void inode_manager::free_inode(uint32_t inum)
{
  /* 
   * your code goes here.
   * note: you need to check if the inode is already a freed one;
   * if not, clear it, and remember to write back to disk.
   */
  if (using_inodes[inum]==0)
  {
    return;
  }
  struct inode *ino = get_inode(inum);
  bzero((char *)ino, sizeof(struct inode));
  using_inodes[inum] = 0;
  put_inode(inum, ino);
  free(ino);
  return;
}

/* Return an inode structure by inum, NULL otherwise.
 * Caller should release the memory. */
struct inode *
inode_manager::get_inode(uint32_t inum)
{
  struct inode *ino, *ino_disk;
  char buf[BLOCK_SIZE];

  printf("\tim: get_inode %d\n", inum);

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

  printf("\tim: put_inode %d\n", inum);
  if (ino == NULL)
    return;

  bm->read_block(IBLOCK(inum, bm->sb.nblocks), buf);
  ino_disk = (struct inode *)buf + inum % IPB;
  *ino_disk = *ino;
  bm->write_block(IBLOCK(inum, bm->sb.nblocks), buf);
}

#define MIN(a, b) ((a) < (b) ? (a) : (b))

/* Get all the data of a file by inum. 
 * Return alloced data, should be freed by caller. */
void inode_manager::read_file(uint32_t inum, char **buf_out, int *size)
{
  /*
   * your code goes here.
   * note: read blocks related to inode number inum,
   * and copy them to buf_Out
   */
  struct inode *ino = get_inode(inum);
  if (!ino)
  {
    printf("\tim: invaild inum %d", inum);
    return;
  }

  if (ino->size == 0)
  {
    *size = 0;
    ino->atime = time(0);
    put_inode(inum, ino);
    free(ino);
    return;
  }

  *size = ino->size;
  ino->atime = time(0);
  int fileSize = ino->size;
  *buf_out = (char *)malloc(fileSize);

  char buf[BLOCK_SIZE];
  uint32_t i = 0;

  for (i = 0; i < NDIRECT && ino->blocks[i]; i++)
  {
    bm->read_block(ino->blocks[i], buf);
    if (fileSize > BLOCK_SIZE)
    {
      memcpy(*buf_out + i * BLOCK_SIZE, buf, BLOCK_SIZE);
      fileSize -= BLOCK_SIZE;
    }
    else
    {
      memcpy(*buf_out + i * BLOCK_SIZE, buf, fileSize);
      fileSize -= fileSize;
      break;
    }
  }
  if (fileSize > 0)
  {
    bm->read_block(ino->blocks[NDIRECT], buf);
    uint32_t indirect_block[NINDIRECT];
    memcpy(indirect_block, buf, BLOCK_SIZE);
    for (uint32_t j = 0; j < NINDIRECT; j++)
    {
      bm->read_block(indirect_block[j], buf);
      if (fileSize > BLOCK_SIZE)
      {
        memcpy(*buf_out + (i + j) * BLOCK_SIZE, buf, BLOCK_SIZE);
        fileSize -= BLOCK_SIZE;
      }
      else
      {
        memcpy(*buf_out + (i + j) * BLOCK_SIZE, buf, fileSize);
        fileSize -= fileSize;
        break;
      }
    }
  }
  put_inode(inum, ino);
  free(ino);
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
  if (size < 0 || (uint32_t)size > BLOCK_SIZE * MAXFILE)
  {
    printf("\tim: invalid size.\n");
    return;
  }
  inode *ino = get_inode(inum);
  if (ino == NULL)
  {
    printf("\tim:node not exist\n");
    return;
  }
  uint32_t block_num0 = (ino->size + BLOCK_SIZE - 1) / BLOCK_SIZE;
  uint32_t block_num1 = (size + BLOCK_SIZE - 1) / BLOCK_SIZE;

  ino->size = size;

  ino->atime = time(0);
  ino->ctime = time(0);
  ino->mtime = time(0);

  char local_buf[BLOCK_SIZE];
  uint32_t indirect_block[BLOCK_SIZE];
  //not enough
  if (block_num0 < block_num1)
  {
    // prev has INDIRECT
    if (block_num0 > NDIRECT)
    {
      bm->read_block(ino->blocks[NDIRECT], local_buf);
      memcpy(indirect_block, local_buf, BLOCK_SIZE);
      for (uint32_t i = block_num0 - NDIRECT; i < block_num1 - NDIRECT; i++)
      {
        indirect_block[i - 1] = bm->alloc_block();
      }
      bm->write_block(ino->blocks[NDIRECT], (const char *)indirect_block);
    }
    // prev has no INDIRECT, but now need
    else if (block_num1 > NDIRECT)
    {
      for (uint32_t i = block_num0; i < NDIRECT; i++)
      {
        ino->blocks[i] = bm->alloc_block();
      }
      ino->blocks[NDIRECT] = bm->alloc_block();
      // bm->read_block(ino->blocks[NDIRECT], local_buf);
      // memcpy(indirect_block, local_buf, BLOCK_SIZE);
      for (uint32_t i = 0; i < block_num1 - NDIRECT; i++)
      {
        indirect_block[i] = bm->alloc_block();
      }
      bm->write_block(ino->blocks[NDIRECT], (const char *)indirect_block);
    }
    // don't need INDIRECT
    else
    {
      for (uint32_t i = block_num0; i < block_num1; i++)
      {
        ino->blocks[i] = bm->alloc_block();
      }
    }
  }
  //enough
  else if (block_num0 > block_num1)
  {
    if (block_num1 > NDIRECT)
    {
      bm->read_block(ino->blocks[NDIRECT], local_buf);
      memcpy(indirect_block, local_buf, BLOCK_SIZE);
      for (uint32_t i = block_num1 - NDIRECT; i < block_num0 - NDIRECT; i++)
        bm->free_block(indirect_block[i]);
    }
    else if (block_num0 > NDIRECT)
    {
      bm->read_block(ino->blocks[NDIRECT], local_buf);
      memcpy(indirect_block, local_buf, BLOCK_SIZE);
      for (uint32_t i = 0; i < block_num0 - NDIRECT; i++)
        bm->free_block(indirect_block[i]);
      for (uint32_t i = block_num1; i <= NDIRECT; i++)
        bm->free_block(ino->blocks[i]);
    }
    else
    {
      for (uint32_t i = block_num1; i < block_num0; i++)
        bm->free_block(ino->blocks[i]);
    }
  }

  for (uint32_t i = 0; i < MIN(block_num1, NDIRECT); i++)
  {
    memcpy(local_buf, buf + i * BLOCK_SIZE, BLOCK_SIZE);
    bm->write_block(ino->blocks[i], local_buf);
  }

  if (block_num1 > NDIRECT)
  {
    bm->read_block(ino->blocks[NDIRECT], local_buf);
    memcpy(indirect_block, local_buf, BLOCK_SIZE);
    for (uint32_t i = 0; i < block_num1 - NDIRECT; i++)
    {
      memcpy(local_buf, buf + (i + NDIRECT) * BLOCK_SIZE, BLOCK_SIZE);
      bm->write_block(indirect_block[i], local_buf);
    }
  }

  put_inode(inum, ino);
  free(ino);
  return;
}

void inode_manager::getattr(uint32_t inum, extent_protocol::attr &a)
{
  /*
   * your code goes here.
   * note: get the attributes of inode inum.
   * you can refer to "struct attr" in extent_protocol.h
   */
  printf("\tim: getattr %d\n", inum);
  struct inode *ino = get_inode(inum);
  if (ino == NULL)
  {
    return;
  }
  a.atime = ino->atime;
  a.ctime = ino->ctime;
  a.mtime = ino->mtime;
  a.type = ino->type;
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
  inode *ino = get_inode(inum);
  if (ino == NULL)
    return;
  uint32_t num = (ino->size + BLOCK_SIZE - 1) / BLOCK_SIZE;
  for (uint32_t i = 0; i < MIN(num, NDIRECT); i++)
    bm->free_block(ino->blocks[i]);
  if (num > NDIRECT)
  {
    char buf[BLOCK_SIZE];
    uint32_t indirect_block[BLOCK_SIZE];

    bm->read_block(ino->blocks[NDIRECT], buf);
    memcpy(indirect_block, buf, BLOCK_SIZE);

    for (uint32_t i = 0; i < num - NDIRECT; i++)
      bm->free_block(indirect_block[i]);
    bm->free_block(ino->blocks[NDIRECT]);
  }
  free(ino);
  free_inode(inum);
  return;
}
