
/*
 * ntfscloneimgdelta
 *
 * Written 2011 by Kolja Nowak <kolja@nowak2000.de>, 
 * with some parts taken from ntfsclone.
 *
 * Create efficient delta between special image files created by ntfsclone.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 */

#define _LARGEFILE_SOURCE
#define _FILE_OFFSET_BITS 64

#include <unistd.h>
#include <stdlib.h>
#include <stdio.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <stdarg.h>
#include <stdint.h>
#include <string.h>
#include <errno.h>

#if defined(__LITTLE_ENDIAN) && (__BYTE_ORDER == __LITTLE_ENDIAN)

#define le32_to_cpu(x) (x)
#define sle64_to_cpu(x) (x)
#define cpu_to_sle64(x) (x)

#else

#error Big endian handling not implemented yet

#endif

static void err_exit(const char *fmt, ...)
{
  va_list ap;
  va_start(ap, fmt);
  vfprintf(stderr, fmt, ap);
  va_end(ap);
  fflush(stderr);
  exit(1);
}

static void perr_exit(const char *fmt, ...)
{
  va_list ap;
  int eo = errno;
  va_start(ap, fmt);
  vfprintf(stderr, fmt, ap);
  va_end(ap);
  fprintf(stderr, ": %s\n", strerror(eo));
  fflush(stderr);
  exit(1);
}

static void read_all(int fd, void *buf, int count)
{
  int i;
  while(count > 0)
  {
    i = read(fd, buf, count);
    if(i < 0) 
    {
      if(errno != EAGAIN && errno != EINTR)
        perr_exit("read");
    }
    else if(i == 0)
    {
      err_exit("read: unexpected end of file\n");
    }
    else 
    {
      count -= i;
      buf = i + (char *) buf;
    }
  }
}

static void write_all(int fd, void *buf, int count)
{
  int i;
  while(count > 0)
  {
    i = write(fd, buf, count);
    if(i < 0) 
    {
      if(errno != EAGAIN && errno != EINTR)
        perr_exit("write");
    } 
    else 
    {
      count -= i;
      buf = i + (char *) buf;
    }
  }
}

#define CMD_SKIP 0
#define CMD_DATA 1
#define CMD_DROP 2

#define IMAGE_MAGIC "\0ntfsclone-image"
#define DELTA_MAGIC "\0ntfsclone-delta"
#define IMAGE_MAGIC_SIZE  16

struct image_hdr
{
  char magic[IMAGE_MAGIC_SIZE];
  uint8_t major_ver;
  uint8_t minor_ver;
  uint32_t cluster_size; /* all values are in little endian */
  int64_t device_size;
  int64_t nr_clusters;
  int64_t inuse;
  uint32_t offset_to_image_data; /* from start of image_hdr */
}
__attribute__((__packed__));

#define NTFSCLONE_IMG_VER_MAJOR	10
#define NTFSCLONE_IMG_VER_MINOR_OLD 0
#define NTFSCLONE_IMG_VER_MINOR_NEW 1
#define NTFS_MAX_CLUSTER_SIZE 65536

struct input_image 
{
  int fd;
  struct image_hdr hdr;
  char* hdr_extra;
  uint32_t hdr_extra_len;
  uint32_t csize;
  int64_t ccount;
  int bbs_present; /* backup boot sector present after the last cluster */
  char cmd;
  int64_t cmd_repeat;
  char cdata[NTFS_MAX_CLUSTER_SIZE];
};

struct output_image
{
  int fd;
  char cmd;
  int64_t cmd_repeat;
};

static void open_input_image(char* file, struct input_image* img, char* magic)
{
  memset(img, 0, sizeof(*img));

  if(strcmp(file, "-") == 0) 
  {
    if((img->fd = fileno(stdin)) == -1)
      err_exit("fileno for stdin failed");
  } 
  else 
  {
    if((img->fd = open(file, O_RDONLY)) == -1)
      perr_exit("failed to open input image");
  }

  read_all(img->fd, &img->hdr, ((size_t)&((struct image_hdr*)0)->offset_to_image_data));

  if(memcmp(img->hdr.magic, magic, IMAGE_MAGIC_SIZE) != 0)
    err_exit("Input file doesn't have the expected magic header field!\n");
  if(img->hdr.major_ver != NTFSCLONE_IMG_VER_MAJOR ||
    (img->hdr.minor_ver != NTFSCLONE_IMG_VER_MINOR_OLD && img->hdr.minor_ver != NTFSCLONE_IMG_VER_MINOR_NEW))
    err_exit("Image version %d.%d not supported\n", img->hdr.major_ver, img->hdr.minor_ver);

  read_all(img->fd, &img->hdr.offset_to_image_data, sizeof(img->hdr.offset_to_image_data));

  img->hdr_extra_len = le32_to_cpu(img->hdr.offset_to_image_data) - sizeof(img->hdr);
    
  if(img->hdr_extra_len > 0)
  {
    img->hdr_extra = malloc(img->hdr_extra_len);
    read_all(img->fd, img->hdr_extra, img->hdr_extra_len);
  }

  img->csize = le32_to_cpu(img->hdr.cluster_size);
  img->ccount = sle64_to_cpu(img->hdr.nr_clusters);
  img->bbs_present = (img->hdr.minor_ver == NTFSCLONE_IMG_VER_MINOR_NEW) ? 1 : 0;
  
//fprintf(stderr, "Image opened for reading: %s %ld %lld -> %d\n", file, img->csize, img->ccount, img->fd); fflush(stderr);
}

static void create_output_image(char* file, struct output_image* img, char* magic, struct input_image* old_img)
{
  memset(img, 0, sizeof(*img));
  img->cmd = CMD_DATA;

  if(strcmp(file, "-") == 0) 
  {
    if((img->fd = fileno(stdout)) == -1)
      perr_exit("fileno for stdout failed");
  }
  else
  {
    if((img->fd = open(file, O_WRONLY | O_CREAT | O_TRUNC, 0666)) == -1)
      perr_exit("failed to open output image");
  }
  
  write_all(img->fd, magic, IMAGE_MAGIC_SIZE);
  write_all(img->fd, &old_img->hdr.major_ver, sizeof(old_img->hdr) - IMAGE_MAGIC_SIZE);

  if(old_img->hdr_extra_len)
    write_all(img->fd, old_img->hdr_extra, old_img->hdr_extra_len);
  
//fprintf(stderr, "Image opened for writing: %s\n", file); fflush(stderr);
}

static void read_next_cluster(struct input_image* img, int allow_drop_cmd)
{
  if(img->cmd_repeat > 0)
    img->cmd_repeat--;
  else 
  {
    read_all(img->fd, &img->cmd, sizeof(img->cmd));
      
    if(img->cmd == CMD_SKIP || (allow_drop_cmd && img->cmd == CMD_DROP))
    {
      read_all(img->fd, &img->cmd_repeat, sizeof(img->cmd_repeat));

      img->cmd_repeat = sle64_to_cpu(img->cmd_repeat);

      if(img->cmd_repeat == 0)
        err_exit("Zero repeat length after command code in image\n");

//fprintf(stderr, "<%d:%d:%lld>", img->fd, (int)img->cmd, img->cmd_repeat); 

      img->cmd_repeat--;
    } 
    else if(img->cmd == CMD_DATA) 
      read_all(img->fd, img->cdata, img->csize);
    else
      err_exit("Invalid command code in image\n");
  }
}

static void write_pending_cmd(struct output_image* img)
{
  if(img->cmd != CMD_DATA)
  {
    int64_t repeat = cpu_to_sle64(img->cmd_repeat);

    write_all(img->fd, &img->cmd, sizeof(img->cmd));
    write_all(img->fd, &repeat, sizeof(repeat));
      
//fprintf(stderr, "[%d:%lld]", (int)img->cmd, img->cmd_repeat);

    img->cmd = CMD_DATA;
    img->cmd_repeat = 0;
  }
}

static void write_cmd(struct output_image* img, char cmd)
{
  if(img->cmd == cmd)
  {
    img->cmd_repeat += 1;
  }
  else
  {
    write_pending_cmd(img);
    img->cmd = cmd;
    img->cmd_repeat = 1;
  }
}

static void write_data(struct output_image* img, char* cdata, uint32_t csize)
{
  write_pending_cmd(img);

  write_all(img->fd, &img->cmd, sizeof(img->cmd));
  write_all(img->fd, cdata, csize);
}

static void prepare_image_files(
  char* file1, struct input_image* img1, char* magic1, 
  char* file2, struct input_image* img2, char* magic2, 
  char* file3, struct output_image* img3, char* magic3)
{
  open_input_image(file1, img1, magic1);
  open_input_image(file2, img2, magic2);

  if(memcmp(&img1->hdr.cluster_size, &img2->hdr.cluster_size, ((size_t)&((struct image_hdr*)0)->inuse) - IMAGE_MAGIC_SIZE - 2) != 0)
    err_exit("Input images do not have identical headers\n");
    
  if(img1->hdr_extra_len && memcmp(img1->hdr_extra, img2->hdr_extra, img1->hdr_extra_len) != 0)
    err_exit("Input images do not have identical headers\n");
		
  create_output_image(file3, img3, magic3, img2);
}

static void finish_image_files(struct input_image* img1, struct input_image* img2, struct output_image* img3)
{
  if(img1->cmd_repeat > 0)
    err_exit("First input image has %d remaining unused clusters at the end\n", (int)img1->cmd_repeat);

  if(img2->cmd_repeat > 0)
    err_exit("Second input image has %d remaining unused clusters at the end\n", (int)img2->cmd_repeat);

  write_pending_cmd(img3);
  fsync(img3->fd);
}
  

static void create_delta(char* file1, char* file2, char* file3)
{
  int64_t pos, ccount;
  struct input_image old, new;
  struct output_image delta;
  
  prepare_image_files(file1, &old, IMAGE_MAGIC, file2, &new, IMAGE_MAGIC, file3, &delta, DELTA_MAGIC);

  ccount = old.ccount;                                    /* if both files have the new format with */
  if(old.bbs_present == 1 && new.bbs_present == 1)        /* the backup boot sector at the end, we  */
    ccount += 1;                                          /* just have one block more to compare    */

  for(pos = 0; pos < ccount; pos++)
  {
    read_next_cluster(&old, 0);
    read_next_cluster(&new, 0);

    if((old.cmd == new.cmd) && (old.cmd == CMD_SKIP || memcmp(old.cdata, new.cdata, old.csize) == 0)) 
      write_cmd(&delta, CMD_SKIP);
    else if(new.cmd == CMD_SKIP)
      write_cmd(&delta, CMD_DROP);
    else
      write_data(&delta, new.cdata, new.csize);
  }
  
  if(old.bbs_present == 1 && new.bbs_present == 0)        /* if only the first file has the new     */
  {                                                       /* format with the backup boot sector at  */
    read_next_cluster(&old, 0);                           /* the end, we just discard this block    */
  }
  else if(old.bbs_present == 0 && new.bbs_present == 1)   /* if only the second file has the new    */
  {                                                       /* format with the backup boot sector at  */
    read_next_cluster(&new, 0);                           /* the end, we have to keep this block    */
    write_data(&delta, new.cdata, new.csize);
  }
  
  finish_image_files(&old, &new, &delta);
}

static void apply_patch(char *file1, char* file2, char* file3)
{
  int64_t pos, ccount;
  struct input_image old, delta;
  struct output_image new;
  
  prepare_image_files(file1, &old, IMAGE_MAGIC, file2, &delta, DELTA_MAGIC, file3, &new, IMAGE_MAGIC);
  
  ccount = old.ccount;                                    /* if both files have the new format with */
  if(old.bbs_present == 1 && delta.bbs_present == 1)      /* the backup boot sector at the end, we  */
    ccount += 1;                                          /* just have one block more to compare    */

  for(pos = 0; pos < old.ccount; pos++)
  {
    read_next_cluster(&old, 0);
    read_next_cluster(&delta, 1);

    if(delta.cmd == CMD_DROP || (old.cmd == CMD_SKIP && delta.cmd == CMD_SKIP))
      write_cmd(&new, CMD_SKIP);
    else if(delta.cmd == CMD_SKIP)
      write_data(&new, old.cdata, old.csize);
    else 
      write_data(&new, delta.cdata, delta.csize);
  }
  
  if(old.bbs_present == 1 && delta.bbs_present == 0)      /* if only the first file has the new     */
  {                                                       /* format with the backup boot sector at  */
    read_next_cluster(&old, 0);                           /* the end, we just discard this block    */
  }
  else if(old.bbs_present == 0 && delta.bbs_present == 1) /* if only the second file has the new    */
  {                                                       /* format with the backup boot sector at  */
    read_next_cluster(&delta, 0);                         /* the end, we have to keep this block    */
    write_data(&new, delta.cdata, delta.csize);
  }
  
  finish_image_files(&old, &delta, &new);
}

static void usage()
{
  err_exit(
    "Usage: ntfscloneimgdelta delta OLDFILE [NEWFILE [DELTA]]\n"
    "       ntfscloneimgdelta patch OLDFILE [DELTA [NEWFILE]]\n");
}

int main(int argc, char** argv)
{
  char* file1, * file2, * file3;

  if(argc < 3)
    usage();
    
  file1 = argv[2];
  file2 = argc > 3 ? argv[3] : "-";
  file3 = argc > 4 ? argv[4] : "-";
  
  if(strcmp(file1, "-") == 0 && strcmp(file2, "-") == 0)
    err_exit("You cannot select stdin for both input files\n");

  if(strcmp(argv[1], "delta") == 0)
    create_delta(file1, file2, file3);
  else if(strcmp(argv[1], "patch") == 0)
    apply_patch(file1, file2, file3);
  else
    usage();
    
  return 0;
}
