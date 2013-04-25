/*
 * dvdwrap, a fuse filesystem for easy access to DVD image directories
 * Copyright (C) 2013 Mike Stirling
 *
 * This file is part of dvdwrap (http://mikestirling.co.uk/dvdwrap)
 * All rights reserved.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License along
 * with this program; if not, write to the Free Software Foundation, Inc.,
 * 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
 */

#define FUSE_USE_VERSION 26

#include <fuse.h>
#include <libgen.h>
#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <fcntl.h>
#include <dirent.h>
#include <limits.h>

#include "dvdwrap_fuse.h"

#define VTS_MIN_SKIP	1
#define MAX_VTS_MIN		10
#define MAX_VTS_MAJ		100
#define FILE_EXTENSION	".mpg"

#ifdef DEBUG
#define LOG(a,...)		fprintf(stderr, __FILE__ "(%d): " a, __LINE__, ##__VA_ARGS__)
#else
#define LOG(a,...)
#endif

/*! Private data held per input file */
typedef struct {
	int		fd;
	size_t	size;
} dvdwrap_vts_t;

/*! Private data held per output file */
typedef struct {
	dvdwrap_vts_t	vts[MAX_VTS_MIN];
	size_t	total;
} dvdwrap_fh_t;

static int dvdwrap_getattr(const char *path, struct stat *stbuf);

static int dvdwrap_opendir(const char* path, struct fuse_file_info* fi);
static int dvdwrap_readdir(const char *path, void *buf, fuse_fill_dir_t filler,
	off_t offset, struct fuse_file_info *fi);
static int dvdwrap_releasedir(const char* path, struct fuse_file_info *fi);

static int dvdwrap_open(const char *path, struct fuse_file_info *fi);
static int dvdwrap_read(const char *path, char *buf, size_t size, off_t offset,
	struct fuse_file_info *fi);
static int dvdwrap_release(const char* path, struct fuse_file_info *fi);

static struct fuse_operations dvdwrap_oper = {
	.getattr	= dvdwrap_getattr,
	.opendir	= dvdwrap_opendir,
	.readdir	= dvdwrap_readdir,
	.releasedir	= dvdwrap_releasedir,
	.open		= dvdwrap_open,
	.read		= dvdwrap_read,
	.release	= dvdwrap_release,

	.flag_nullpath_ok	= 1,
};

static int dvdwrap_getattr(const char *path, struct stat *stbuf)
{
	dvdwrap_ctx_t *ctx = PRIVATE;
	char targetpath[PATH_MAX];
	char *tp1, *tp2;
	char *dirpath;
	char *filename;
	int rc = 0;
	int maj, min;
	size_t total;

	LOG("%s(%s, %p)\n", __FUNCTION__, path, stbuf);

	snprintf(targetpath, PATH_MAX, "%s/%s", ctx->sourcepath, path);
	tp1 = strdup(targetpath);
	tp2 = strdup(targetpath);
	dirpath = dirname(tp1);
	filename = basename(tp2);

	memset(stbuf, 0, sizeof(struct stat));
	if (sscanf(filename, "%2d" FILE_EXTENSION, &maj) == 1) {
		char vtspath[PATH_MAX];

		/* This is a combined output file - stat all the source VOB files and
		 * declare the cumulative size */
		total = 0;
		for (min = VTS_MIN_SKIP; min < MAX_VTS_MIN; min++) {
			snprintf(vtspath, PATH_MAX, "%s/VIDEO_TS/VTS_%02d_%01d.VOB", dirpath, maj, min);
			if (lstat(vtspath, stbuf) < 0)
				break;
			total += stbuf->st_size;
		}
		stbuf->st_size = total;
		if (!total)
			rc = -ENOENT;
	} else {
		/* Pass through */
		if (lstat(targetpath, stbuf) < 0)
			rc = -errno;
		stbuf->st_mode &= ~0222; /* Everything is read-only */
	}
	free(tp1);
	free(tp2);
	return rc;
}

/* Directory operations */

static void dvdwrap_fill_videots(const char *path, void *buf, fuse_fill_dir_t filler)
{
	DIR *d;
	struct dirent *dir;
	int vts[MAX_VTS_MAJ];
	int n;

	LOG("%s(%s, %p, %p)\n", __FUNCTION__, path, buf, filler);

	memset(vts, 0, sizeof(vts));

	d = opendir(path);
	if (d) {
		while ((dir = readdir(d)) != NULL) {
			int maj, min;

			/* Pick out files with names like VTS_##_#.VOB */
			if (sscanf(dir->d_name, "VTS_%2d_%1d.VOB", &maj, &min) != 2)
				continue;

			/* Sanity checks */
			if (maj >= MAX_VTS_MAJ || min >= MAX_VTS_MIN)
				continue;

			/* Mark presence of this vts */
			vts[maj]++;
		}
	}

	/* For every group of VTS files with the same major number we create
	 * a single concatenated output file */
	for (n = 0; n < MAX_VTS_MAJ; n++) {
		if (vts[n]) {
			char filename[32];

			snprintf(filename, 32, "%02d" FILE_EXTENSION, n);
			filler(buf, filename, NULL, 0);
		}
	}
}

static int dvdwrap_opendir(const char* path, struct fuse_file_info* fi)
{
	LOG("%s(%s, %p)\n", __FUNCTION__, path, fi);

	/* FIXME: We need to implement separate opendir/releasedir because we
	 * declare flag_nullpath_ok, but there is no optimisation here yet */

	fi->fh = (uint64_t)strdup(path);
	return 0;
}

static int dvdwrap_readdir(const char *path, void *buf, fuse_fill_dir_t filler,
	off_t offset, struct fuse_file_info *fi)
{
	DIR *d;
	struct dirent *dir;
	dvdwrap_ctx_t *ctx = PRIVATE;
	char targetpath[PATH_MAX];

	LOG("%s(%s, %p, %p, %zd, %p)\n", __FUNCTION__, path, buf, filler, offset, fi);

	if (!path)
		path = (const char*)fi->fh;
	snprintf(targetpath, PATH_MAX, "%s/%s", ctx->sourcepath, path);

	/* Always return current and parent directories */
	filler(buf, ".", NULL, 0);
	filler(buf, "..", NULL, 0);

	/* Scan the equivalent location in the source path and proxy
	 * through all subdirectories except VIDEO_TS.  Files are ignored. */
	d = opendir(targetpath);
	if (d) {
		while ((dir = readdir(d)) != NULL) {
			char thispath[PATH_MAX];
			struct stat st;

			snprintf(thispath, PATH_MAX, "%s/%s", targetpath, dir->d_name);

			/* Skip hidden entities and current/parent directory */
			if (dir->d_name[0] == '.')
				continue; /* hidden */

			/* Some filesystems will tell us this is a dir straight away */
			if (dir->d_type != DT_DIR) {
				/* or maybe that it definitely isn't a dir */
				if (dir->d_type != DT_UNKNOWN)
					continue; /* not a dir */

				/* Otherwise call lstat to determine the entity type */
				if (lstat(thispath, &st) < 0)
					continue; /* stat failed */
				if (!S_ISDIR(st.st_mode))
					continue; /* not a dir */
			}

			if (strcmp(dir->d_name, "VIDEO_TS") == 0) {
				/* Special handling for VIDEO_TS directories */
				dvdwrap_fill_videots(thispath, buf, filler);
				continue;
			}

			/* Pass through directory name to output */
			filler(buf, dir->d_name, NULL, 0);
		}
		closedir(d);
	}
	return 0;
}

static int dvdwrap_releasedir(const char* path, struct fuse_file_info *fi)
{
	LOG("%s(%s, %p)\n", __FUNCTION__, path, fi);

	free((void*)fi->fh);
	return 0;
}

/* File operations */

static int dvdwrap_open(const char *path, struct fuse_file_info *fi)
{
	dvdwrap_ctx_t *ctx = PRIVATE;
	dvdwrap_fh_t *private;
	int maj, min;
	char targetpath[PATH_MAX];
	char vtspath[PATH_MAX];
	char *tp1, *tp2;
	char *dirpath, *filename;
	struct stat st;

	LOG("%s(%s, %p)\n", __FUNCTION__, path, fi);

	/* Process path for filename and VTS major number */
	snprintf(targetpath, PATH_MAX, "%s/%s", ctx->sourcepath, path);
	tp1 = strdup(targetpath);
	tp2 = strdup(targetpath);
	dirpath = dirname(tp1);
	filename = basename(tp2);
	if (sscanf(filename, "%2d" FILE_EXTENSION, &maj) < 1) {
		LOG("Bad filename\n");
		return -ENOENT;
	}

	/* Allocate private data */
	private = calloc(1, sizeof(dvdwrap_fh_t));
	if (private == NULL)
		return -ENOMEM;
	fi->fh = (uint64_t)private;

	/* Open all input files */
	private->total = 0;
	for (min = VTS_MIN_SKIP; min < MAX_VTS_MIN; min++) {
			snprintf(vtspath, PATH_MAX, "%s/VIDEO_TS/VTS_%02d_%01d.VOB", dirpath, maj, min);
			if (lstat(vtspath, &st) < 0)
				break; /* No more files in the group */
			LOG("Open %s (size = %zd)\n", vtspath, st.st_size);
			private->vts[min].fd = open(vtspath, O_RDONLY);
			if (private->vts[min].fd < 0)
				goto fail;
			private->vts[min].size = st.st_size;
			private->total += st.st_size;
	}

	return 0;
fail:
	/* Clean up */
	dvdwrap_release(NULL, fi);
	return -ENOENT;
}

static int dvdwrap_read(const char *path, char *buf, size_t size, off_t offset,
	struct fuse_file_info *fi)
{
	dvdwrap_fh_t *private = (dvdwrap_fh_t*)fi->fh;
	int min;

	LOG("%s(%s, %p, %zd, %zd, %p)\n", __FUNCTION__, path, buf, size, offset, fi);

	if (offset >= private->total) {
		/* EOF */
		return 0;
	}

	/* Determine the source file for this read */
	for (min = VTS_MIN_SKIP; min < MAX_VTS_MIN; min++) {
		if (offset < private->vts[min].size)
			break;
		offset -= private->vts[min].size;
	}
	if (size > private->vts[min].size - offset)
		size = private->vts[min].size - offset;
	LOG("File %d offset %zd size %zd\n", min, offset, size);

	lseek(private->vts[min].fd, offset, SEEK_SET);
	return read(private->vts[min].fd, buf, size);
}

static int dvdwrap_release(const char* path, struct fuse_file_info *fi)
{
	dvdwrap_fh_t *private = (dvdwrap_fh_t*)fi->fh;
	int min;

	LOG("%s(%s, %p)\n", __FUNCTION__, path, fi);

	/* Close files and release private data */
	for (min = VTS_MIN_SKIP; min < MAX_VTS_MIN; min++) {
		if (private->vts[min].size) {
			LOG("Closing VTS %d (fd = %d)\n", min, private->vts[min].fd);
			close(private->vts[min].fd);
		}
	}
	free(private);
	fi->fh = 0;

	return -ENOENT;
}

/* Main */

int main(int argc, char **argv)
{
	dvdwrap_ctx_t *ctx;
	int n;

	if (argc < 3) {
		fprintf(stderr,"Usage: %s <source> <mount point> [options]\n\n", argv[0]);
		return 1;
	}

	/* Allocate context */
	ctx = (dvdwrap_ctx_t*)malloc(sizeof(dvdwrap_ctx_t));
	if (ctx == NULL) {
		fprintf(stderr, "Failed to allocate private data\n");
		return 1;
	}
	ctx->sourcepath = realpath(argv[1], NULL);
	LOG("sourcepath = %s\n", ctx->sourcepath);
	for (n = 1; n < argc - 1; n++)
		argv[n] = argv[n + 1];
	argc--;

	return fuse_main(argc, argv, &dvdwrap_oper, ctx);
}


