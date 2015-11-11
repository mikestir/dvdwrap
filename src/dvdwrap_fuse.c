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
#include <stdint.h>
#include <string.h>
#include <errno.h>
#include <fcntl.h>
#include <dirent.h>
#include <limits.h>

#include "dvdwrap_fuse.h"

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
	int			fd;
	uint64_t	size;
} dvdwrap_vts_t;

/*! Private data held per output file */
typedef struct {
	dvdwrap_vts_t	vts[MAX_VTS_MIN];
	uint64_t		total_size;
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

/*!
 * Scans DVD image.  Looks for the titleset containing the largest title
 * and assumes that this is the main feature.
 *
 * \param path		Path to top level of DVD image (containing VIDEO_TS)
 */
static int dvdwrap_scan_videots(const char *path, int *vts_maj, uint64_t *total_size)
{
	int maj, min, longest_maj = 0;
	uint64_t titlesize[MAX_VTS_MAJ];
	uint64_t longest_size = 0;
	struct stat st;

	LOG("%s(%s)\n", __FUNCTION__, path);

	memset(titlesize, 0, sizeof(titlesize));

	for (maj = 1; maj < MAX_VTS_MAJ; maj++) {
		/* Skip VTS_nn_0 because this is always the menu content */
		for (min = 1; min < MAX_VTS_MIN; min++) {
			char vtspath[PATH_MAX];
			snprintf(vtspath, PATH_MAX, "%s/VIDEO_TS/VTS_%02d_%01d.VOB", path, maj, min);
			LOG("%s\n", vtspath);
			if (lstat(vtspath, &st) < 0) {
				/* No more VOBs in this titleset */
				LOG("No more VOBs at minor %d\n", min);
				break;
			}
			titlesize[maj] += st.st_size;
		}
		if (min == 1) {
			LOG("No more titlesets at major %d\n", maj);
			break;
		}
		if (titlesize[maj] > longest_size) {
			longest_size = titlesize[maj];
			longest_maj = maj;
		}
	}

	if (longest_maj) {
		LOG("Found longest titleset %d with length %llu\n", longest_maj, (unsigned long long)longest_size);
		*vts_maj = longest_maj;
		*total_size = longest_size;
		return 0;
	}

	return -1; /* Not found */
}

static int dvdwrap_getattr(const char *path, struct stat *stbuf)
{
	dvdwrap_ctx_t *ctx = PRIVATE;
	char targetpath[PATH_MAX];

	LOG("%s(%s, %p)\n", __FUNCTION__, path, stbuf);

	snprintf(targetpath, PATH_MAX, "%s/%s", ctx->sourcepath, path);

	memset(stbuf, 0, sizeof(struct stat));
	if (strcmp(&targetpath[strlen(targetpath) - strlen(FILE_EXTENSION)], FILE_EXTENSION) == 0) {
		/* File ends in FILE_EXTENSION so is probably a DVD. Remove
		 * the suffix to get back to the original DVD image path. */
		char vtspath[PATH_MAX];
		targetpath[strlen(targetpath) - strlen(FILE_EXTENSION)] = '\0';

		/* Stat the VIDEO_TS.IFO file to obtain ownership, etc. and as a
		 * pre-flight sanity check */
		snprintf(vtspath, PATH_MAX, "%s/VIDEO_TS/VIDEO_TS.IFO", targetpath);
		if (lstat(vtspath, stbuf) == 0) {
			int maj;
			uint64_t total_size;

			/* Scan titlesets for main feature and return aggregate file size */
			if (dvdwrap_scan_videots(targetpath, &maj, &total_size) == 0) {
				stbuf->st_size = (off_t)total_size;
			} else {
				LOG("VTS scan failed\n");
				return -ENOENT;
			}
		} else {
			LOG("VIDEO_TS.IFO not found\n");
			return -ENOENT;
		}
	} else {
		/* For all other files just pass straight through */
		if (lstat(targetpath, stbuf) < 0) {
			return -ENOENT;
		}
		stbuf->st_mode &= ~0222; /* Everything is read-only */
	}
	return 0;
}

/* Directory operations */

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
			char thispath[PATH_MAX], thatpath[PATH_MAX];
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

			/* If directory contains VIDEO_TS then squash to a file */
			snprintf(thatpath, PATH_MAX, "%s/VIDEO_TS", thispath);
			if (lstat(thatpath, &st) < 0) {
				/* Pass through directory name to output */
				filler(buf, dir->d_name, NULL, 0);
			} else {
				/* Turn this directory into an MPEG file */
				snprintf(thatpath, PATH_MAX, "%s" FILE_EXTENSION, dir->d_name);
				filler(buf, thatpath, NULL, 0);
			}
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
	uint64_t total_size;
	char targetpath[PATH_MAX];
	char vtspath[PATH_MAX];
	struct stat st;

	LOG("%s(%s, %p)\n", __FUNCTION__, path, fi);

	/* Process path for filename and remove extension */
	snprintf(targetpath, PATH_MAX, "%s/%s", ctx->sourcepath, path);
	if (strcmp(&targetpath[strlen(targetpath) - strlen(FILE_EXTENSION)], FILE_EXTENSION) != 0) {
		/* This file doesn't refer to a DVD image */
		LOG("Bad filename\n");
		return -ENOENT;
	}
	targetpath[strlen(targetpath) - strlen(FILE_EXTENSION)] = '\0';

	/* Scan for titleset major number and total size */
	if (dvdwrap_scan_videots(targetpath, &maj, &total_size) < 0) {
		LOG("VTS scan failed\n");
		return -ENOENT;
	}

	/* All is well - allocate private data */
	private = calloc(1, sizeof(dvdwrap_fh_t));
	if (private == NULL) {
		return -ENOMEM;
	}
	fi->fh = (uint64_t)private;

	/* Open all VOBs in this titleset, skipping the menu (index 0) */
	private->total_size = 0;
	for (min = 1; min < MAX_VTS_MIN; min++) {
			snprintf(vtspath, PATH_MAX, "%s/VIDEO_TS/VTS_%02d_%01d.VOB", targetpath, maj, min);
			if (lstat(vtspath, &st) < 0) {
				break; /* No more files in the titleset */
			}
			LOG("Open %s (size = %zu)\n", vtspath, st.st_size);
			private->vts[min].fd = open(vtspath, O_RDONLY);
			if (private->vts[min].fd < 0) {
				goto fail;
			}
			private->vts[min].size = (uint64_t)st.st_size;
			private->total_size += (uint64_t)st.st_size;
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
	int min, rc;
	size_t total = 0;

	LOG("%s(%s, %p, %zd, %zd, %p)\n", __FUNCTION__, path, buf, size, offset, fi);

	/* Initial sanity check */
	if (offset >= private->total_size) {
		/* EOF */
		return 0;
	}

	while (total < size) {
		off_t thisoffset = offset;
		off_t thissize = size - total;

		/* Determine the source file for this read and convert overall
		 * offset into offset for that specific VOB */
		for (min = 1; min < MAX_VTS_MIN; min++) {
			if (thisoffset < private->vts[min].size) {
				break;
			}
			thisoffset -= private->vts[min].size;
		}
		if (min == MAX_VTS_MIN) {
			LOG("Read beyond end of titleset\n");
			break;
		}
		if (thissize > private->vts[min].size - thisoffset) {
			thissize = private->vts[min].size - thisoffset;
		}
		LOG("File %d offset %zd size %zd\n", min, thisoffset, thissize);

		/* Read next block - we may span into next VOB if we read over the end */
		lseek(private->vts[min].fd, thisoffset, SEEK_SET);
		rc = read(private->vts[min].fd, buf, thissize);
		if (rc < 0) {
			/* Read error */
			return rc;
		}

		/* Adjust pointers and repeat read if we need more data */
		buf += rc;
		offset += rc;
		total += rc;
	}

	return total;
}

static int dvdwrap_release(const char* path, struct fuse_file_info *fi)
{
	dvdwrap_fh_t *private = (dvdwrap_fh_t*)fi->fh;
	int min;

	LOG("%s(%s, %p)\n", __FUNCTION__, path, fi);

	/* Close files and release private data */
	for (min = 1; min < MAX_VTS_MIN; min++) {
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


