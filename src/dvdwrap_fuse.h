#ifndef _DVDWRAP_FUSE_H
#define _DVDWRAP_FUSE_H

#include <stdio.h>
#include <fuse.h>

#define PRIVATE		((dvdwrap_ctx_t*)fuse_get_context()->private_data)

typedef struct {
	const char *sourcepath;
} dvdwrap_ctx_t;

#endif

