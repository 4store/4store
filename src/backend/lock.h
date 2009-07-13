#ifndef LOCK_H
#define LOCK_H

#include "backend.h"

//#define flock(f, o) fs_flock_logged(f, o, __FILE__, __LINE__)

typedef enum {
    FS_LOCK_SHARED = 1000,
    FS_LOCK_EXCLUSIVE,
    FS_LOCK_RELEASE
} fs_lock_action;

int fs_lock(fs_backend *be, const char *name, fs_lock_action action, int block);
int fs_lock_taken(fs_backend *be, const char *name);

int fs_lock_kb(const char *kb);

int fs_flock_logged(int fd, int op, const char *file, int line);

#endif
