#ifndef DISK_SPACE_H
#define DISK_SPACE_H

/* returns GB free */
float fs_free_disk_gb(const char *kb);

/* returns %age free */
float fs_free_disk(const char *kb);

#endif
