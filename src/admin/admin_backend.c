/*
    4store - a clustered RDF storage and query engine

    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.


    Copyright 2011 Dave Challis
 */

#define _GNU_SOURCE

#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <stdio.h>
#include <dirent.h>
#include <signal.h>

#include "../common/params.h"
#include "../common/error.h"
#include "../backend/metadata.h"

#include "admin_protocol.h"
#include "admin_common.h"
#include "admin_backend.h"

/* Read runtime.info and metadata.nt to fill in info for a kb.
 * Leave ipaddr unset, caller can set if needed.
 */
int fsab_kb_info_init(fsa_kb_info *ki, const unsigned char *kb_name)
{
    FILE *ri_file;
    int len, rv;
    char *path;
    fs_metadata *md;

    ki->name = (unsigned char *)strdup((char *)kb_name);

    /* alloc mem for string path to runtime.info */
    len = (strlen(FS_RI_FILE)-2) + strlen((char *)kb_name) + 1;
    path = (char *)malloc(len * sizeof(char));
    if (path == NULL) {
        errno = ENOMEM;
        return -1;
    }

    /* generate full path to runtime.info */
    rv = sprintf(path, FS_RI_FILE, kb_name);
    if (rv < 0) {
        fsa_error(LOG_DEBUG, "sprintf failed");
        free(path);
        return -1;
    }
 
    /* attempt to open file for reading, ignore failures, but log them */
    ri_file = fopen(path, "r");

    if (ri_file == NULL) {
        fsa_error(LOG_ERR, "failed to read runtime info file at '%s': %s",
                  path, strerror(errno));
        free(path);
    }
    else {
        free(path);

        /* check lock on file, and ignore if not locked - info is stale */
        struct flock ri_lock;
        int fd = fileno(ri_file);

        ri_lock.l_type = F_WRLCK;    /* write lock */
        ri_lock.l_whence = SEEK_SET; /* l_start begins at start of file */
        ri_lock.l_start = 0;         /* offset from whence */
        ri_lock.l_len = 0;           /* until EOF */

        rv = fcntl(fd, F_GETLK, &ri_lock);
        if (rv == -1) {
            fsa_error(LOG_CRIT, "fnctl locking error: %s", strerror(errno));
            fclose(ri_file);
            return -1;
        }

        if (ri_lock.l_type == F_WRLCK) {
            /* file locked, so use info */
            int port, pid;

            ki->pid = ri_lock.l_pid;

            rv = fscanf(ri_file, "%d %d", &pid, &port);
            if (rv == 0 || rv == EOF) {
                fsa_error(LOG_CRIT,
                          "bad data in runtime info file, fscanf failed");
                fclose(ri_file);
                return -1;
            }
            else {
                /* file locked and contains running port and pid */
                ki->port = port;    
                ki->status = KB_STATUS_RUNNING;
            }
        }
        else if (ri_lock.l_type == F_UNLCK) {
            /* file readable, but not locked */
            ki->status = KB_STATUS_STOPPED;
        }

        fclose(ri_file);
    }

    /* pull data from metadata.nt */
    md = fs_metadata_open((char *)kb_name);
    if (md != NULL) {
        ki->num_segments =
            atoi(fs_metadata_get_string(md, FS_MD_SEGMENTS, "0"));

        fs_rid_vector *vec = fs_metadata_get_int_vector(md, FS_MD_SEGMENT_P);
        fs_rid_vector_sort(vec);

        /* segment ID and max segments should be 256 */
        ki->p_segments_len = (uint8_t)vec->length;
        ki->p_segments_data =
            (uint8_t *)malloc(ki->p_segments_len * sizeof(uint8_t));

        for (int i = 0; i < vec->length; i++) {
            ki->p_segments_data[i] = (uint8_t)vec->data[i];
        }

        fs_rid_vector_free(vec);
        fs_metadata_close(md);

        fsa_error(LOG_DEBUG, "metadata.nt read for kb %s", kb_name);
    }
    else {
        fsa_error(LOG_ERR, "unable to read metadata.nt for kb %s", kb_name);
    }

    return 0;
}

/* get all local info about a kb. Uses errno to differentiate between
   returning NULL when there are no kbs, and NULL due to a read error */
fsa_kb_info *fsab_get_local_kb_info(const unsigned char *kb_name)
{
    int rv;
    fsa_kb_info *ki = fsa_kb_info_new();

    rv = fsab_kb_info_init(ki, kb_name);
    if (rv == -1) {
        fsa_error(LOG_ERR, "failed to get local kb info for kb %s", kb_name);
        errno = ADM_ERR_GENERIC; 
        fsa_kb_info_free(ki);
        return NULL;
    }

    errno = 0;
    return ki;
}

/* get info on all kbs on this host */
fsa_kb_info *fsab_get_local_kb_info_all(void)
{
    struct dirent *entry;
    DIR *dp;
    fsa_kb_info *first_ki = NULL;
    fsa_kb_info *cur_ki = NULL;
    int rv;

    dp = opendir(FS_STORE_ROOT);
    if (dp == NULL) {
        fsa_error(LOG_ERR, "failed to open directory '%s': %s",
                  FS_STORE_ROOT, strerror(errno));
        errno = ADM_ERR_GENERIC;
        return NULL;
    }

    while ((entry = readdir(dp)) != NULL) {
        if (entry->d_name[0] == '.') {
            /* skip ., .., and hidden dirs */
            continue;
        }

        cur_ki = fsa_kb_info_new();
        rv = fsab_kb_info_init(cur_ki, (unsigned char *)entry->d_name);
        if (rv == -1) {
            fsa_error(LOG_ERR, "failed to initialise kb info for %s",
                      entry->d_name);
            fsa_kb_info_free(cur_ki);
            continue;
        }
        else {
            cur_ki->next = first_ki;
            first_ki = cur_ki;
        }
    }

    closedir(dp);

    /* set errno to differentiate between no kbs, and error */
    errno = 0;
    return first_ki;
}

/* return 0 on success, -1 otherwise, and sets err */
int fsab_stop_local_kb(const unsigned char *kb_name, int *err)
{
    fs_error(LOG_DEBUG, "stopping kb '%s'", kb_name);

    fsa_kb_info *ki = fsab_get_local_kb_info(kb_name);
    if (ki == NULL) {
        *err = ADM_ERR_KB_GET_INFO;
        return -1;
    }

    /* check whether kb is running */
    if (ki->status == KB_STATUS_STOPPED) {
        fs_error(LOG_INFO, "cannot stop %s, already stopped", kb_name);
        *err = ADM_ERR_KB_STATUS_STOPPED;
        fsa_kb_info_free(ki);
        return -1;
    }
    else if (ki->status == KB_STATUS_UNKNOWN) {
        fs_error(LOG_ERR, "cannot stop %s, runtime status unknown", kb_name);
        *err = ADM_ERR_KB_STATUS_UNKNOWN;
        fsa_kb_info_free(ki);
        return -1;
    }

    /* sanity check */
    if (ki->status != KB_STATUS_RUNNING) {
        *err = ADM_ERR_GENERIC;
        fsa_kb_info_free(ki);
        return -1;
    }

    /* check that we've got a sensible pid of the running store */
    if (ki->pid == 0) {
        fs_error(LOG_ERR, "cannot stop %s, no pid found", kb_name);
        *err = ADM_ERR_GENERIC;
        fsa_kb_info_free(ki);
        return -1;
    }

    /* send sigterm to 4s-backend process, returns 0 or -1, sets errno  */
    int rv = kill(ki->pid, SIGTERM);
    if (rv != 0) {
        *err = ADM_ERR_SEE_ERRNO;
        return -1;
        fsa_kb_info_free(ki);
    }

    fsa_kb_info_free(ki);
    return 0;
}

/* Returns -1 on error, 0 on success. Sets exit_val to exit value of
   4-backend, sets output to the output from running the command, and sets
   err to the reason for error. */
int fsab_start_local_kb(const unsigned char *kb_name, int *exit_val,
                        unsigned char **output, int *err)
{
    fsa_error(LOG_DEBUG, "starting kb '%s'", kb_name);

    /* set defaults to indicate no output or retval yet */
    *exit_val = -1;
    *output = NULL;

    fsa_kb_info *ki = fsab_get_local_kb_info(kb_name);
    if (ki == NULL) {
        *err = ADM_ERR_KB_GET_INFO;
        return -1;
    }

    /* check whether kb is running */
    if (ki->status == KB_STATUS_RUNNING) {
        fs_error(LOG_INFO, "cannot start %s, already started", kb_name);
        *err = ADM_ERR_KB_STATUS_RUNNING;
        fsa_kb_info_free(ki);
        return -1;
    }
    fsa_kb_info_free(ki);

    /* TODO check 4s-backend found in path */
    char *cmdname = FS_BIN_DIR "/4s-backend";
    int malloc_size = strlen(cmdname) + 1 + strlen((char *)kb_name) + 1;
    char *cmd = (char *)malloc(malloc_size);
    sprintf(cmd, "%s %s", cmdname, kb_name);

    fsa_error(LOG_DEBUG, "running '%s'", cmd);

    FILE *backend = popen(cmd, "r");
    free(cmd);

    if (backend == NULL) {
        fsa_error(LOG_ERR, "popen failed: %s", strerror(errno));
        *err = ADM_ERR_SEE_ERRNO;
        return -1;
    }

    /* capture output */
    char line[500];
    int size = 0;
    while (fgets(line, 500, backend) != NULL) {
        if (*output == NULL) {
            size = strlen(line) + 1;
            *output = (unsigned char *)malloc(size);
            strcpy((char *)*output, line);
        }
        else {
            size += strlen(line);
            *output = (unsigned char *)realloc(*output, size);
            strcat((char *)*output, line);
        }
    }

    fsa_error(LOG_DEBUG, "output from command is: %s", *output);

    int rv = pclose(backend);
    if (rv == -1) {
        fsa_error(LOG_ERR, "pclose failed: %s", strerror(errno));
        free(*output);
        *output = NULL;
        return ADM_ERR_SEE_ERRNO;
    }
    else {
        *exit_val = WEXITSTATUS(rv);
        fsa_error(LOG_DEBUG, "command exited with status: %d", *exit_val);
    }

    if (*exit_val == 0) {
        return 0;
    }
    else {
        return -1;
    }
}
