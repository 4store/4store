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
#include <time.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <sys/stat.h>

#include "../common/params.h"
#include "../common/4s-store-root.h"
#include "../common/error.h"
#include "../backend/metadata.h"

#include "admin_protocol.h"
#include "admin_common.h"
#include "admin_backend.h"


static int find_pid(pid_t pid) {
    int rv = kill(pid, 0);
    if (rv == -1 && errno == ESRCH) {
        return 0;
    }
    return 1;
}

/* Read runtime.info and metadata.nt to fill in info for a kb.
 * Leave ipaddr unset, caller can set if needed.
 *
 * Returns 0 on normal operation, -1 on error
 *
 * err is set to one of:
 *   ADM_ERR_SEE_ERRNO - check errno to find error
 *   ADM_ERR_GENERIC - usually std lib error where errno not set
 *   ADM_ERR_KB_NOT_EXISTS - KB requested does not exist
 *   ADM_ERR_KB_GET_INFO - KB exists, but runtime/metadata unreadable
 *   ADM_ERR_OK - no errors
 */
int fsab_kb_info_init(fsa_kb_info *ki, const unsigned char *kb_name, int *err)
{
    fsa_error(LOG_DEBUG, "init kb info for '%s'", kb_name);

    FILE *ri_file;
    int len, rv;
    char *path;
    fs_metadata *md;
    struct stat info;

    ki->name = (unsigned char *)strdup((char *)kb_name);

    /* check if kb exists */
    len = (strlen(fs_get_kb_dir_format())-2) + strlen((char *)kb_name) + 1;
    path = (char *)malloc(len * sizeof(char));
    if (path == NULL) {
        errno = ENOMEM;
        *err = ADM_ERR_SEE_ERRNO;
        return -1;
    }

    /* generate full path to kb dir */
    rv = sprintf(path, fs_get_kb_dir_format(), kb_name);
    if (rv < 0) {
        *err = ADM_ERR_GENERIC;
        fsa_error(LOG_DEBUG, "sprintf failed");
        free(path);
        return -1;
    }

    rv = stat(path, &info);
    free(path);
    if (rv == -1) {
        if (errno == ENOENT) {
            /* not an error, return empty kb info, but let caller know */
            fsa_error(LOG_DEBUG, "kb '%s' does not exist", kb_name);
            *err = ADM_ERR_KB_NOT_EXISTS;
            return 0;
        }
        else {
            fsa_error(LOG_DEBUG,
                      "stat error for kb '%s': %s", kb_name, strerror(errno));
            *err = ADM_ERR_SEE_ERRNO;
            return -1;
        }
    }

    /* alloc mem for string path to runtime.info */
    len = (strlen(fs_get_ri_file_format())-2) + strlen((char *)kb_name) + 1;
    path = (char *)malloc(len * sizeof(char));
    if (path == NULL) {
        errno = ENOMEM;
        *err = ADM_ERR_SEE_ERRNO;
        return -1;
    }

    /* generate full path to runtime.info */
    rv = sprintf(path, fs_get_ri_file_format(), kb_name);
    if (rv < 0) {
        *err = ADM_ERR_GENERIC;
        fsa_error(LOG_DEBUG, "sprintf failed");
        free(path);
        return -1;
    }

    /* attempt to open file for reading, ignore failures, but log them */
    ri_file = fopen(path, "r");

    if (ri_file == NULL) {
        fsa_error(LOG_ERR, "failed to read runtime info file at '%s': %s",
                  path, strerror(errno));
        *err = ADM_ERR_KB_GET_INFO;
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
            *err = ADM_ERR_KB_GET_INFO;
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
                *err = ADM_ERR_KB_GET_INFO;
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
            (uint16_t)atoi(fs_metadata_get_string(md, FS_MD_SEGMENTS, "0"));

        fs_rid_vector *vec = fs_metadata_get_int_vector(md, FS_MD_SEGMENT_P);
        fs_rid_vector_sort(vec);

        /* segment ID and max segments should be 256, but allow 65536
           to allow for value to be upped in #define */
        ki->p_segments_len = (uint16_t)vec->length;
        ki->p_segments_data =
            (uint16_t *)malloc(ki->p_segments_len * sizeof(uint16_t));

        for (int i = 0; i < vec->length; i++) {
            ki->p_segments_data[i] = (uint16_t)vec->data[i];
        }

        fs_rid_vector_free(vec);
        fs_metadata_close(md);

        fsa_error(LOG_DEBUG, "metadata.nt read for kb %s", kb_name);
        *err = ADM_ERR_OK;
    }
    else {
        fsa_error(LOG_ERR, "unable to read metadata.nt for kb %s", kb_name);
        *err = ADM_ERR_KB_GET_INFO;
    }

    return 0;
}

/* get all local info about a kb. Uses errno to differentiate between
   returning NULL when there are no kbs, and NULL due to a read error */
fsa_kb_info *fsab_get_local_kb_info(const unsigned char *kb_name, int *err)
{
    int rv;
    fsa_kb_info *ki = fsa_kb_info_new();

    rv = fsab_kb_info_init(ki, kb_name, err);
    if (rv == -1) {
        fsa_error(LOG_ERR, "failed to get local kb info for kb %s", kb_name);
        fsa_kb_info_free(ki);
        return NULL;
    }

    return ki;
}

/* get info on all kbs on this host */
fsa_kb_info *fsab_get_local_kb_info_all(int *err)
{
    struct dirent *entry;
    DIR *dp;
    fsa_kb_info *first_ki = NULL;
    fsa_kb_info *cur_ki = NULL;
    int rv;

    dp = opendir(fs_get_store_root());
    if (dp == NULL) {
        fsa_error(LOG_ERR, "failed to open directory '%s': %s",
                  fs_get_store_root(), strerror(errno));
        *err = ADM_ERR_GENERIC;
        return NULL;
    }

    while ((entry = readdir(dp)) != NULL) {
        if (entry->d_name[0] == '.') {
            /* skip ., .., and hidden dirs */
            continue;
        }

        cur_ki = fsa_kb_info_new();
        rv = fsab_kb_info_init(cur_ki, (unsigned char *)entry->d_name, err);
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
    *err = 0;
    return first_ki;
}


int force_kill(pid_t pid, int *err) {

    /* poll for few seconds until process is dead */
    long int nanosecs = 50000000L * 20; /* 1s */
    struct timespec req = {0, nanosecs};

    int rv = kill(pid, SIGKILL);
    if (rv != 0) {
        *err = ADM_ERR_SEE_ERRNO;
        return -1;
    }
    nanosleep(&req, NULL);
    if (!find_pid(pid)) {
        return 0;
    }
    return -1;
}

/* return 0 on success, -1 otherwise, and sets err */
int fsab_stop_local_kb(const unsigned char *kb_name,int force, int *err)
{
    fs_error(LOG_DEBUG, "stopping kb '%s'", kb_name);

    fsa_kb_info *ki = fsab_get_local_kb_info(kb_name, err);
    if (force && ki->status == KB_STATUS_INCONSISTENT) {
        return force_kill(-1 * ki->pidg, err);
    }

    if (ki == NULL) {
        return -1;
    }

    if (*err == ADM_ERR_KB_NOT_EXISTS) {
        fsa_kb_info_free(ki);
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

    /* only need pid from here on */
    int pid = ki->pid;
    fsa_kb_info_free(ki);

    /* check that we've got a sensible pid of the running store */
    if (pid == 0) {
        fs_error(LOG_ERR, "cannot stop %s, no pid found", kb_name);
        *err = ADM_ERR_GENERIC;
        return -1;
    }

    /* send sigterm to 4s-backend process, returns 0 or -1, sets errno  */
    pid_t pidg = getpgid(pid);
    int rv = kill(pid, SIGTERM);
    if (rv != 0) {
        *err = ADM_ERR_SEE_ERRNO;
        return -1;
    }

    /* poll for few seconds until process is dead */
    long int nanosecs = 50000000L; /* 0.05s */
    int n_tries = 200; /* 10s total */
    struct timespec req = {0, nanosecs};
    while (n_tries > 0) {
        rv = kill(pid, 0);
        if (rv == -1 && errno == ESRCH) {
            /* process with pid not found, kill successful */
            return 0;
        }

        /* ignore errors */
        nanosleep(&req, NULL);

        n_tries -= 1;
    }
    if (force) {
        if (find_pid(pid)) {
            force_kill(-1 * pidg,err);
            *err = ADM_ERR_KB_STATUS_STOPPED;
            return 0;
        }
    }
    *err = ADM_ERR_KILL_FAILED;
    return -1;
}

/* Execute a 4s-* command and return results
 *
 * Returns 0 on normal operation, -1 on error. Arguments should be sanitised
 * first.
 *
 * cmd - one of 4s-backend/4s-backend-destroy/4s-backend-setup
 * n_args - number of cmdline args
 * args - array of cmdline args
 * exit_val - set to return value of 4s-* cmd
 * output - set to output of the command, needs freeing
 * err - reason for any error, or ADM_ERR_OK on success
 */
static int exec_fs_cmd(const char *cmdname, int n_args, char **args,
                           int *exit_val, unsigned char **output, int *err)
{
    /* base command without arguments */
    const char *bin_dir = fsa_get_bin_dir();

    /* length of base command, excluding \0 */
    int base_cmdlen = strlen(bin_dir) + 1 + strlen(cmdname);
    int cmdlen = base_cmdlen;

    /* work out total length including arguments */
    for (int i = 0; i < n_args; i++) {
        /* whitespacespace + length of arg */
        cmdlen += 1 + strlen(args[i]);
    }

    char *cmd = (char *)malloc((cmdlen+1) * sizeof(char));
    char *p = cmd;
    int n_bytes;

    /* write path and command */
    sprintf(p, "%s/%s", bin_dir, cmdname);
    p += base_cmdlen;

    /* write arguments */
    for (int i = 0; i < n_args; i++) {
        n_bytes = sprintf(p, " %s", args[i]);
        p += n_bytes;
    }

    /* will have been null terminated by sprintf */

    fsa_error(LOG_DEBUG, "running '%s'", cmd);

    FILE *process = popen(cmd, "r");
    free(cmd);

    if (process == NULL) {
        fsa_error(LOG_ERR, "popen failed: %s", strerror(errno));
        *err = ADM_ERR_SEE_ERRNO;
        return -1;
    }

    /* capture output */
    char line[500];
    int size = 0;
    while (fgets(line, 500, process) != NULL) {
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

    int rv = pclose(process);
    if (rv == -1) {
        fsa_error(LOG_ERR, "pclose failed: %s", strerror(errno));
        free(*output);
        *output = NULL;
        *err = ADM_ERR_SEE_ERRNO;
        return -1;
    }
    else {
        *exit_val = WEXITSTATUS(rv);
        fsa_error(LOG_DEBUG, "command exited with status: %d", *exit_val);
    }

    if (*exit_val == 0) {
        *err = ADM_ERR_OK;
        return 0;
    }
    else {
        *err = ADM_ERR_POPEN;
        return -1;
    }
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

    fsa_kb_info *ki = fsab_get_local_kb_info(kb_name, err);
    if (ki == NULL) {
        /* failed to read metadata.nt, therefore kb doesn't exist */
        *err = ADM_ERR_KB_GET_INFO;
        return -1;
    }

    if (*err == ADM_ERR_KB_NOT_EXISTS) {
        fsa_kb_info_free(ki);
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

    char *cmdname = "4s-backend";
    char *args[1] = {(char *)kb_name};

    return exec_fs_cmd(cmdname, 1, args, exit_val, output, err);
}

/* Return 0 on normal operation, -1 or error */
int fsab_delete_local_kb(const unsigned char *kb_name, int *exit_val,
                         unsigned char **output, int *err)
{
    fsa_error(LOG_DEBUG, "deleting kb '%s'", kb_name);

    fsa_kb_info *ki = fsab_get_local_kb_info(kb_name, err);
    if (ki == NULL) {
        return -1;
    }

    /* if kb does not exist, nothing to do */
    if (*err == ADM_ERR_KB_NOT_EXISTS) {
        fsa_error(LOG_DEBUG,
                  "kb '%s' does not exist, nothing to delete", kb_name);
        fsa_kb_info_free(ki);
        return 0;
    }

    /* if kb exists, attempt to stop it unless it is definitely stopped */
    if (ki->status != KB_STATUS_STOPPED) {
        /* ignore errors, we're deleting store anyway */
        fsab_stop_local_kb(kb_name, 1, err);
    }
    fsa_kb_info_free(ki);

    /* use 4s-backend-destroy to delete the store */
    char *cmdname = "4s-backend-destroy";
    char *args[1] = {(char *)kb_name};

    return exec_fs_cmd(cmdname, 1, args, exit_val, output, err);
}

/* Return 0 on normal operation, -1 or error */
int fsab_create_local_kb(const fsa_kb_setup_args *ksargs, int *exit_val,
                         unsigned char **output, int *err)
{
    fsa_error(LOG_DEBUG, "creating kb '%s'", ksargs->name);

    /* check whether kb exists already, and it's state */
    fsa_kb_info *ki = fsab_get_local_kb_info(ksargs->name, err);

    /* if KB exists */
    if (ki != NULL && *err != ADM_ERR_KB_NOT_EXISTS) {
        /* do not overwrite existing kb */
        if (!ksargs->delete_existing) {
            *err = ADM_ERR_KB_EXISTS;
            fsa_error(LOG_DEBUG,
                      "kb '%s' exists, and delete_existing=0, doing nothing",
                      ksargs->name);
            fsa_kb_info_free(ki);
            return -1;
        }
        else {
            /* ok to overwrite, so stop first, ignoring errors */
            fsab_stop_local_kb(ksargs->name, 1, err);
        }
    }

    fsa_kb_info_free(ki);

    /* kb should either not exist or be stopped by now */

    /* max args length is 11 */
    char *args[11];
    int n_args = 0;
    char node_id_str[4];        /* holds uint8 */
    char cluster_size_str[4];   /* holds uint8 */
    char num_segments_str[6];   /* holds uint16 */

    sprintf(node_id_str, "%d", ksargs->node_id);
    sprintf(cluster_size_str, "%d", ksargs->cluster_size);
    sprintf(num_segments_str, "%d", ksargs->num_segments);

    args[n_args++] = "--node";
    args[n_args++] = node_id_str;
    args[n_args++] = "--cluster";
    args[n_args++] = cluster_size_str;
    args[n_args++] = "--segments";
    args[n_args++] = num_segments_str;

    if (ksargs->password != NULL) {
        args[n_args++] = "--password";
        args[n_args++] = (char *)ksargs->password;
    }

    if (ksargs->mirror_segments) {
        args[n_args++] = "--mirror";
    }

    if (ksargs->model_files) {
        args[n_args++] = "--model-files";
    }

    args[n_args++] = (char *)ksargs->name;

    /* use 4s-backend-setup to delete the store */
    char *cmdname = "4s-backend-setup";

    return exec_fs_cmd(cmdname, n_args, args, exit_val, output, err);
}
