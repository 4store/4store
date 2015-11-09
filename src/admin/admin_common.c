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

#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <stdio.h>
#include <sys/types.h>
#include <sys/socket.h>
#if defined(__OpenBSD__) || defined(__FreeBSD__)
# include <netinet/in.h>
#endif
#include <arpa/inet.h>

#include "admin_common.h"
#include "../common/params.h"
#include "../common/error.h"

#define ADM_LOG_TO_STDERR   0
#define ADM_LOG_TO_FS_ERROR 1

#ifndef __USE_GNU
char *program_invocation_name = NULL;
char *program_invocation_short_name = NULL;
#endif

/* Globals which control error/debug messages */
int fsa_log_to = ADM_LOG_TO_FS_ERROR;
int fsa_log_level = ADM_LOG_LEVEL;

/* Path to 4store executables, use FS_BIN_DIR if NULL */
static const char *bin_dir = NULL;

/* Get /etc/4store.conf as GKeyFile */
GKeyFile *fsa_get_config(void)
{
    GKeyFile *config_file = g_key_file_new();
    GError *err = NULL;
    const char *filename = fs_get_config_file();
    int rv;
    int flags = G_KEY_FILE_NONE;

    rv = g_key_file_load_from_file(config_file, filename, flags, &err);
    if (!rv || err != NULL) {
        if (err->code != G_FILE_ERROR_NOENT
            && err->code != G_FILE_ERROR_EXIST
            && err->code != G_FILE_ERROR_ISDIR) {
            fsa_error(LOG_ERR, "error reading %s: %s(%d)",
                      filename, err->message, err->code);
        }
        g_error_free(err);
        g_key_file_free(config_file);
        errno = ADM_ERR_GENERIC;
        return NULL;
    }

    errno = 0;
    return config_file;
}

void fsa_config_free(GKeyFile *config_file)
{
    if (config_file != NULL) {
        g_key_file_free(config_file);
    }
}

/* Get a port number to run or query admind on */
int fsa_get_admind_port(GKeyFile *config_file)
{
    /* if no config file, use default port */
    if (config_file == NULL) {
        return FS_ADMIND_PORT;
    }

    char *cport;
    int port;
    GError *err = NULL;

    cport = g_key_file_get_value(config_file, "4s-boss", "port", &err);

    /* if field not set in config, use default port */
    if (cport == NULL) {
        g_error_free(err);
        return FS_ADMIND_PORT;
    }

    /* if non-int value given in config file */
    if (!fsa_is_int(cport)) {
        free(cport);
        errno = ADM_ERR_BAD_CONFIG;
        return -1;
    }

    port = atoi(cport);
    free(cport);

    /* if value in config is out of range */
    if (port < 0 || port > 65535) {
        errno = ERANGE;
        return -1;
    }

    return port;
}

fsa_node_addr *fsa_node_addr_new(const char *host)
{
    fsa_node_addr *na = (fsa_node_addr *)malloc(sizeof(fsa_node_addr));
    if (na == NULL) {
        errno = ENOMEM;
        return NULL;
    }

    na->port = 0;
    na->node_num = 0;
    na->next = NULL;
    na->host = (char *)malloc(strlen(host) + 1);
    if (na->host == NULL) {
        errno = ENOMEM;
        fsa_node_addr_free(na);
        return NULL;
    }

    strcpy(na->host, host);

    return na;
}

void fsa_node_addr_free(fsa_node_addr *na)
{
    if (na == NULL) {
        return;
    }

    free(na->host);
    fsa_node_addr_free(na->next);
    free(na);
}

/* free only current node_addr, do not free rest of list */
void fsa_node_addr_free_one(fsa_node_addr *na)
{
    if (na == NULL) {
        return;
    }

    free(na->host);
    free(na);
}

/* Get list of storage nodes admind host/ports from /etc/4store.conf, default
   to localhost */
fsa_node_addr *fsa_get_node_list(GKeyFile *config_file)
{
    fsa_node_addr *first_na = NULL;
    fsa_node_addr *na = NULL;

    int default_port = fsa_get_admind_port(config_file);

    gsize len;
    GError *err = NULL;
    gchar **nodes =
        g_key_file_get_string_list(config_file, "4s-boss", "nodes", &len, &err);

    if (nodes == NULL) {
        g_error_free(err);
        first_na = fsa_node_addr_new("localhost");
        first_na->port = default_port;
        return first_na;
    }

    gchar *cur;
    char *tok;

    /* build list in reverse order to keep string order from conf */    
    for (int i = len-1; i >= 0; i--) {
        cur = nodes[i];

        if (cur[0] == '[') {
            int cur_len = strlen(cur);
            int start = 1;
            int end = 1;

            while (cur[end] != ']' && end < cur_len) {
                end += 1;
            }

            char *v6addr = (char *)malloc(end - start + 1);
            strncpy(v6addr, &cur[start], end - start);
            v6addr[end - start] = '\0';

            na = fsa_node_addr_new(v6addr);
            free(v6addr);

            end += 1;
            if (end >= cur_len || cur[end] != ':') {
                na->port = default_port;
            }
            else {
                na->port = atoi(&cur[end+1]);
            }
        }
        else {
            /* treat as v4 addr or hostname */
            tok = strtok(cur, ":");
            na = fsa_node_addr_new(tok);

            tok = strtok(NULL, ":");
            if (tok != NULL) {
                na->port = atoi(tok);
            }
            else {
                na->port = default_port;
            }
        }

        na->next = first_na;
        first_na = na;
    }

    g_strfreev(nodes);

    return first_na;
}

fsa_kb_response *fsa_kb_response_new(void)
{
    fsa_kb_response *kbr = (fsa_kb_response *)malloc(sizeof(fsa_kb_response));
    kbr->return_val = ADM_RSP_ERROR;
    kbr->kb_name = NULL;
    kbr->msg = NULL;

    return kbr;
}

void fsa_kb_response_free(fsa_kb_response *kbr)
{
    if (kbr == NULL) {
        return;
    }

    if (kbr->kb_name != NULL) {
        free(kbr->kb_name);
    }

    if (kbr->msg != NULL) {
        free(kbr->msg);
    }
    free(kbr);
}

/* Creates with same default options as current 4s-backend-setup */
fsa_kb_setup_args *fsa_kb_setup_args_new(void)
{
    fsa_kb_setup_args *ksargs =
        (fsa_kb_setup_args*)malloc(sizeof(fsa_kb_setup_args));

    ksargs->name = NULL;
    ksargs->password = NULL;
    ksargs->node_id = 0;
    ksargs->cluster_size = 1;
    ksargs->num_segments = 2;

    ksargs->mirror_segments = 0;
    ksargs->model_files = 0;
    ksargs->delete_existing = 1;

    return ksargs;
}

void fsa_kb_setup_args_free(fsa_kb_setup_args *ksargs)
{
    if (ksargs == NULL) {
        return;
    }

    if (ksargs->name != NULL) {
        free(ksargs->name);
    }

    if (ksargs->password != NULL) {
        free(ksargs->password);
    }

    free(ksargs);
}

/* Utility function to force sending of all bytes in a packet */
int fsa_sendall(int socket, unsigned char *buf, int *len)
{
    int num_bytes_sent = 0;
    int num_bytes_to_send = *len;
    int n = -1;

    while (num_bytes_sent < *len) {
        n = send(socket, buf+num_bytes_sent, num_bytes_to_send, 0);
        if (n == -1) {
            break;
        }

        num_bytes_sent += n;
        num_bytes_to_send -= n;
    }

    *len = num_bytes_sent;

    if (n == -1) {
        fsa_error(LOG_ERR, "failed to send full packet, %d bytes sent", 
                  num_bytes_sent);
        return -1;
    }

    return 0;
}

fsa_kb_info *fsa_kb_info_new(void)
{
    fsa_kb_info *ki = (fsa_kb_info *)malloc(sizeof(fsa_kb_info));
    if (ki == NULL) {
        errno = ENOMEM;
        return NULL;
    }

    ki->name = NULL;
    ki->ipaddr = NULL;
    ki->pid = 0;
    ki->port = 0;
    ki->status = KB_STATUS_UNKNOWN;
    ki->num_segments = 0;
    ki->p_segments_len = 0;
    ki->p_segments_data = NULL;
    ki->next = NULL;

    return ki;
}

void fsa_kb_info_free(fsa_kb_info *ki)
{
    if (ki == NULL) {
        return;
    }

    free(ki->name);
    free(ki->ipaddr);
    free(ki->p_segments_data);
    fsa_kb_info_free(ki->next);
    free(ki);
}

char *fsa_kb_info_status_to_string(int status)
{
    switch (status) {
        case KB_STATUS_RUNNING:
            return "running";
        case KB_STATUS_STOPPED:
            return "stopped";
        default:
            return "unknown";
    }
}

/* fetch header packet from client/server socket */
int fsa_fetch_header(int sock_fd, unsigned char *buf)
{
    int nbytes = recv(sock_fd, buf, ADM_HEADER_LEN, MSG_WAITALL);
    if (nbytes <= 0) {
        if (nbytes == 0) {
            fsa_error(LOG_DEBUG, "socket %d hung up", sock_fd);
        }
        else {
            fsa_error(LOG_ERR, "error receiving header on socket %d: %s",
                      sock_fd, strerror(errno));
        }
        close(sock_fd);
        return -1;
    }
    return nbytes;
}

/* Utility function to check if a string represents a positive integer */
int fsa_is_int(const char *str)
{
    int len = strlen(str);

    if (len == 0) {
        return 0;
    }

    for (int i = 0; i < len; i++) {
        if (str[i] < '0' || str[i] > '9') {
            return 0;
        }
    }

    return 1;
}

/* comparison function for fsa_kb_info_sort */
static int kb_info_compare_name(const fsa_kb_info* a, const fsa_kb_info *b)
{
    return strcmp((char *)a->name, (char *)b->name);
}

/* comparison function for fsa_kb_info_sort */
static int kb_info_compare_port(const fsa_kb_info* a, const fsa_kb_info *b)
{
    /* cast to signed int */
    int ia = (int)a->port;    
    int ib = (int)b->port;
    return ia - ib;
}

/* comparison function for fsa_kb_info_sort */
static int kb_info_compare_status(const fsa_kb_info* a, const fsa_kb_info *b)
{
    /* cast to signed int */
    int ia = (int)a->status;    
    int ib = (int)b->status;
    return ia - ib;
}

/* Sort kb_info list on different params */
/*
void fsa_kb_info_sort(fsa_kb_info *ki, int sort_type)
{
    int n_items = 0;
    fsa_kb_info *p = ki;

    for (p = ki; p != NULL; p = p->next) {
        n_items += 1;
    }

    if (sort_type == KB_SORT_BY_NAME) {
        qsort(ki, n_items, sizeof(fsa_kb_info), kb_info_compare_name);
    }
    else if (sort_type == KB_SORT_BY_PORT) {
        qsort(ki, n_items, sizeof(fsa_kb_info), kb_info_compare_port);
    }
    else if (sort_type == KB_SORT_BY_STATUS) {
        qsort(ki, n_items, sizeof(fsa_kb_info), kb_info_compare_status);
    }
}
*/

/* Sorting on various fields for kb_info list.
   Based on mergesort code from:
   http://www.chiark.greenend.org.uk/~sgtatham/algorithms/listsort.html */
fsa_kb_info *fsa_kb_info_sort(fsa_kb_info *list, int sort_type)
{
    if (list == NULL) {
        return NULL;
    }

    fsa_kb_info *p, *q, *e, *tail;
    int insize, nmerges, psize, qsize, i;
    int (*compare)(const fsa_kb_info*, const fsa_kb_info*);

    switch (sort_type) {
        case KB_SORT_BY_STATUS:
            compare = &kb_info_compare_status;
            break;
        case KB_SORT_BY_PORT:
            compare = &kb_info_compare_port;
            break;
        case KB_SORT_BY_NAME:
        default:
            compare = &kb_info_compare_name;
    }


    insize = 1;

    while (1) {
        p = list;
        list = NULL;
        tail = NULL;

        nmerges = 0;

        while (p != NULL) {
            nmerges += 1; /* there is a merge to be done */

            /* step insize places along from p */
            q = p;
            psize = 0;
            for (i = 0; i < insize; i++) {
                psize += 1;
                q = q->next;

                if (q == NULL) {
                    break;
                }
            }

            /* if q hasn't fallen off end, have 2 lists to merge */
            qsize = insize;

            /* merge 2 lists */
            while (psize > 0 || (qsize > 0 && q)) {
                /* work out whether next elem comes from p or q */
                if (psize == 0) {
                    /* p empty, get from q */
                    e = q;
                    q = q->next;
                    qsize -= 1;
                }
                else if (qsize == 0 || q == NULL) {
                    /* q empty, get from p */
                    e = p;
                    p = p->next;
                    psize -= 1;
                }
                else if (compare(p, q) <= 0) {
                    /* 1st elem of p is lower or same, get from p */
                    e = p;
                    p = p->next;
                    psize -= 1;
                }
                else {
                    /* 1st elem of q is lower, get from q */
                    e = q;
                    q = q->next;
                    qsize -= 1;
                }

                /* add next elem to merged list */
                if (tail != NULL) {
                    tail->next = e;
                }
                else {
                    list = e;
                }

                tail = e;
            }

            /* both p and q have stepped insize places */
            p = q;
        }

        tail->next = NULL;

        /* if only 1 merge or empty list, done, so return */
        if (nmerges <= 1) {
            return list;
        }

        /* carry on, merge lists twice the size */
        insize *= 2;
    }
}

int fsa_is_valid_kb_name(const char *kb_name)
{
    if (kb_name == NULL) {
        return 0;
    }

    char *rv;
    for (int i = 0; i < strlen(kb_name); i++) {
        rv = strchr(FS_LEGAL_KB_CHARS, kb_name[i]);
        if (rv == NULL) {
            return 0;
        }
    }

    return 1;
}

const char *fsa_get_bin_dir(void)
{
    if (bin_dir != NULL) {
        return bin_dir;
    }
    else {
        return FS_BIN_DIR;
    }
}

void fsa_set_bin_dir(const char *path)
{
    bin_dir = path;
}


/* not currently used */
/*
const char *fsa_log_level_to_string(int log_level)
{
    switch (log_level) {
        case LOG_ALERT:   return "alert";
        case LOG_CRIT:    return "crit";
        case LOG_DEBUG:   return "debug";
        case LOG_EMERG:   return "emerg";
        case LOG_ERR:     return "err";
        case LOG_INFO:    return "info";
        case LOG_NOTICE:  return "notice";
        case LOG_WARNING: return "warning";
        default:          return NULL;
    }
}
*/
