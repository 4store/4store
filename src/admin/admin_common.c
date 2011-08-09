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

/* Get /etc/4store.conf as GKeyFile */
GKeyFile *fsa_get_config(void)
{
    GKeyFile *config_file = g_key_file_new();
    GError *err = NULL;
    const char *filename = FS_CONFIG_FILE;
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

    cport = g_key_file_get_value(config_file, "default", "4s-boss_port", &err);

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

/* Get list of storage nodes admind host/ports from /etc/4store.conf */
fsa_node_addr *fsa_get_node_list(GKeyFile *config_file)
{
    gsize len;
    GError *err = NULL;
    gchar **nodes =
        g_key_file_get_string_list(config_file, "default", "nodes", &len, &err);

    if (nodes == NULL) {
        g_error_free(err);
        return NULL;
    }

    int default_port = fsa_get_admind_port(config_file);
    gchar *cur;
    char *tok;
    fsa_node_addr *first_na = NULL;
    fsa_node_addr *na = NULL;

    /* build list in reverse order to keep string order from conf */    
    for (int i = len-1; i >= 0; i--) {
        cur = nodes[i];

        tok = strtok(cur, ":");
        na = fsa_node_addr_new(tok);

        tok = strtok(NULL, ":");
        if (tok != NULL) {
            na->port = atoi(tok);
        }
        else {
            na->port = default_port;
        }

        na->next = first_na;
        first_na = na;
    }

    g_strfreev(nodes);

    return first_na;
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

/* used for debugging only */
void fsa_kb_info_print(const fsa_kb_info *ki)
{
    if (ki == NULL) {
        printf("kb_info is null\n");
        return;
    }

    printf("name: %s\n", ki->name);
    printf("status: %s\n", fsa_kb_info_status_to_string(ki->status));
    if (ki->ipaddr != NULL) {
        printf("ipaddr: %s\n", ki->ipaddr);
    }
    else {
        printf("host: unknown\n");
    }

    if (ki->status == KB_STATUS_RUNNING) {
        printf("pid: %d\n", ki->pid);
        printf("port: %d\n", ki->port);
    }

    if (ki->num_segments > -1) {
        printf("num_segments: %d\n", ki->num_segments);
    }
    else {
        printf("num_segments: unknown\n");
    }

    if (ki->p_segments_len > 0) {
        printf("segments:");
        for (int i = 0; i < ki->p_segments_len; i++) {
            printf(" %d", ki->p_segments_data[i]);
        }
        printf("\n");
    }
}

/* used for debugging only */
void fsa_kb_info_print_all(const fsa_kb_info *ki)
{
    const fsa_kb_info *cur = ki;
    while (cur != NULL) {
        fsa_kb_info_print(cur);
        printf("\n");
        cur = cur->next;
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
