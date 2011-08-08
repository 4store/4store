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
#include "admin_frontend.h"
#include "../common/params.h"
#include "../common/error.h"

/* Get a port number to run or query admind on */
int fsaf_get_admind_usage(void)
{
    GKeyFile *conf = fsa_get_config();
    if (conf == NULL) {
        fs_error(LOG_DEBUG,
                 "could not read config file, returning default port");
        return ADMIND_USAGE_NONE;
    }
    
    char *usage_str;
    int usage = -1;
    GError *err = NULL;

    usage_str =
        g_key_file_get_value(conf, "default", "4s-boss_discovery", &err);

    /* field not set in config file */
    if (usage_str == NULL) {
        fs_error(LOG_DEBUG,
                 "no port set in config file, returning default port");
        g_error_free(err);
        fsa_config_free(conf);
        return ADMIND_USAGE_NONE;
    }

    if (strcmp(usage_str, "default") == 0) {
        usage = ADMIND_USAGE_DEFAULT;
    }
    else if (strcmp(usage_str, "fallback") == 0) {
        usage = ADMIND_USAGE_FALLBACK;
    }
    else if (strcmp(usage_str, "sole") == 0) {
        usage = ADMIND_USAGE_SOLE;
    }
    else if (strcmp(usage_str, "none") == 0) {
        usage = ADMIND_USAGE_NONE;
    }
    else {
        usage = ADM_ERR_BAD_CONFIG;
    }

    free(usage_str);
    fsa_config_free(conf);

    return usage;
}

/* returns socket fd, and sets ipaddr to IP (4 or 6) of host when connected */
int fsaf_connect_to_admind(char *host, int port, struct addrinfo *hints,
                           char *ipaddr)
{
    char cport[6]; /* 16 bit int + '\0' */
    int rv;
    int sock_fd;
    struct addrinfo *server_ai, *p;

    /* convert integer port to string */
    sprintf(cport, "%d", port);
    rv = getaddrinfo(host, cport, hints, &server_ai);
    if (rv != 0) {
        fs_error(LOG_DEBUG, "getaddrinfo failed: %s", gai_strerror(rv));
        errno = rv; /* use gai_strerror to get value */
        return -1;
    }

    /* connect to first available socket */
    for (p = server_ai; p != NULL; p = p->ai_next) {
        sock_fd = socket(p->ai_family, p->ai_socktype, p->ai_protocol);
        if (sock_fd == -1) {
            fs_error(LOG_DEBUG, "socket failed: %s", strerror(errno));
            continue;
        }

        rv = connect(sock_fd, p->ai_addr, p->ai_addrlen);
        if (rv == -1) {
            fs_error(LOG_DEBUG, "connect failed: %s", strerror(errno));
            close(sock_fd);
            continue;
        }

        /* no errors, so break */
        break;
    }


    /* check that we actually connected */
    if (p == NULL) {
        fs_error(LOG_DEBUG, "failed to connect to %s:%d", host, port);
        freeaddrinfo(server_ai);
        errno = ADM_ERR_CONN_FAILED;
        return -1;
    }

    if (server_ai->ai_family == AF_INET6) {
        struct sockaddr_in6 *ipv6 = (struct sockaddr_in6 *)server_ai->ai_addr;
        inet_ntop(AF_INET6, &(ipv6->sin6_addr),
                  ipaddr, INET6_ADDRSTRLEN);
    }
    else if (server_ai->ai_family == AF_INET) {
        struct sockaddr_in *ipv4 = (struct sockaddr_in *)server_ai->ai_addr;
        inet_ntop(AF_INET, &(ipv4->sin_addr),
                  ipaddr, INET6_ADDRSTRLEN);
    }
    freeaddrinfo(server_ai);

    fs_error(LOG_DEBUG, "connected to %s:%d (%s) on sock_fd %d",
             host, port, ipaddr, sock_fd);
    return sock_fd;
}

/* convenience wrapper around recv */
static int recv_from_admind(int sock_fd, unsigned char *buf, size_t datasize)
{
    fs_error(LOG_DEBUG, "waiting for %d bytes from client", (int)datasize);

    int nbytes = nbytes = recv(sock_fd, buf, datasize, MSG_WAITALL);
    if (nbytes <= 0) {
        if (nbytes == 0) {
            fs_error(LOG_DEBUG, "client socket %d hung up", sock_fd);
        }
        else {
            fs_error(LOG_ERR, "error receiving data from client: %s",
                     strerror(errno));
        }
        close(sock_fd);
        return nbytes;
    }

    fs_error(LOG_DEBUG, "received %d bytes from client", nbytes);
    return nbytes;
}


/* convenience function */
fsa_kb_info *fsaf_fetch_kb_info_all()
{
    return fsaf_fetch_kb_info(NULL, NULL);
}

/* Get info on a kb from any number of nodes. For all kbs, or all nodes,
   set the corresponding argument to NULL */
fsa_kb_info *fsaf_fetch_kb_info(char *kb_name, fsa_node_addr *nodes)
{
    fsa_node_addr *cur_node;

    /* if no nodes given, get all nodes from config */
    if (nodes == NULL) {
        fs_error(LOG_DEBUG, "fetching kb info for all nodes");
        GKeyFile *conf = fsa_get_config();
        nodes = fsa_get_node_list(conf);
        fsa_config_free(conf);
    }

    unsigned char *buf;
    unsigned char header_buf[ADM_HEADER_LEN]; /* store single header packet */
    int nbytes, sock_fd, rv;

    struct addrinfo hints;

    memset(&hints, 0, sizeof(hints));
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;

    /* command to be sent to all stores */
    int cmd_len; /* length of command packet */
    unsigned char *cmd_pkt; /* command packet itself */
    fsa_kb_info *ki_list = NULL;
    uint8_t cmdval;
    uint16_t datasize;
    char ipaddr[INET6_ADDRSTRLEN]; /* large enough to hold a v4 or v6 addr */

    if (kb_name == NULL) {
        /* get info on all kbs */
        fs_error(LOG_DEBUG, "fetching info for all kbs");
        cmd_pkt = fsap_encode_cmd_get_kb_info_all(&cmd_len);
    }
    else {
        /* request info on named kb */
        fs_error(LOG_DEBUG, "fetching info for kb '%s'", kb_name);
        cmd_pkt = fsap_encode_cmd_get_kb_info(kb_name, &cmd_len);
    }

    fs_error(LOG_DEBUG, "admin command length is %d bytes", cmd_len);

    /* connect to each storage node */
    for (cur_node = nodes; cur_node != NULL; cur_node = cur_node->next) {
        sock_fd = fsaf_connect_to_admind(cur_node->host, cur_node->port,
                                         &hints, ipaddr);
        if (sock_fd == -1) {
            fs_error(LOG_ERR, "failed to connect to %s:%d, skipping node",
                     cur_node->host, cur_node->port);
            continue;
        }

        /* send command to node */
        nbytes = cmd_len;
        rv = fsa_sendall(sock_fd, cmd_pkt, &nbytes);
        if (rv == -1) {
            fs_error(LOG_ERR, "failed to send command to %s:%d, skipping node",
                     cur_node->host, cur_node->port);
            continue;
        }

        fs_error(LOG_DEBUG,
                 "header (%d bytes) sent to %s:%d, waiting for response",
                 nbytes, cur_node->host, cur_node->port);

        /* get response header from node */
        rv = fsa_fetch_header(sock_fd, header_buf);
        if (rv == -1) {
            fs_error(LOG_ERR,
                     "failed to get response from %s:%d, skipping node",
                     cur_node->host, cur_node->port);
            continue;
        }

        fs_error(LOG_DEBUG, "response received from %s:%d",
                 cur_node->host, cur_node->port);

        /* server sent us data */
        rv = fsap_decode_header(header_buf, &cmdval, &datasize);
        if (rv == -1) {
            fs_error(LOG_ERR, "unable to decode header from %s:%d",
                     cur_node->host, cur_node->port);
            close(sock_fd);
            continue;
        }

        /* alloc buffer for receiving further data into */
        buf = (unsigned char *)malloc(datasize);
        if (buf == NULL) {
            errno = ENOMEM;
            free(cmd_pkt);
            return NULL;
        }

        fs_error(LOG_DEBUG, "response header from %s:%d decoded",
                 cur_node->host, cur_node->port);

        /* handle response from client */
        if (cmdval == ADM_RSP_GET_KB_INFO_ALL) {
            fs_error(LOG_DEBUG, "ADM_RSP_GET_KB_INFO_ALL received");
            fs_error(LOG_DEBUG, "fetching data from client");

            nbytes = recv_from_admind(sock_fd, buf, datasize);
            if (nbytes <= 0) {
                /* error already handled */
                free(buf);
                continue;
            }

            /* local list of kb info for a single node */
            fsa_kb_info *kid = fsap_decode_rsp_get_kb_info_all(buf);
            free(buf);
            fsa_kb_info *tmp_ki = NULL;
            fsa_kb_info *cur_ki = NULL;

            /* add kb info to list of info across nodes */
            if (kid != NULL) {
                tmp_ki = kid; /* pointer to start of list */
                cur_ki = kid;
                int done = 0;

                while (!done) {
                    cur_ki->ipaddr = (unsigned char *)strdup(ipaddr);
                    if (cur_ki->next == NULL) {
                        /* if last item in list */
                        cur_ki->next = ki_list; /* append existing vals */
                        ki_list = tmp_ki; /* point to new head */
                        done = 1;
                    }
                    else {
                        cur_ki = cur_ki->next;
                    }
                }
            }
        }
        else if (cmdval == ADM_RSP_GET_KB_INFO) {
            fs_error(LOG_DEBUG, "ADM_RSP_GET_KB_INFO_ALL received");
            fs_error(LOG_DEBUG, "fetching data from client");

            nbytes = recv_from_admind(sock_fd, buf, datasize);
            if (nbytes <= 0) {
                /* error already handled */
                free(buf);
                continue;
            }

            fsa_kb_info *kid = fsap_decode_rsp_get_kb_info(buf);
            free(buf);

            if (kid != NULL) {
                /* copy ip addr to struct */
                kid->ipaddr = (unsigned char *)strdup(ipaddr);
                kid->next = ki_list;
                ki_list = kid;
            }
        }

        close(sock_fd);
    }

    free(cmd_pkt);

    return ki_list;
}
