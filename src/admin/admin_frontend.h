#ifndef ADMIN_FRONTEND_H
#define ADMIN_FRONTEND_H

#include <netdb.h>
#include "admin_protocol.h"

#define ADMIND_USAGE_NONE 0
#define ADMIND_USAGE_DEFAULT 1
#define ADMIND_USAGE_FALLBACK 2
#define ADMIND_USAGE_SOLE 3

/* Check config file to find out how and if 4s-boss lookups should be used */
int fsaf_get_admind_usage(void);

/* Connect to admin daemon on host:port. Returns sock fd, and sets ipaddr
   to IP of host (v4 or v6) */
int fsaf_connect_to_admind(char *host, int port, struct addrinfo *hints,
                           char *ipaddr);

/* Get data from admind (header already received) */
int fsaf_recv_from_admind(int sock_fd, unsigned char *buf, size_t datasize);

/* Get info on all KBs on all storage nodes */
fsa_kb_info *fsaf_fetch_kb_info_all(void);

fsa_kb_info *fsaf_fetch_kb_info(const unsigned char *kb_name,
                                fsa_node_addr *nodes);

/* Wrapper around common send/receive call to servers */
unsigned char *fsaf_send_recv_cmd(fsa_node_addr *node, int sock_fd,
                                  unsigned char *cmd, int len,
                                  int *response, int *bufsize, int *err);
#endif
