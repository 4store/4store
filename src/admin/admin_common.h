#ifndef ADMIN_COMMON_H
#define ADMIN_COMMON_H

#include <glib.h>

/*
"AC" vers command data_len data
2    1    1       2        *
*/

/* Protocol headers */
#define ADM_PROTO_VERS 0x1
#define ADM_H_LEN 2
#define ADM_H_VERS_LEN 1
#define ADM_H_CMD_LEN 1
#define ADM_H_DL_LEN 2
#define ADM_HEADER_LEN ADM_H_LEN+ADM_H_VERS_LEN+ADM_H_CMD_LEN+ADM_H_DL_LEN
#define ADM_MAX_DATA_LEN 5000
#define ADM_PKT_BUF_LEN ADM_HEADER_LEN+ADM_MAX_DATA_LEN

/* Protocol commands and responses */
#define ADM_CMD_GET_KB_PORT     1
#define ADM_RSP_GET_KB_PORT     2
#define ADM_CMD_GET_PORT_ALL    3
#define ADM_RSP_GET_PORT_ALL    4
#define ADM_CMD_GET_KB_INFO     5
#define ADM_RSP_GET_KB_INFO     6
#define ADM_CMD_GET_KB_INFO_ALL 7
#define ADM_RSP_GET_KB_INFO_ALL 8
#define ADM_RSP_ERROR         255

/* Error codes */
#define ADM_ERR_PROTO       101
#define ADM_ERR_PROTO_VERS  102
#define ADM_ERR_CONN_FAILED 103
#define ADM_ERR_BAD_CONFIG  104
#define ADM_ERR_GENERIC     105

/* Status codes */
#define KB_STATUS_RUNNING 1
#define KB_STATUS_STOPPED 2
#define KB_STATUS_UNKNOWN 3

/* Linked list of information about a KB on a storage node */
typedef struct _fsa_kb_info {
    unsigned char *name;
    unsigned char *ipaddr;
    uint32_t pid;
    uint16_t port;
    uint8_t status;
    uint8_t num_segments;
    uint8_t p_segments_len;
    uint8_t *p_segments_data;

    struct _fsa_kb_info *next;
} fsa_kb_info;

/* Linked list of storage node hosts/ports */
typedef struct _fsa_node_addr {
    char *host;
    int port;

    struct _fsa_node_addr *next;
} fsa_node_addr;

/* KB info creation + debugging functions */
fsa_kb_info *fsa_kb_info_new();
void fsa_kb_info_free(fsa_kb_info *ki);
void fsa_kb_info_print(const fsa_kb_info *ki);
void fsa_kb_info_print_all(const fsa_kb_info *ki);
char *fsa_kb_info_status_to_string(int status);

/* Storage node info  */
fsa_node_addr *fsa_node_addr_new(const char *host);
void fsa_node_addr_free(fsa_node_addr *na);
void fsa_node_addr_free_one(fsa_node_addr *na);

/* Config file reading */
GKeyFile *fsa_get_config();
void fsa_config_free(GKeyFile *config_file);
int fsa_get_admind_port(GKeyFile *config_file);
fsa_node_addr *fsa_get_node_list(GKeyFile *config_file);

/* Socket/network functions */
int fsa_sendall(int socket, unsigned char *buf, int *len);
int fsa_fetch_header(int sock_fd, unsigned char *buf);

/* Misc/utility functions */
int fsa_is_int(const char *str);

#endif
