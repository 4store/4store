#ifndef ADMIN_COMMON_H
#define ADMIN_COMMON_H

#include <glib.h>
#include <syslog.h>

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
#define ADM_CMD_STOP_KB         9
#define ADM_RSP_STOP_KB        10
#define ADM_CMD_START_KB       11
#define ADM_RSP_START_KB       12
#define ADM_RSP_ERROR         255

/* Error codes */
#define ADM_ERR_OK                0
#define ADM_ERR_PROTO             1
#define ADM_ERR_PROTO_VERS        2
#define ADM_ERR_CONN_FAILED       3
#define ADM_ERR_BAD_CONFIG        4
#define ADM_ERR_KB_GET_INFO       5
#define ADM_ERR_KB_STATUS_RUNNING 7
#define ADM_ERR_KB_STATUS_STOPPED 8
#define ADM_ERR_KB_STATUS_UNKNOWN 9
#define ADM_ERR_NETWORK          10 
#define ADM_ERR_SEE_ERRNO       254
#define ADM_ERR_GENERIC         255

/* Status codes */
#define KB_STATUS_RUNNING 1
#define KB_STATUS_STOPPED 2
#define KB_STATUS_UNKNOWN 3

/* Default port for 4s-boss - currently FS_DEFAULT_PORT-1 */
#define FS_ADMIND_PORT 6733

/* Logging settings and macro */
#define ADM_LOG_LEVEL LOG_ERR
#define ADM_LOG_TO_STDERR   0
#define ADM_LOG_TO_FS_ERROR 1

/* Sort types for kb_info */
#define KB_SORT_BY_NAME   0
#define KB_SORT_BY_PORT   1
#define KB_SORT_BY_STATUS 2

#define fsa_error(s, f...) { if (s <= fsa_log_level) { if (fsa_log_to == ADM_LOG_TO_FS_ERROR) { fs_error(s, f); } else { fprintf(stderr, "%s: ", program_invocation_short_name); fprintf(stderr, f); fprintf(stderr, "\n"); } } }

/* Define these for use if GNU extensions not enabled. Will set manually
   in 4s-boss and 4s-admin anyway */
#ifndef __USE_GNU
extern char *program_invocation_name;
extern char *program_invocation_short_name;
#endif


/* Globals used throughout code */
extern int fsa_log_to;
extern int fsa_log_level;

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
fsa_kb_info *fsa_kb_info_sort(fsa_kb_info *ki, int sort_type);
int fsa_is_valid_kb_name(const char *kb_name);
/*const char *fsa_log_level_to_string(int log_level);*/

#endif
