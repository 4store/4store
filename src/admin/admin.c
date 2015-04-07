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

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <syslog.h>
#include <getopt.h>
#include <libgen.h>
#include <errno.h>
#include <stdarg.h>
#include <math.h>
#include <glib.h>

#include <sys/types.h>
#include <sys/socket.h>
#if defined(__OpenBSD__)
# include <netinet/in.h>
#endif

#include "../common/4store.h"
#include "../common/error.h"
#include "../common/params.h"

#include "admin_protocol.h"
#include "admin_common.h"
#include "admin_frontend.h"

#define ANSI_COLOUR_RED     "\x1b[31m"
#define ANSI_COLOUR_GREEN   "\x1b[32m"
#define ANSI_COLOUR_YELLOW  "\x1b[33m"
#define ANSI_COLOUR_BLUE    "\x1b[34m"
#define ANSI_COLOUR_MAGENTA "\x1b[35m"
#define ANSI_COLOUR_CYAN    "\x1b[36m"
#define ANSI_COLOUR_RESET   "\x1b[0m"

#define V_QUIET  -1
#define V_NORMAL  0
#define V_VERBOSE 1
#define V_DEBUG   2

#define STOP_STORE  0
#define START_STORE 1

/***** Global application state *****/
/* Command line params */
static int help_flag = 0;
static int version_flag = 0;
static int verbosity = 0;
static int force = 0;

/* indexes into argv for start of command, and start of command arguments */
static int cmd_index = -1;
static int args_index = -1;

/* Enable/disable colour output using ansi escapes */
static int colour_flag = 1;

/* argc and argv used in most functions, so global for convenience */
static int argc;
static char **argv;
/***** End of global application state *****/

/* Conditionally print to stdout depending on verbosity level.
   Conditional printing to stderr should be handled by fsa_error already. */
/*
static void printv(int level, FILE *stream, const char *fmt, ...)
{
    if (verbosity < level) {
        return;
    }

    va_list argp;
    va_start(argp, fmt);
    vfprintf(stream, fmt, argp);
    va_end(argp);
    fprintf(stream, "\n");
}
*/

/* get num digits in a positive integer */
static int int_len(int n)
{
    if (n > 0) {
        return (int)log10(n) + 1;
    }

    return 1;
}

static void print_colour(const char *txt, const char *colour)
{
    if (colour_flag) {
        printf("%s%s%s", colour, txt, ANSI_COLOUR_RESET);
    }
    else {
        printf("%s", txt);
    }
}

static void print_node_line(int node_num, const char *host, int print_header)
{
    if (print_header) {
        print_colour("node_number hostname\n", ANSI_COLOUR_BLUE);
    }

    printf("%-11d %s\n", node_num, host);
}

static void print_node_status_line(int node_num, const char *host,
                                   int node_status, int host_len,
                                   int print_header)
{
    if (print_header) {
        if (colour_flag) {
            printf(ANSI_COLOUR_BLUE);
        }

        printf("node_number %-*s status\n", host_len, "hostname");

        if (colour_flag) {
            printf(ANSI_COLOUR_RESET);
        }
    }

    printf("%-11d %-*s ", node_num, host_len, host);

    if (node_status == 0) {
        print_colour("ok\n", ANSI_COLOUR_GREEN);
    }
    else {
        print_colour("unreachable\n", ANSI_COLOUR_RED);
    }
}

/* convenience function to return all nodes, or default node if none found */
static fsa_node_addr *get_storage_nodes(void)
{
    GKeyFile *config = fsa_get_config();
    int default_port = fsa_get_admind_port(config);
    fsa_node_addr *nodes = NULL;

    /* use localhost if no config file found */
    if (config == NULL) {
        fsa_error(LOG_WARNING,
                  "Unable to read config file at '%s', assuming localhost\n",
                  fs_get_config_file());
        nodes = fsa_node_addr_new("localhost");
        nodes->port = default_port;
        return nodes;
    }

    nodes = fsa_get_node_list(config);

    /* done with config */
    fsa_config_free(config);

    /* if no nodes found in config file, use localhost */
    if (nodes == NULL) {
        fsa_error(LOG_WARNING,
                  "No nodes found in '%s', assuming localhost\n",
                  fs_get_config_file());
        nodes = fsa_node_addr_new("localhost");
        nodes->port = default_port;
        return nodes;
    }

    return nodes;
}

static void number_nodes(fsa_node_addr *nodes)
{
    int n = 0;
    for (fsa_node_addr *node = nodes; node != NULL; node = node->next) {
        node->node_num = n;
        n += 1;
    }
}

/* get single node_addr based on matching host name in config file */
static fsa_node_addr *node_name_to_node_addr(char *name)
{
    GKeyFile *config = fsa_get_config();
    int default_port = fsa_get_admind_port(config);
    fsa_node_addr *nodes = NULL;

    /* use localhost if no config file found */
    if (config == NULL) {
        if (strcmp(name, "localhost") == 0) {
            nodes = fsa_node_addr_new("localhost");
            nodes->port = default_port;
            return nodes;
        }
        else {
            return NULL;
        }
    }

    nodes = fsa_get_node_list(config);

    /* done with config */
    fsa_config_free(config);

    /* if no nodes found in config file, use localhost */
    if (nodes == NULL) {
        if (strcmp(name, "localhost") == 0) {
            nodes = fsa_node_addr_new("localhost");
            nodes->port = default_port;
            return nodes;
        }
        else {
            return NULL;
        }
    }

    /* count through node list to get node node_num in */
    fsa_node_addr *cur = nodes;
    fsa_node_addr *tmp = NULL;
    while (cur != NULL) {
        if (strcmp(name, cur->host) == 0) {
            /* free rest of node list, then return the current */
            fsa_node_addr_free(cur->next);
            cur->next = NULL;
            return cur;
        }

        /* free current node then move to next */
        tmp = cur;
        cur = cur->next;
        fsa_node_addr_free_one(tmp);
    }

    return NULL;
}

/* gets host info from config file based on node number (starting at 0) */
static fsa_node_addr *node_num_to_node_addr(int node_num)
{
    /* sanity check node number */
    if (node_num < 0 || node_num >= FS_MAX_SEGMENTS) {
        return NULL;
    }

    GKeyFile *config = fsa_get_config();
    fsa_node_addr *nodes = NULL;
    int default_port = fsa_get_admind_port(config);

    /* assume localhost if no config file found */
    if (config == NULL) {
        if (node_num == 0) {
            nodes = fsa_node_addr_new("localhost");
            nodes->port = default_port;
            return nodes;
        }
        else {
            return NULL;
        }
    }

    /* if no nodes found in config file, use localhost */
    nodes = fsa_get_node_list(config);

    /* done with config */
    fsa_config_free(config);

    if (nodes == NULL) {
        if (node_num == 0) {
            nodes = fsa_node_addr_new("localhost");
            nodes->port = default_port;
            return nodes;
        }
        else {
            return NULL;
        }
    }

    /* count through node list to get node node_num in */
    int i = 0;
    fsa_node_addr *cur = nodes;
    fsa_node_addr *tmp = NULL;
    while (cur != NULL) {
        if (i == node_num) {
            /* free rest of node list, then return the current */
            fsa_node_addr_free(cur->next);
            cur->next = NULL;
            cur->node_num = node_num;
            return cur;
        }

        /* free current node then move to next */
        tmp = cur;
        cur = cur->next;
        fsa_node_addr_free_one(tmp);
        i += 1;
    }

    return NULL;
}

static int get_node_list_length(fsa_node_addr *nodes) {
    int n = 0;
    for (fsa_node_addr *node = nodes; node != NULL; node = node->next) {
        n += 1;
    }
    return n;
}

/* Print short usage message */
static void print_usage(int listhelp)
{
    printf("Usage: %s [--version] [--help] [--verbose] <command> [<args>]\n",
           program_invocation_short_name);

    if (listhelp) {
        printf("Try `%s help' or `%s help <command> for more information\n",
               program_invocation_short_name, program_invocation_short_name);
    }
}

/* Print available available commands */
static void print_help(void)
{
    if (cmd_index < 0 || args_index < 0) {
        /* print basic help */
        print_usage(0);

        printf(
"    --help             Display this message and exit\n"
"    --version          Display version information and exit\n"
"    --verbose          Increase amount of information returned\n"
"    -c, --config-file  Path and filename of configuration file to use\n"
"\n"
"Commands (see `man %s' for more info)\n"
"  list-nodes    List hostname:port and status of all known storage nodes\n"
"  list-stores   List stores, along with the nodes they're hosted on\n"
"  stop-stores   Stop a store backend process on all nodes\n"
"  start-stores  Start a store backend process on all nodes\n"
"  delete-stores Delete a store on all nodes\n"
"  create-store  Create a new store on some or all nodes\n"
"\n",
            program_invocation_short_name
        );
    }
    else {
        /* print help on a specific command */
        int i;
        if (args_index > 0) {
            i = args_index;
        }
        else {
            i = cmd_index;
        }

        if (strcmp(argv[i], "list-nodes") == 0) {
            printf(
"Usage: %s %s\n"
"List names of all storage nodes known, and checks whether or not each\n"
"node is reachable over the network.\n",
                program_invocation_short_name, argv[i]
            );
        }
        else if (strcmp(argv[i], "list-stores") == 0) {
            printf(
"Usage: %s %s [<host_name or node_number>]\n"
"List all running or stopped stores. Specify a host name or node number to \n"
"only list stores on that host.\n",
                program_invocation_short_name, argv[i]
            );
        }
        else if (strcmp(argv[i], "stop-stores") == 0) {
            printf(
"Usage: %s %s <store_names>...\n"
"       %s %s -a|--all\n"
"Stop 4s-backend processes for given stores across all nodes of the \n"
"cluster.  Either pass in a space separated list of store names, or use\n"
"the '-a' argument to stop all stores.\n",
                program_invocation_short_name, argv[i],
                program_invocation_short_name, argv[i]
            );
        }
        else if (strcmp(argv[i], "start-stores") == 0) {
            printf(
"Usage: %s %s <store_names>...\n"
"       %s %s -a|--all\n"
"Start 4s-backend processes for given stores across all nodes of the \n"
"cluster.  Either pass in a space separated list of store names, or use\n"
"the '-a' argument to start all stores.\n",
                program_invocation_short_name, argv[i],
                program_invocation_short_name, argv[i]
            );
        }
        else if (strcmp(argv[i], "delete-stores") == 0) {
            printf(
"Usage: %s %s <store_names>...\n"
"Delete store(s) on all nodes of a cluster.  Pass in a space separated \n"
"list of store names to delete.\n",
                program_invocation_short_name, argv[i]
            );
        }
        else {
            printf("%s: unrecognized command '%s'\n",
                   program_invocation_short_name, argv[i]);
        }
    }
}

/* Print version of 4store this was built with */
static void print_version(void)
{
    printf("%s, built for 4store %s\n", program_invocation_short_name, GIT_REV);
}

static int handle_create_store_opts(fsa_kb_setup_args *ksargs,
                                    fsa_node_addr **nodes)
{
    fsa_error(LOG_DEBUG, "parsing create-store arguments/options");

    int c;
    int opt_index = 0;

    /* set defaults before parsing flags */
    ksargs->mirror_segments = 0;
    ksargs->model_files = 0;
    ksargs->delete_existing = 0;

    char *nodes_arg = NULL;
    char *segments_arg = NULL;
    char *password_arg = NULL;

    struct option long_opts[] = {
        {"nodes",       required_argument, NULL,                     'n'},
        {"segments",    required_argument, NULL,                     's'},
        {"password",    required_argument, NULL,                     'p'},
        {"mirror",      no_argument,       &(ksargs->mirror_segments), 1},
        {"model-files", no_argument,       &(ksargs->model_files),     1},
        {"force",       no_argument,       &(ksargs->delete_existing), 1},
        {NULL,          0,                 NULL,                       0}
    };

    while(1) {
        c = getopt_long(argc, argv, "", long_opts, &opt_index);

        if (c == -1) {
            break;
        }

        switch(c) {
            case 0:
                /* flag set */
                break;
            case 'n':
                nodes_arg = optarg;
                break;
            case 's':
                segments_arg = optarg;
                break;
            case 'p':
                password_arg = optarg;
                break;
            case '?':
                print_usage(1);
                return 1;
                break;
            default:
                abort();
        }
    }

    /* if we don't have exactly one argument remaining */
    if (argc - optind != 1) {
        fsa_error(LOG_ERR, "Exactly one store name must be specified");
        print_usage(1);
        return 1;
    }

    /* validate kb name argument */
    if (!fsa_is_valid_kb_name(argv[optind])) {
        fsa_error(LOG_ERR, "'%s' is not a valid store name", argv[optind]);
        return 1;
    }
    ksargs->name = (unsigned char *)strdup(argv[optind]);
    /* end of kb name validation */

    if (password_arg != NULL) {
        ksargs->password = (unsigned char *)strdup(password_arg);
    }

    if (nodes_arg == NULL) {
        /* use all nodes if no args specified */
        *nodes = get_storage_nodes();
        number_nodes(*nodes);
    }
    else {
        /* parse --nodes argument */
        fsa_node_addr *prev_node = NULL;
        char *cur = nodes_arg; /* point to start of string */

        /* check chars in nodelist */
        for (int i = 0; i < strlen(nodes_arg); i++) {
            if (nodes_arg[i] != ','
                && (nodes_arg[i] < '0' || nodes_arg[i] > '9')) {
                fsa_error(LOG_ERR, "Invalid nodes argument: %s\n", nodes_arg);
                return 1;
            }
        }

        while (cur != NULL && cur != '\0') {
            errno = 0;
            int node_num = (int)strtol(cur, (char **)NULL, 10);
            if (node_num == 0 && errno != 0) {
                fsa_error(LOG_ERR, "Invalid node number: %s\n", cur);
                return 1;
            }

            fsa_node_addr *node = node_num_to_node_addr(node_num);
            if (node == NULL) {
                fsa_error(
                    LOG_ERR,
                    "Invalid node number, no node with ID %d in cluster\n",
                    node_num
                );
                return 1;
            }

            if (prev_node == NULL) {
                /* set pointer to first node in list */
                *nodes = node;
            }
            else {
                /* else append to list so that order is preserved */
                prev_node->next = node;
            }
            prev_node = node;

            /* move to next ',' */
            cur = strchr(cur, ',');
            if (cur != NULL && cur != '\0') {
                /* if not at end of string, move past ',' */
                cur += 1;
            }
        }
    }

    if (*nodes == NULL) {
        fsa_error(LOG_ERR, "No nodes specified\n");
        return 1;
    }

    ksargs->cluster_size = get_node_list_length(*nodes);
    /* end of nodes validation */

    /* validate segments argument */
    int n_segments;

    if (segments_arg == NULL) {
        /* round up to nearest power of 2 */
        int32_t ns = 2 * ksargs->cluster_size;
        ns -= 1;
        ns |= ns >> 1;
        ns |= ns >> 2;
        ns |= ns >> 4;
        ns |= ns >> 8;
        ns |= ns >> 16;
        ns += 1;
        n_segments = ns;
    }
    else {
        n_segments = (int)strtol(segments_arg, (char **)NULL, 10);
        if (n_segments == 0 && errno != 0) {
            fsa_error(LOG_ERR, "Invalid number of segments: %s\n",
                      segments_arg);
            return 1;
        }
    }

    if (n_segments % 2 != 0) {
        fsa_error(LOG_ERR, "Number of segments must be a power of 2");
        return 1;
    }

    if (n_segments < 2 || n_segments > FS_MAX_SEGMENTS) {
        fsa_error(LOG_ERR, "Number of segments must be between 2 and %d",
                  FS_MAX_SEGMENTS);
        return 1;
    }

    ksargs->num_segments = n_segments;
    /* end of segments validation */

    return 0;
}

/* Parse command line opts/args into variables */
static int parse_cmdline_opts(int argc)
{
    fsa_error(LOG_DEBUG, "parsing command line arguments/options");

    int c;
    int opt_index = 0;

    /* verbosity level flags */
    int verbose_flag = 0;
    int quiet_flag = 0;
    int debug_flag = 0;

    struct option long_opts[] = {
        {"help",        no_argument,        &help_flag,     1},
        {"quiet",       no_argument,        &quiet_flag,    1},
        {"verbose",     no_argument,        &verbose_flag,  1},
        {"debug",       no_argument,        &debug_flag,    1},
        {"version",     no_argument,        &version_flag,  1},
        {"config-file", required_argument,  NULL,           'c'},
        {"force",       no_argument,        &force, 1},
        {NULL,          0,                  NULL,           0}
    };

    while(1) {
        c = getopt_long(argc, argv, "+c:", long_opts, &opt_index);

        /* end of options */
        if (c == -1) {
            break;
        }

        switch(c) {
            case 0:
                /* option set flag, do nothing */
                break;
            case '?':
                /* getopt_long has already printed err msg */
                print_usage(1);
                return 1;
            case 'c':
                fs_set_config_file(optarg);
                break;
            default:
                abort();
        }
    }

    /* set verbosity based on options */
    if (debug_flag) {
        verbosity = V_DEBUG;
        fsa_log_level = LOG_DEBUG;
    }
    else if (verbose_flag) {
        verbosity = V_VERBOSE;
    }
    else if (quiet_flag) {
        verbosity = V_QUIET;
    }
    else {
        verbosity = V_NORMAL;
    }

    /* get admin command */
    if (optind < argc) {
        cmd_index = optind;
        optind += 1; /* move to start of arguments to command */
        if (strcmp(argv[cmd_index], "help") == 0) {
            help_flag = 1;
        }
        else if (strcmp(argv[cmd_index], "version") == 0) {
            version_flag = 1;
        }

        if (optind < argc) {
            /* track start of arguments to command */
            args_index = optind;
        }
    }

    return 0;
}

static void print_invalid_arg(void)
{
    printf("Invalid argument to '%s': %s\n",
           argv[cmd_index], argv[args_index]);
}

/* Used to handle store starting/stopping, interface is mostly identical */
static int start_or_stop_stores(int action)
{
    int all = 0; /* if -a flag, then start/stop all, instead of kb name */
    int kb_name_len = 11;

    if (args_index < 0) {
        /* no argument given, display help text */
        fsa_error(
            LOG_ERR,
            "No store name(s) given. Use `%s help %s' for details",
            program_invocation_short_name, argv[cmd_index]
        );
        return 1;
    }

    if (strcmp("-a", argv[args_index]) == 0
        || strcmp("--all", argv[args_index]) == 0) {
        all = 1; /* all stores */
    }
    else {
        /* check for invalid store names */
        for (int i = args_index; i < argc; i++) {
            if (!fsa_is_valid_kb_name(argv[i])) {
                fsa_error(LOG_ERR, "'%s' is not a valid store name", argv[i]);
                return 1;
            }

            int len = strlen(argv[i]);
            if (len > kb_name_len) {
                kb_name_len = len;
            }
        }
    }

    /* get list of all storage nodes */
    fsa_node_addr *nodes = get_storage_nodes();

    /* Setup hints information */
    struct addrinfo hints;
    memset(&hints, 0, sizeof(hints));
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;

    int sock_fd, len;
    char ipaddr[INET6_ADDRSTRLEN];
    unsigned char *buf;

    int node_num = 0;
    unsigned char *cmd = NULL;
    int n_errors = 0;

    /* to be set by fsa_send_recv_cmd */
    int response, bufsize, err;

    int print_node_header = 1;
    int print_store_header = 1;

    for (fsa_node_addr *n = nodes; n != NULL; n = n->next) {
        sock_fd = fsaf_connect_to_admind(n->host, n->port, &hints, ipaddr);

        print_node_line(node_num, n->host, print_node_header);
        print_node_header = 0;

        node_num += 1;

        if (sock_fd == -1) {
            print_colour("unreachable\n", ANSI_COLOUR_RED);
            continue;
        }

        if (all) {
            /* start/stop all kbs */
            if (action == STOP_STORE) {
                cmd = fsap_encode_cmd_stop_kb_all(&len,force);
            }
            else if (action == START_STORE) {
                cmd = fsap_encode_cmd_start_kb_all(&len);
            }

            if (cmd == NULL) {
                fsa_error(LOG_CRIT, "failed to encode %s command",
                          argv[cmd_index]);
                n_errors += 1;
                break;
            }

            fsa_error(LOG_DEBUG, "sending '%s' command to %s:%d",
                      argv[cmd_index], n->host, n->port);

            /* send command and get reply */
            buf = fsaf_send_recv_cmd(n, sock_fd, cmd, len,
                                     &response, &bufsize, &err);

            /* usually a network error */
            if (buf == NULL) {
                /* error already handled */
                n_errors += 1;
                break;
            }

            /* should get this if all went well */
            if (response == ADM_RSP_EXPECT_N_KB) {
                int rv;
                uint8_t rspval;
                uint16_t datasize;
                unsigned char header_buf[ADM_HEADER_LEN];
                fsa_kb_response *kbr = NULL;

                int max_kb_len;
                int expected_responses =
                    fsap_decode_rsp_expect_n_kb(buf, &max_kb_len);
                free(buf);

                fsa_error(LOG_DEBUG, "expecting %d responses from server",
                          expected_responses);

                /* print header */
                if (print_store_header) {
                    if (colour_flag) {
                        printf(ANSI_COLOUR_BLUE);
                    }

                    printf("  %-*s status\n", max_kb_len, "store_name");

                    if (colour_flag) {
                        printf(ANSI_COLOUR_RESET);
                    }
                }

                /* get packet from server for each kb started/stopped */
                for (int i = 0; i < expected_responses; i++) {
                    rv = fsa_fetch_header(sock_fd, header_buf);
                    if (rv == -1) {
                        fsa_error(LOG_ERR,
                                  "failed to get response from %s:%d",
                                  n->host, n->port);
                        break;
                    }

                    fsa_error(LOG_DEBUG, "got header %d/%d",
                              i+1, expected_responses);

                    rv = fsap_decode_header(header_buf, &rspval, &datasize);
                    if (rv == -1) {
                        fsa_error(LOG_CRIT,
                                  "unable to decode header from %s:%d",
                                  n->host, n->port);
                        break;
                    }

                    if (rspval == ADM_RSP_ABORT_EXPECT) {
                        fsa_error(LOG_ERR, "operation aborted by server");
                        break;
                    }

                    buf = (unsigned char *)malloc(datasize);
                    rv = fsaf_recv_from_admind(sock_fd, buf, datasize);
                    if (rv < 0) {
                        /* error already handled/logged */
                        free(buf);
                        break;
                    }

                    if (rspval == ADM_RSP_STOP_KB) {
                        kbr = fsap_decode_rsp_stop_kb(buf);
                        printf("  %-*s ", max_kb_len, kbr->kb_name);
                        switch (kbr->return_val) {
                            case ADM_ERR_OK:
                            case ADM_ERR_KB_STATUS_STOPPED:
                                print_colour("stopped", ANSI_COLOUR_GREEN);
                                break;
                            case ADM_ERR_KB_STATUS_UNKNOWN:
                                print_colour("unknown", ANSI_COLOUR_RED);
                                break;
                            default:
                                fsa_error(
                                    LOG_CRIT,
                                    "Unknown server response: %d",
                                    kbr->return_val
                                );
                                print_colour("unknown", ANSI_COLOUR_RED);
                                break;
                        }
                    }
                    else if (rspval == ADM_RSP_START_KB) {
                        kbr = fsap_decode_rsp_start_kb(buf);
                        printf("  %-*s ", max_kb_len, kbr->kb_name);
                        switch (kbr->return_val) {
                            case ADM_ERR_OK:
                            case ADM_ERR_KB_STATUS_RUNNING:
                                print_colour("running", ANSI_COLOUR_GREEN);
                                break;
                            case ADM_ERR_KB_STATUS_STOPPED:
                                print_colour("stopped", ANSI_COLOUR_YELLOW);
                                break;
                            case ADM_ERR_KB_STATUS_UNKNOWN:
                                print_colour("unknown", ANSI_COLOUR_RED);
                                break;
                            default:
                                fsa_error(
                                    LOG_CRIT,
                                    "Unknown server response: %d",
                                    kbr->return_val
                                );
                                print_colour("unknown", ANSI_COLOUR_RED);
                                break;
                        }
                    }
                    printf("\n");

                    free(buf);
                    buf = NULL;
                    fsa_kb_response_free(kbr);
                    kbr = NULL;
                }
            }
            else if (response == ADM_RSP_ERROR) {
                unsigned char *errmsg = fsap_decode_rsp_error(buf, bufsize);
                fsa_error(LOG_ERR, "server error: %s", errmsg);
                free(errmsg);
                free(buf);
            }
            else {
                fsa_error(LOG_ERR, "unexpected response from server: %d",
                          response);
                free(buf);
            }
        }
        else {
            /* print header */
            if (print_store_header) {
                if (colour_flag) {
                    printf(ANSI_COLOUR_BLUE);
                }

                printf("  %-*s status\n", kb_name_len, "store_name");

                if (colour_flag) {
                    printf(ANSI_COLOUR_RESET);
                }

                print_store_header = 0;
            }

            /* stop kbs given on command line */
            for (int i = args_index; i < argc; i++) {
                /* send start/stop command for each kb */
                if (action == STOP_STORE) {
                    cmd = fsap_encode_cmd_stop_kb((unsigned char *)argv[i],
                                                  &len,force);
                }
                else if (action == START_STORE) {
                    cmd = fsap_encode_cmd_start_kb((unsigned char *)argv[i],
                                                   &len);
                }

                if (cmd == NULL) {
                    fsa_error(LOG_CRIT, "failed to encode %s command",
                            argv[cmd_index]);
                    n_errors += 1;
                    break;
                }

                fsa_error(LOG_DEBUG, "sending %s '%s' command to %s:%d",
                          argv[cmd_index], argv[i], n->host, n->port);

                buf = fsaf_send_recv_cmd(n, sock_fd, cmd, len,
                                         &response, &bufsize, &err);
                free(cmd);
                cmd = NULL;

                /* usually a network error */
                if (buf == NULL) {
                    /* error already handled */
                    n_errors += 1;
                    break;
                }

                printf("  %-*s ", kb_name_len, argv[i]);

                fsa_kb_response *kbr = NULL;

                if (response == ADM_RSP_STOP_KB) {
                    kbr = fsap_decode_rsp_stop_kb(buf);
                    switch (kbr->return_val) {
                        case ADM_ERR_OK:
                        case ADM_ERR_KB_STATUS_STOPPED:
                            print_colour("stopped", ANSI_COLOUR_GREEN);
                            break;
                        case ADM_ERR_KB_NOT_EXISTS:
                            print_colour("store_not_found", ANSI_COLOUR_RED);
                            break;
                        default:
                            print_colour("unknown", ANSI_COLOUR_RED);
                            break;
                    }
                    printf("\n");
                    fsa_kb_response_free(kbr);
                }
                else if (response == ADM_RSP_START_KB) {
                    kbr = fsap_decode_rsp_start_kb(buf);
                    switch (kbr->return_val) {
                        case ADM_ERR_OK:
                        case ADM_ERR_KB_STATUS_RUNNING:
                            print_colour("running", ANSI_COLOUR_GREEN);
                            break;
                        case ADM_ERR_KB_STATUS_STOPPED:
                            print_colour("stopped", ANSI_COLOUR_YELLOW);
                            break;
                        case ADM_ERR_KB_NOT_EXISTS:
                            print_colour("store_not_found", ANSI_COLOUR_RED);
                            break;
                        default:
                            print_colour("unknown", ANSI_COLOUR_RED);
                            break;
                    }
                    printf("\n");
                    fsa_kb_response_free(kbr);
                }
                else if (response == ADM_RSP_ERROR) {
                    unsigned char *msg = fsap_decode_rsp_error(buf, bufsize);
                    printf("unknown: %s\n", msg);
                    free(msg);
                    n_errors += 1;
                }
                else {
                    fsa_error(LOG_CRIT, "unknown response from server");
                    n_errors += 1;
                }

                free(buf);
            }
        }

        /* done with current server */
        close(sock_fd);
    }

    if (n_errors > 0) {
        return 1;
    }
    return 0;
}

static int cmd_stop_stores(void)
{
    return start_or_stop_stores(STOP_STORE);
}

static int cmd_start_stores(void)
{
    return start_or_stop_stores(START_STORE);
}

/* convenience func to create int array of socket fds */
static int *init_sock_fds(int n)
{
    int *sock_fds = (int *)malloc(n * sizeof(int));
    for (int i = 0; i < n; i++) {
        sock_fds[i] = -1;
    }
    return sock_fds;
}

static void cleanup_sock_fds(int *sock_fds, int n)
{
    for (int i = 0; i < n; i++) {
        if (sock_fds[i] != -1) {
            close(sock_fds[i]);
        }
    }
    free(sock_fds);
}

/* convenience function since this is used a few times */
static int get_max_node_name_length(fsa_node_addr *nodes, int max)
{
    for (fsa_node_addr *node = nodes; node != NULL; node = node->next) {
        int len = strlen(node->host);
        if (len > max) {
            max = len;
        }
    }
    return max;
}

static int cmd_create_store(void)
{
    int rv;
    fsa_kb_setup_args *ksargs = fsa_kb_setup_args_new();

    /* check then parse remaining cmd line args into struct */
    fsa_node_addr *nodes = NULL;
    rv = handle_create_store_opts(ksargs, &nodes);
    if (rv != 0) {
        return rv;
    }

    /* nodes guaranteed to be non-null */

    int node_name_len = get_max_node_name_length(nodes, 11);
    int n_nodes = get_node_list_length(nodes);

    /* connections to each node */
    int *sock_fds = init_sock_fds(n_nodes);

    /* Setup hints information */
    struct addrinfo hints;
    memset(&hints, 0, sizeof(hints));
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;

    char ipaddr[INET6_ADDRSTRLEN];
    int print_node_header = 1;
    int node_status;

    printf("Checking cluster status:\n");

    int cur_node = 0;
    for (fsa_node_addr *node = nodes; node != NULL; node = node->next) {
        sock_fds[cur_node] =
            fsaf_connect_to_admind(node->host, node->port, &hints, ipaddr);

        if (sock_fds[cur_node] == -1) {
                node_status = -1;
        }
        else {
            node_status = 0;
        }

        print_node_status_line(node->node_num, node->host, node_status,
                               node_name_len, print_node_header);
        print_node_header = 0;

        cur_node += 1;
    }

    /* check that we've got a connection to all nodes */
    for (int i = 0; i < n_nodes; i++) {
        if (sock_fds[i] == -1) {
            cleanup_sock_fds(sock_fds, n_nodes);
            printf("Failed to connect to all nodes, aborting.\n");
            fsa_kb_setup_args_free(ksargs);
            fsa_node_addr_free(nodes);
            return 1;
        }
    }
    printf("\n");

    /* all nodes responding, attempt to create store on each */
    int response, bufsize, err, len;
    int n_errors = 0;
    int n_created = 0; /* incremented on successful kb creation */
    unsigned char *buf;

    if (colour_flag) {
        printf(ANSI_COLOUR_BLUE);
    }

    printf("node_number store_status");

    if (colour_flag) {
        printf(ANSI_COLOUR_RESET);
    }

    printf("\n");

    fsa_error(LOG_DEBUG, "ksargs len is: %d", len);
    fsa_error(LOG_DEBUG, "ksargs->name: %s", ksargs->name);
    fsa_error(LOG_DEBUG, "ksargs->password: %s", ksargs->password);
    fsa_error(LOG_DEBUG, "ksargs->node_id: %d", ksargs->node_id);
    fsa_error(LOG_DEBUG, "ksargs->cluster_size: %d", ksargs->cluster_size);
    fsa_error(LOG_DEBUG, "ksargs->num_segments: %d", ksargs->num_segments);
    fsa_error(LOG_DEBUG, "ksargs->mirror_segments: %d",ksargs->mirror_segments);
    fsa_error(LOG_DEBUG, "ksargs->model_files: %d", ksargs->model_files);
    fsa_error(LOG_DEBUG, "ksargs->delete_existing: %d",ksargs->delete_existing);

    unsigned char *cmd = NULL;
    cur_node = 0;
    for (fsa_node_addr *node = nodes; node != NULL; node = node->next) {
        ksargs->node_id =cur_node;

        cmd = fsap_encode_cmd_create_kb(ksargs, &len);
        if (cmd == NULL) {
            fsa_error(LOG_CRIT, "failed to encode %s command",
                      argv[cmd_index]);
            fsa_kb_setup_args_free(ksargs);
            fsa_node_addr_free(nodes);
            cleanup_sock_fds(sock_fds, n_nodes);
            return 1;
        }

        fsa_error(LOG_DEBUG, "sending %s '%s' command to %s:%d",
                  argv[cmd_index], ksargs->name, node->host, node->port);

        buf = fsaf_send_recv_cmd(node, sock_fds[cur_node], cmd, len,
                                 &response, &bufsize, &err);

        /* usually a network error */
        if (buf == NULL) {
            /* error already handled */
            n_errors += 1;
            break;
        }

        if (response == ADM_RSP_CREATE_KB) {
            fsa_kb_response *kbr = fsap_decode_rsp_create_kb(buf);

            switch (kbr->return_val) {
                case ADM_ERR_OK:
                    fsa_error(
                        LOG_DEBUG,
                        "kb '%s' created on node %s",
                        kbr->kb_name, node->host
                    );
                    printf("%-11d ", node->node_num);
                    print_colour("created", ANSI_COLOUR_GREEN);
                    printf("\n");
                    n_created += 1;
                    break;
                case ADM_ERR_KB_EXISTS:
                    fsa_error(
                        LOG_ERR,
                        "kb '%s' already exists on node %s",
                        kbr->kb_name, node->host
                    );
                    n_errors += 1;
                    break;
                case ADM_ERR_POPEN:
                case ADM_ERR_GENERIC:
                default:
                    fsa_error(
                        LOG_ERR,
                        "failed to create kb '%s' on node %s",
                        kbr->kb_name, node->host
                    );
                    n_errors += 1;
                    break;
            }

            fsa_kb_response_free(kbr);
        }
        else if (response == ADM_RSP_ERROR) {
            unsigned char *errmsg = fsap_decode_rsp_error(buf, bufsize);
            fsa_error(LOG_ERR, "server error: %s", errmsg);
            free(errmsg);
            free(buf);
            n_errors += 1;
            break;
        }
        else {
            fsa_error(LOG_CRIT, "unknown response from server");
            free(buf);
            n_errors += 1;
            break;
        }

        free(buf);
        if (n_errors > 0) {
            break;
        }

        cur_node += 1;
    }

    if (cmd != NULL) {
        free(cmd);
    }

    /* delete any stores already created, ignore errors */
    if (n_errors > 0 && n_created > 0) {
        int cmd_len;
        unsigned char *delete_cmd =
            fsap_encode_cmd_delete_kb(ksargs->name, &cmd_len);

        if (cmd == NULL) {
            fsa_error(LOG_CRIT, "failed to encode delete_stores command");
        }
        else {
            cur_node = 0;
            for (fsa_node_addr *node = nodes; node != NULL; node = node->next) {
                fsa_error(LOG_DEBUG,
                          "sending delete-store '%s' command to %s:%d",
                          ksargs->name, node->host, node->port);

                fsaf_send_recv_cmd(node, sock_fds[cur_node], delete_cmd,
                                   cmd_len, &response, &bufsize, &err);
                cur_node += 1;

                if (cur_node == n_created) {
                    /* only delete as many as we created */
                    break;
                }
            }
        }
    }

    /* cleanup */
    cleanup_sock_fds(sock_fds, n_nodes);
    fsa_kb_setup_args_free(ksargs);
    fsa_node_addr_free(nodes);

    if (n_errors > 0) {
        printf(
          "Store creation aborted.\n"
        );
        return 1;
    }
    return 0;
}

static int cmd_delete_stores(void)
{
    if (args_index < 0) {
        /* no argument given, display help text */
        fsa_error(
            LOG_ERR,
            "No store name(s) given. Use `%s help %s' for details",
            program_invocation_short_name, argv[cmd_index]
        );
        return 1;
    }

    /* check for invalid store names */
    int store_name_len = 11;
    for (int i = args_index; i < argc; i++) {
        if (!fsa_is_valid_kb_name(argv[i])) {
            fsa_error(LOG_ERR, "'%s' is not a valid store name", argv[i]);
            return 1;
        }

        int len = strlen(argv[i]);
        if (len > store_name_len) {
            store_name_len = len;
        }
    }

    /* get list of all storage nodes */
    fsa_node_addr *node;
    fsa_node_addr *nodes = get_storage_nodes();
    int n_nodes = 0;
    int node_name_len = 9;
    for (node = nodes; node != NULL; node = node->next) {
        int len = strlen(node->host);
        if (len > node_name_len) {
            node_name_len = len;
        }

        n_nodes += 1;
    }

    /* connections to each node */
    int *sock_fds = init_sock_fds(n_nodes);

    /* Setup hints information */
    struct addrinfo hints;
    memset(&hints, 0, sizeof(hints));
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;

    char ipaddr[INET6_ADDRSTRLEN];
    int cur_node = 0;
    int print_node_header = 1;
    int node_status;

    printf("Checking cluster status:\n");

    for (fsa_node_addr *n = nodes; n != NULL; n = n->next) {
        sock_fds[cur_node] =
            fsaf_connect_to_admind(n->host, n->port, &hints, ipaddr);

        if (sock_fds[cur_node] == -1) {
            node_status = -1;
        }
        else {
            node_status = 0;
        }

        print_node_status_line(cur_node, n->host, node_status,
                               node_name_len, print_node_header);
        print_node_header = 0;

        cur_node += 1;
    }

    /* check that we've got a connection to all nodes */
    for (int i = 0; i < n_nodes; i++) {
        if (sock_fds[i] == -1) {
            cleanup_sock_fds(sock_fds, n_nodes);
            printf("Failed to connect to all nodes, aborting.\n");
            return 1;
        }
    }
    printf("\n");

    int response, bufsize, err, len;
    int n_errors = 0;
    int n_deleted;
    unsigned char *cmd;
    unsigned char *buf;
    int print_store_header = 1;

    /* delete one kb at a time across all nodes */
    for (int i = args_index; i < argc; i++) {
        cur_node = 0;
        n_deleted = 0;
        cmd = fsap_encode_cmd_delete_kb((unsigned char *)argv[i], &len);
        if (cmd == NULL) {
            fsa_error(LOG_CRIT, "failed to encode %s command",
                      argv[cmd_index]);
            n_errors += 1;
            break;
        }

        for (node = nodes; node != NULL; node = node->next) {
            fsa_error(LOG_DEBUG, "sending %s '%s' command to %s:%d",
                      argv[cmd_index], argv[i], node->host, node->port);

            buf = fsaf_send_recv_cmd(node, sock_fds[cur_node], cmd, len,
                                     &response, &bufsize, &err);

            /* usually a network error */
            if (buf == NULL) {
                /* error already handled */
                n_errors += 1;
                break;
            }

            if (response == ADM_RSP_DELETE_KB) {
                fsa_kb_response *kbr = fsap_decode_rsp_delete_kb(buf);

                switch (kbr->return_val) {
                    case ADM_ERR_OK:
                        fsa_error(
                            LOG_DEBUG,
                            "kb '%s' deleted on node %s",
                            kbr->kb_name, node->host
                        );
                        n_deleted += 1;
                        break;
                    case ADM_ERR_KB_STATUS_UNKNOWN:
                        /* kb deleted, but runtime status not confirmed */
                        fsa_error(
                            LOG_DEBUG,
                            "kb '%s' deleted on node %s, but runtime status "
                            "not confirmed",
                            kbr->kb_name, node->host
                        );
                        n_deleted += 1;
                        break;
                    case ADM_ERR_KB_NOT_EXISTS:
                        fsa_error(
                            LOG_DEBUG,
                            "kb '%s' does not exist on node %s",
                            kbr->kb_name, node->host
                        );
                        break;
                    case ADM_ERR_KB_GET_INFO:
                        fsa_error(
                            LOG_ERR,
                            "failed to get info for kb '%s' on node %s",
                            kbr->kb_name, node->host
                        );
                        n_errors += 1;
                        break;
                    case ADM_ERR_POPEN:
                    case ADM_ERR_GENERIC:
                    default:
                        fsa_error(
                            LOG_ERR,
                            "failed to delete kb '%s' on node %s",
                            kbr->kb_name, node->host
                        );
                        n_errors += 1;
                        break;
                }

                fsa_kb_response_free(kbr);
            }
            else if (response == ADM_RSP_ERROR) {
                unsigned char *errmsg = fsap_decode_rsp_error(buf, bufsize);
                fsa_error(LOG_ERR, "server error: %s", errmsg);
                free(errmsg);
                free(buf);
                n_errors += 1;
                break;
            }
            else {
                fsa_error(LOG_CRIT, "unknown response from server");
                free(buf);
                n_errors += 1;
                break;
            }

            free(buf);

            if (n_errors > 0) {
                break;
            }
            cur_node += 1;
        }

        free(cmd);

        if (n_errors > 0) {
            break;
        }

        if (print_store_header) {
            if (colour_flag) {
                printf(ANSI_COLOUR_BLUE);
            }

            printf("%-*s store_status\n", store_name_len, "store_name");

            if (colour_flag) {
                printf(ANSI_COLOUR_RESET);
            }

            print_store_header = 0;
        }

        printf("%-*s ", store_name_len, argv[i]);

        /* else kb deleted on all nodes */
        if (n_deleted == 0) {
            print_colour("store_not_found\n", ANSI_COLOUR_YELLOW);
        }
        else {
            print_colour("deleted\n", ANSI_COLOUR_GREEN);
        }
    }

    /* cleanup */
    cleanup_sock_fds(sock_fds, n_nodes);

    if (n_errors > 0) {
        printf(
          "An error occurred while deleting stores, check status manually\n"
        );
        return 1;
    }
    return 0;
}

/* convenience function to avoid code duplication */
static fsa_node_addr *get_nodes_from_cmd_line(void)
{
    fsa_node_addr *nodes = NULL;

    if (args_index < 0) {
        /* no arguments, so get all nodes */
        nodes = get_storage_nodes();
    }
    else {
        /* optional host name or node number is given, use single node  */
        char *host_or_nodenum = argv[args_index];
        args_index += 1;
        if (args_index != argc) {
            /* shouldn't be any args after this one */
            print_invalid_arg();
            return NULL;
        }

        /* check whether we have a hostname or a node number */
        if (fsa_is_int(host_or_nodenum)) {
            /* get host by number, starting from 0 */
            int node_num = atoi(host_or_nodenum);
            nodes = node_num_to_node_addr(node_num);
            if (nodes == NULL) {
                fsa_error(LOG_ERR, "Node number '%d' not found in config\n",
                          node_num);
                return NULL;
            }
        }
        else {
            /* get host by name */
            nodes = node_name_to_node_addr(host_or_nodenum);
            if (nodes == NULL) {
                fsa_error(LOG_ERR,
                          "Node with name '%s' not found in config\n",
                          host_or_nodenum);
                return NULL;
            }
        }
    }

    return nodes;
}


static int cmd_list_stores_verbose(void)
{
    fsa_node_addr *nodes = get_nodes_from_cmd_line();
    if (nodes == NULL) {
        /* error messages already printed */
        return 1;
    }

    fsa_node_addr *node = nodes;
    fsa_node_addr *tmp_node = NULL;
    fsa_kb_info *ki;
    fsa_kb_info *kis;
    int node_num = 0;
    int info_header_printed = 0;
    int print_node_header = 1;

    /* connect to each node separately */
    while (node != NULL) {
        print_node_line(node_num, node->host, print_node_header);
        print_node_header = 0;

        /* only pass a single node to fetch_kb_info, so break linked list  */
        tmp_node = node->next;
        node->next = NULL;

        kis = fsaf_fetch_kb_info(NULL, node);

        /* restore next item in list */
        node->next = tmp_node;

        /* TODO: better error handling, differentiate between err and no
         * stores */
        if (kis != NULL) {
            /* get column widths */
            int max_name = 10;
            int max_segs = 10;
            int curlen;

            for (ki = kis; ki != NULL; ki = ki->next) {
                curlen = strlen((char *)ki->name);
                if (curlen > max_name) {
                    max_name = curlen;
                }

                curlen = ki->p_segments_len * 3;
                if (curlen > max_segs) {
                    max_segs = curlen;
                }
            }

            /* print header */
            if (!info_header_printed) {
                if (colour_flag) {
                    printf(ANSI_COLOUR_BLUE);
                }

                printf("  %-*s status  port  number_of_segments\n",
                       max_name, "store_name");

                if (colour_flag) {
                    printf(ANSI_COLOUR_RESET);
                }

                info_header_printed = 1;
            }

            /* print kb info */
            for (ki = kis; ki != NULL; ki = ki->next) {
                printf("  %-*s ", max_name, ki->name);

                const char *kistat = fsa_kb_info_status_to_string(ki->status);

                if (ki->status == KB_STATUS_RUNNING) {
                    print_colour(kistat, ANSI_COLOUR_GREEN);
                }
                else if (ki->status == KB_STATUS_STOPPED) {
                    print_colour(kistat, ANSI_COLOUR_RED);
                }
                else {
                    print_colour(kistat, ANSI_COLOUR_YELLOW);
                }
                printf(" ");

                if (ki->port > 0) {
                    printf("%-5d ", ki->port);
                }
                else {
                    printf("      ");
                }

                if (ki->p_segments_len > 0) {
                    if (verbosity == V_VERBOSE) {
                        for (int i = 0; i < ki->p_segments_len; i++) {
                            printf("%d", ki->p_segments_data[i]);
                            if (i == ki->p_segments_len-1) {
                                printf(" of %d", ki->num_segments);
                            }
                            else {
                                printf(",");
                            }
                        }
                    }
                    else {
                        /* print summary of segments */
                        printf("%d of %d",
                               ki->p_segments_len, ki->num_segments);
                    }
                }
                printf("\n");
            }
        }

        fsa_kb_info_free(kis);
        node_num += 1;
        node = node->next;
    }

    fsa_node_addr_free(nodes);

    return 0;
}

static int cmd_list_stores(void)
{
    /* verbose listing moved to separate function for clarity */
    if (verbosity >= V_VERBOSE) {
        return cmd_list_stores_verbose();
    }

    fsa_node_addr *nodes = get_nodes_from_cmd_line();
    if (nodes == NULL) {
        /* error messages already printed */
        return 1;
    }

    GHashTable *kb_hash = g_hash_table_new(g_str_hash, g_str_equal);

    fsa_node_addr *node = nodes;
    fsa_node_addr *tmp_node = NULL;
    fsa_kb_info *kis;
    fsa_kb_info *ki;
    fsa_kb_info *next_ki;
    int node_num = 0;
    int name_len;
    int max_name_len = 10; /* track lengths of kb names */
    GList *kb_name_list = NULL; /* track hash keys */

    /* connect to each node separately */
    while (node != NULL) {
        /* only pass a single node to fetch_kb_info, so break linked list  */
        tmp_node = node->next;
        node->next = NULL;

        kis = fsaf_fetch_kb_info(NULL, node);

        /* restore next item in list */
        node->next = tmp_node;

        if (kis != NULL) {
            /* insert each kb info record into hash table */
            ki = kis;
            while (ki != NULL) {
                /* will be breaking the linked list, so track next */
                next_ki = ki->next;

                /* will return NULL if key not found */
                ki->next = g_hash_table_lookup(kb_hash, ki->name);
                if (ki->next == NULL) {
                    /* add each name to list exactly once */
                    kb_name_list = g_list_append(kb_name_list, ki->name);
                }
                g_hash_table_insert(kb_hash, ki->name, ki);

                /* used to align columns when printing */
                name_len = strlen((char *)ki->name);
                if (name_len > max_name_len) {
                    max_name_len = name_len;
                }

                ki = next_ki;
            }
        }

        node_num += 1;
        node = node->next;
    }

    /* sort hash keys, print info for each kb in turn */
    kb_name_list = g_list_sort(kb_name_list, (GCompareFunc)strcmp);

    char *kb_name;
    int n_total, n_running, n_stopped, n_unknown;
    int comma = 0;
    int print_store_header = 1;

    if (kb_name_list == NULL) {
        printf("No stores found\n");
    }

    while (kb_name_list != NULL) {
        if (print_store_header) {
            if (colour_flag) {
                printf(ANSI_COLOUR_BLUE);
            }

            printf("%-*s store_status backend_status\n",
                   max_name_len, "store_name");

            if (colour_flag) {
                printf(ANSI_COLOUR_RESET);
            }
            print_store_header = 0;
        }

        n_running = n_stopped = n_unknown = 0;
        kb_name = kb_name_list->data;

        kis = g_hash_table_lookup(kb_hash, kb_name);
        for (ki = kis; ki != NULL; ki = ki->next) {
            switch (ki->status) {
                case KB_STATUS_RUNNING:
                    n_running += 1;
                    break;
                case KB_STATUS_STOPPED:
                    n_stopped += 1;
                    break;
                case KB_STATUS_UNKNOWN:
                    n_unknown += 1;
                    break;
                default:
                    break;
            }
        }

        n_total = n_running + n_stopped + n_unknown;

        /* output info for this kb */
        printf("%-*s ", max_name_len, kb_name);

        if (n_running == n_total) {
            print_colour("available    ", ANSI_COLOUR_GREEN);
        }
        else {
            print_colour("unavailable  ", ANSI_COLOUR_RED);
        }

        comma = 0;
        if (n_running > 0) {
            printf("%d/%d ", n_running, n_total);
            print_colour("running", ANSI_COLOUR_GREEN);
            comma = 1;
        }

        if (n_stopped > 0) {
            if (comma) {
                printf(", ");
            }
            printf("%d/%d ", n_stopped, n_total);
            print_colour("stopped", ANSI_COLOUR_RED);
            comma = 1;
        }

        if (n_unknown > 0) {
            if (comma) {
                printf(", ");
            }
            printf("%d/%d ", n_unknown, n_total);
            print_colour("unknown", ANSI_COLOUR_YELLOW);
        }

        printf("\n");


        /* done with data in hash entry now */
        fsa_kb_info_free(kis);

        kb_name_list = g_list_next(kb_name_list);
    }

    kb_name_list = g_list_first(kb_name_list);
    g_list_free(kb_name_list);

    fsa_node_addr_free(nodes);
    g_hash_table_destroy(kb_hash);

    return 0;
}

/* Check whether admin daemon on all nodes is reachable */
static int cmd_list_nodes(void)
{
    /* this command has no arguments, exit if any are found */
    if (args_index >= 0) {
        print_invalid_arg();
        return 1;
    }

    /* network related vars */
    struct addrinfo hints;
    int default_port = FS_ADMIND_PORT;
    fsa_node_addr *nodes = NULL;
    fsa_node_addr *p;
    int sock_fd;
    char ipaddr[INET6_ADDRSTRLEN];

    /* printing/output related vars */
    int all_nodes_ok = 0;
    int hostlen = 13;
    int len;
    int node_num = 0;
    int n_nodes = 0;

    /* Setup hints information */
    memset(&hints, 0, sizeof(hints));
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;

    /* attempt to read /etc/4store.conf */
    GKeyFile *config = fsa_get_config();

    if (config == NULL) {
        /* assume localhost if no config file found */
        fsa_error(LOG_WARNING,
                  "Unable to read config file at '%s', assuming localhost\n",
                  fs_get_config_file());
    }
    else {
        nodes = fsa_get_node_list(config);
        if (nodes == NULL) {
            fsa_error(LOG_WARNING,
                      "No nodes found in '%s', assuming localhost\n",
                      fs_get_config_file());
            default_port = fsa_get_admind_port(config);
        }
    }

    if (nodes == NULL) {
        /* Use localhost and default port */
        nodes = fsa_node_addr_new("localhost");
        nodes->port = default_port;
    }


    /* loop through once to get lengths of various fields */
    for (p = nodes; p != NULL; p = p->next) {
        len = strlen(p->host) + 1;
        if (len > hostlen) {
            hostlen = len;
        }

        n_nodes += 1;
    }

    int n_nodes_len = int_len(n_nodes);
    if (n_nodes_len < 11) {
        n_nodes_len = 11;
    }

    /* print column headers */
    if (colour_flag) {
        printf(ANSI_COLOUR_BLUE);
    }

    printf("%-*s %-*s port  status      ip_address\n",
           n_nodes_len, "node_number", hostlen, "hostname");

    if (colour_flag) {
        printf(ANSI_COLOUR_RESET);
    }

    /* loop through all nodes and attempt to connect admin daemon on each */
    for (p = nodes; p != NULL; p = p->next) {
        /* set default output for IP address */
        strcpy(ipaddr, "unknown");

        /* check if we can open conn to admin daemon */
        sock_fd = fsaf_connect_to_admind(p->host, p->port, &hints, ipaddr);

        /* print result of attempted connection */
        printf("%-*d %-*s %-5d ",
               n_nodes_len, node_num, hostlen, p->host, p->port);
        if (sock_fd == -1) {
            print_colour("unreachable", ANSI_COLOUR_RED);
            all_nodes_ok = 2;
        }
        else {
            print_colour("ok         ", ANSI_COLOUR_GREEN);
            close(sock_fd);
        }
        printf(" %s\n", ipaddr);

        node_num += 1;
    }

    fsa_node_addr_free(nodes);
    fsa_config_free(config);

    return all_nodes_ok;
}

static int handle_command(void)
{
    if (cmd_index < 0) {
        print_usage(0);
        return 1;
    }

    fsa_error(LOG_DEBUG, "command '%s' called", argv[cmd_index]);

    if (strcmp(argv[cmd_index], "list-nodes") == 0) {
        return cmd_list_nodes();
    }
    else if (strcmp(argv[cmd_index], "list-stores") == 0) {
        return cmd_list_stores();
    }
    else if (strcmp(argv[cmd_index], "stop-stores") == 0) {
        return cmd_stop_stores();
    }
    else if (strcmp(argv[cmd_index], "start-stores") == 0) {
        return cmd_start_stores();
    }
    else if (strcmp(argv[cmd_index], "delete-stores") == 0) {
        return cmd_delete_stores();
    }
    else if (strcmp(argv[cmd_index], "create-store") == 0) {
        return cmd_create_store();
    }
    else {
        fsa_error(LOG_ERR, "unrecognized command '%s'", argv[cmd_index]);
        print_usage(1);
        return 1;
    }
}

int main(int local_argc, char **local_argv)
{
    int rv;

    /* argc and argv used pretty much everywhere, so make global */
    argc = local_argc;
    argv = local_argv;

    /* will be set already if __USE_GNU is defined, but set anyway */
    program_invocation_name = argv[0];
    program_invocation_short_name = basename(argv[0]);

    /* Set logging to stderr and log level globals */
    fsa_log_to = ADM_LOG_TO_STDERR;
    fsa_log_level = ADM_LOG_LEVEL;

    /* Disable colour output if no tty */
    if (!isatty(STDOUT_FILENO)) {
        colour_flag = 0;
    }

    /* Parse command line options and arguments into global vars  */
    rv = parse_cmdline_opts(argc);
    if (rv != 0) {
        return rv;
    }

    /* Handle simple flags (version, help) */
    if (help_flag) {
        fsa_error(LOG_DEBUG, "help command called");
        print_help();
        return 0;
    }

    if (version_flag) {
        fsa_error(LOG_DEBUG, "version command called");
        print_version();
        return 0;
    }

    /* Perform command pointed to by cmd_index */
    rv = handle_command();
    return rv;
}
