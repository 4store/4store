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

/***** Global application state *****/
/* Command line params */
static int help_flag = 0;
static int version_flag = 0;
static int verbosity = 0;

/* indexes into argv for start of command, and start of command arguments */
static int cmd_index = -1;
static int args_index = -1;

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
                  FS_CONFIG_FILE); 
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
                  FS_CONFIG_FILE); 
        nodes = fsa_node_addr_new("localhost");
        nodes->port = default_port;
        return nodes;
    }

    return nodes;
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
"    --help    Display this message and exit\n"
"    --version Display version information and exit\n"
"    --verbose Increase amount of information returned\n"
"    --debug   Output full debugging information\n"
"\n"
"Common commands (use `%s help <command>' for more info)\n"
"  list-nodes   List hostname:port of all known storage nodes\n"
"  check-nodes  Check whether storage nodes are reachable\n"
"  list-stores  List stores, along with the nodes they're hosted on\n"
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
"List names of all storage nodes known\n",
                program_invocation_short_name, argv[i]
            );
        }
        else if (strcmp(argv[i], "check-nodes") == 0) {
            printf(
"Usage: %s %s\n"
"Check that the admin daemon on each storage node is reachable over the \n"
"network. Returns 0 if all nodes are reachable, non-zero if not.\n",
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
         {"help",       no_argument,    &help_flag,     1},
         {"quiet",      no_argument,    &quiet_flag,    1},
         {"verbose",    no_argument,    &verbose_flag,  1},
         {"debug",      no_argument,    &debug_flag,    1},
         {"version",    no_argument,    &version_flag,  1},
         {NULL,         0,              NULL,           0}
    };

    while(1) {
        c = getopt_long(argc, argv, "", long_opts, &opt_index);

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

static int cmd_list_kbs(void)
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
            return 1;
        }

        /* check whether we have a hostname or a node number */
        if (fsa_is_int(host_or_nodenum)) {
            /* get host by number, starting from 0 */
            int node_num = atoi(host_or_nodenum);
            nodes = node_num_to_node_addr(node_num);
            if (nodes == NULL) {
                fsa_error(LOG_ERR, "Node number '%d' not found in config\n",
                          node_num);
                return 1;
            }
        }
        else {
            /* get host by name */
            nodes = node_name_to_node_addr(host_or_nodenum);
            if (nodes == NULL) {
                fsa_error(LOG_ERR,
                          "Node with name '%s' not found in config\n",
                          host_or_nodenum);
                return 1;
            }
        }
    }

    fsa_node_addr *node = nodes;
    fsa_node_addr *tmp_node = NULL;
    fsa_kb_info *ki;
    fsa_kb_info *kis;
    int node_num = 0;

    /* connect to each node separately */
    while (node != NULL) {
        printf("%d\t%s:%d\n", node_num, node->host, node->port);

        /* only pass a single node to fetch_kb_info, so break linked list  */
        tmp_node = node->next;
        node->next = NULL;

        kis = fsaf_fetch_kb_info(NULL, node);

        /* restore next item in list */
        node->next = tmp_node;

        if (kis == NULL) {
            /* check errno to see why we got no data */
            if (errno == 0) {
                /* no error, but no kbs on node */
                printf("no kbs on node\n");
            }
            else if (errno == ADM_ERR_CONN_FAILED) {
                /* do nothing, error already handled */
            }
            else {
                fsa_error(LOG_ERR,
                          "Connection to node %d (%s:%d) failed: %s\n",
                          node_num, node->host, node->port, strerror(errno));
            }
        }
        else {
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

            /* print kb info */
            for (ki = kis; ki != NULL; ki = ki->next) {
                printf("  %-*s\t", max_name, ki->name);

                if (ki->status == KB_STATUS_RUNNING) {
                    printf(ANSI_COLOUR_GREEN);
                }
                else if (ki->status == KB_STATUS_STOPPED) {
                    printf(ANSI_COLOUR_RED);
                }
                else {
                    printf(ANSI_COLOUR_YELLOW);
                }
                printf("%s", fsa_kb_info_status_to_string(ki->status));
                printf(ANSI_COLOUR_RESET);
                printf("\t");
                printf("%d\t", ki->port);

                if (ki->p_segments_len > 0) {
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

/* Check whether admin daemon on all nodes is reachable */
static int cmd_check_nodes(void)
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
    int hostlen = 0;
    int len;
    int node_num = 0;
    int n_nodes = 0;
    char *buf = NULL;

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
                  FS_CONFIG_FILE); 
    }
    else {
        nodes = fsa_get_node_list(config);
        if (nodes == NULL) {
            fsa_error(LOG_WARNING,
                      "No nodes found in '%s', assuming localhost\n",
                      FS_CONFIG_FILE); 
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
        len = strlen(p->host) + 1 + int_len(p->port);
        if (len > hostlen) {
            hostlen = len;
        }

        n_nodes += 1;
    }
    buf = (char *)malloc(hostlen + 1);

    /* loop through all nodes and attempt to connect admin daemon on each */
    for (p = nodes; p != NULL; p = p->next) {
        /* set default output for IP address */
        strcpy(ipaddr, "unknown");

        /* check if we can open conn to admin daemon */
        sock_fd = fsaf_connect_to_admind(p->host, p->port, &hints, ipaddr);

        /* print result of attempted connection */
        sprintf(buf, "%s:%d", p->host, p->port);
        printf("%-*d %-*s ", int_len(n_nodes), node_num, hostlen, buf);
        if (sock_fd == -1) {
            printf(ANSI_COLOUR_RED "unreachable");
            all_nodes_ok = 2;
        }
        else {
            printf(ANSI_COLOUR_GREEN "ok         ");
            close(sock_fd);
        }
        printf(ANSI_COLOUR_RESET);
        printf(" %s\n", ipaddr);

        node_num += 1;
    }

    free(buf);
    fsa_node_addr_free(nodes);
    fsa_config_free(config);

    return all_nodes_ok;
}

/* List all storage nodes known, probably from /etc/4store.conf */
static int cmd_list_nodes(void)
{
    /* this command has no arguments, exit if any are found */
    if (args_index >= 0) {
        print_invalid_arg();
        return 1;
    }

    fsa_error(LOG_DEBUG, "Attempting to read config file at %s",
              FS_CONFIG_FILE);

    /* attempt to read /etc/4store.conf */
    GKeyFile *config = fsa_get_config();

    /* assume localhost if no config file found */
    if (config == NULL) {
        fsa_error(LOG_WARNING,
                  "Unable to read config file at '%s', assuming localhost\n",
                  FS_CONFIG_FILE); 
        printf("0 localhost:%d\n", FS_ADMIND_PORT);
        return 0;
    }

    fsa_error(LOG_DEBUG, "Looking for node config in %s", FS_CONFIG_FILE);
    fsa_node_addr *nodes = fsa_get_node_list(config);

    /* if no 'nodes = host1:port;host2:port' entry, assume localhost */
    if (nodes == NULL) {
        int default_port = fsa_get_admind_port(config);
        fsa_config_free(config);

        fsa_error(LOG_WARNING,
                  "No nodes found in '%s', assuming localhost\n",
                  FS_CONFIG_FILE); 
        printf("0 localhost:%d\n", default_port);
        return 0;
    }

    /* print list of storage nodes */
    fsa_node_addr *p;
    int node_num = 0;
    int n_nodes = 0;

    /* loop through once to get count */
    for (p = nodes; p != NULL; p = p->next) {
        n_nodes += 1;
    }

    fsa_error(LOG_DEBUG, "%d nodes to print", n_nodes);

    /* loop through again to print each entry */
    for (p = nodes; p != NULL; p = p->next) {
        printf("%-*d %s:%d\n", int_len(n_nodes), node_num, p->host, p->port);
        node_num += 1;
    }

    fsa_node_addr_free(nodes);
    fsa_config_free(config);

    return 0;
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
    else if (strcmp(argv[cmd_index], "check-nodes") == 0) {
        return cmd_check_nodes();
    }
    else if (strcmp(argv[cmd_index], "list-stores") == 0) {
        return cmd_list_kbs();
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
