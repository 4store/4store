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
#include <getopt.h>
#include <dirent.h>
#include <libgen.h>
#include <errno.h>
#include <signal.h>
#include <sys/stat.h>

#include <sys/select.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <netdb.h>

#include "../common/4store.h"
#include "../common/error.h"
#include "../common/params.h"
#include "../backend/metadata.h"

#include "admin_common.h"
#include "admin_protocol.h"
#include "admin_backend.h"

#define STOP_STORES  0
#define START_STORES 1

/***** Server State *****/
/* Fields initialised by parse_cmdline_opts() */
static int verbose_flag = 0;
static int help_flag = 0;
static int version_flag = 0;
static int daemonize_flag = 1;
static int debug_flag = 0;
static const char *cport = NULL;

/* Port to actually run server on */
static char server_port[6];

/* Socket/network globals */
static fd_set master_read_fds;
static fd_set read_fds;
static int fd_max;
static int listener_fds[2]; /* 0 -> ipv4, 1 -> ipv6 */
/***** End of Server State *****/

static void print_usage(int listhelp) {
    printf("Usage: %s [OPTION]...\n", program_invocation_short_name);

    if (listhelp) {
        printf("Try `%s --help' for more information.\n",
               program_invocation_short_name);
    }
}

static void print_help(void)
{
    print_usage(0);

    printf(
"  -p, --port=PORT       specify port to listen on (default: %d)\n"
"  -D, --no-daemonize    do not daemonise\n"
"  -c, --config-file     path and filename of configuration file to use\n"
"  -b, --bin-dir         path to directory containing 4store executables\n"
"      --help            display this message and exit\n"
"      --verbose         display more detailed output\n"
"      --debug           display full debugging output\n"
"      --version         display version information and exit\n",
        FS_ADMIND_PORT
    );
}

static void print_version(void)
{
    printf("%s, built for 4store %s\n", program_invocation_short_name, GIT_REV);
}

static int parse_cmdline_opts(int argc, char **argv)
{
    int c;
    int opt_index;

    struct option long_opts[] = {
        {"version",         no_argument,        &version_flag,      1},
        {"verbose",         no_argument,        &verbose_flag,      1},
        {"debug",           no_argument,        &debug_flag,        1},
        {"help",            no_argument,        &help_flag,         1},
        {"no-daemonize",    no_argument,        &daemonize_flag,    0},
        {"port",            required_argument,  NULL,               'p'},
        {"config-file",     required_argument,  NULL,               'c'},
        {"bin-dir",         required_argument,  NULL,               'b'},
        {NULL,              0,                  NULL,               0}
    };

    while(1) {
        opt_index = 0;
        c = getopt_long(argc, argv, "Dp:c:b:", long_opts, &opt_index);

        /* end of options */
        if (c == -1) {
            break;
        }

        switch(c) {
            case 0:
                /* option set flag, do nothing */
                break;
            case 'D':
                daemonize_flag = 0;
                break;
            case 'p':
                cport = optarg;
                break;
            case 'c':
                fs_set_config_file(optarg);
                break;
            case 'b':
                fsa_set_bin_dir(optarg);
                break;
            case '?':
                /* getopt_long has already printed err msg */
                print_usage(1);
                return -1;
            default:
                abort();
        }
    }

    /* get remaining command line args */
    /*
    if (optind < argc) {
        while (optind < argc) {
            printf("arg: %s\n", argv[optind++]);
        }
    }
    */
    return 0;
}

/* Fail early if this is not readable */
static int check_bin_dir(void)
{
    DIR *dir;
    const char *bin_dir = fsa_get_bin_dir();

    dir = opendir(bin_dir);
    if (dir == NULL) {
        fsa_error(LOG_ERR,
                  "failed to read bin dir '%s': %s",
                  bin_dir, strerror(errno));
        return -1;
    }

    closedir(dir);

    return 0;
}

/* check command line opts and config file to set server_port global */
static int init_server_port(void)
{
    int port;

    /* no port given on command line */
    if (cport == NULL) {
        /* get default or config file port */
        GKeyFile *config = fsa_get_config();
        port = fsa_get_admind_port(config);
        fsa_config_free(config);

        if (port == -1) {
            if (errno == ADM_ERR_BAD_CONFIG) {
                fprintf(
                    stderr,
                    "%s: non-numeric port specified in %s",
                    program_invocation_short_name, fs_get_config_file()
                );
            }
            else if (errno == ERANGE) {
                fprintf(
                    stderr,
                    "%s: port number out of range 0-65535 in %s",
                    program_invocation_short_name, fs_get_config_file()
                );
            }
            else {
                fprintf(
                    stderr,
                    "%s: unknown error reading port from config file at %s\n",
                    program_invocation_short_name, fs_get_config_file()
                );
            }
            return -1;
        }
    }
    else {
        /* cport has been specified on command line */

        if (!fsa_is_int(cport)) {
            fprintf(stderr, "%s: non-numeric port specified on command line\n",
                    program_invocation_short_name);
            return -1;
        }

        port = atoi(cport);
        if (port < 0 || port > 65535) {
            fprintf(stderr, "%s: port number %d out of range 0-65535\n",
                    program_invocation_short_name, port);
            return -1;
        }
    }

    /* have a port 0-65535 if we got here */
    sprintf(server_port, "%d", port);
    return 0;
}

/* fork once, and exit parent */
static void fork_from_parent(void)
{
    pid_t pid;

    pid = fork();
    if (pid == -1) {
        /* fork failed */
        fsa_error(LOG_ERR, "fork() error starting daemon: %s",
                  strerror(errno));
        exit(EXIT_FAILURE);
    }
    else if (pid > 0) {
        /* fork success, and this is parent, so exit parent */
        _exit(EXIT_SUCCESS);
    }
}

static void daemonize(void)
{
    pid_t sid;
    int fd, rv;

    /* fork once from parent process */
    fork_from_parent();

    /* create new process group */
    sid = setsid();
    if (sid < 0) {
        fsa_error(LOG_ERR, "setsid() error starting daemon: %s",
                  strerror(errno));
        exit(EXIT_FAILURE);
    }

    /* fork again to separate from parent permanently */
    fork_from_parent();

    /* change working directory somewhere known */
    if (chdir("/") == -1) {
        fsa_error(LOG_ERR, "chdir failed: %s", strerror(errno));
    }

    /* close all file descriptors */
    for (fd = getdtablesize(); fd >= 0; fd--) {
        close(fd);
    }

    /* set file mode mask */
    umask(0);

    /* reopen standard fds */
    fd = open("/dev/null", O_RDWR); /* stdin */
    rv = dup(fd); /* stdout */
    rv = dup(fd); /* stderr */

    /* log to syslog instead of stderr */
    fsp_syslog_enable();
    fsa_log_to = ADM_LOG_TO_FS_ERROR;
    setlogmask(LOG_UPTO(fsa_log_level));
}

static void signal_handler(int sig)
{
    if (daemonize_flag) {
        /* make info message a bit clearer in syslog */
        fsa_error(LOG_INFO, "%s received %s (%d) signal",
                  program_invocation_short_name, strsignal(sig), sig);
    }
    else {
        fsa_error(LOG_INFO, "received %s (%d) signal", strsignal(sig), sig);
    }

    switch (sig) {
        case SIGINT:
        case SIGTERM:
            for (int i = 0; i < sizeof(listener_fds); i++) {
                if (listener_fds[i] > -1) {
                    FD_CLR(listener_fds[i], &master_read_fds);
                    close(listener_fds[i]);
                }
            }

            if (daemonize_flag) {
                fsa_error(LOG_INFO, "%s shutdown cleanly",
                          program_invocation_short_name);
            }
            else {
                fsa_error(LOG_INFO, "shutdown cleanly");
            }

            fsp_syslog_disable();
            exit(EXIT_SUCCESS);
            break;
        default:
            fsa_error(LOG_DEBUG, "signal %s (%d) unhandled",
                      strsignal(sig), sig);
            break;
    }
}

/* receive data from client, clear from master fd set on error/hangup */
static int recv_from_client(int client_fd, unsigned char *buf, int len)
{
    int nbytes = recv(client_fd, buf, len, MSG_WAITALL);
    if (nbytes <= 0) {
        if (nbytes == 0) {
            fsa_error(LOG_DEBUG, "client on fd %d hung up", client_fd);
        }
        else {
            fsa_error(LOG_ERR, "error receiving from client: %s",
                      strerror(errno));
        }

        /* cleanup client fd */
        close(client_fd);
        FD_CLR(client_fd, &master_read_fds);
    }

    fsa_error(LOG_DEBUG, "received %d bytes from client", nbytes);

    return nbytes;
}

static unsigned char *get_string_from_client(int client_fd, int len)
{
    /* len = num chars in string to fetch */
    unsigned char *str = (unsigned char *)malloc(len + 1);

    /* get kb name from client */
    int nbytes = recv_from_client(client_fd, str, len);
    if (nbytes <= 0) {
        /* errors already logged/handled */
        free(str);
        return NULL;
    }
    str[len] = '\0';

    return str;
}

static int setup_server(void)
{
    struct addrinfo hints;
    struct addrinfo *ai;
    struct addrinfo *p;
    int rv;
    int yes = 1;

    listener_fds[0] = -1;
    listener_fds[1] = -1;

    /* zero global and local data structures */
    FD_ZERO(&master_read_fds);
    FD_ZERO(&read_fds);
    memset(&hints, 0, sizeof(hints));

    hints.ai_family = AF_UNSPEC;     /* IPv4 or IPv6 */
    hints.ai_socktype = SOCK_STREAM; /* TCP */
    hints.ai_flags = AI_PASSIVE;     /* Get local IP if needed */

    /* get host details */
    rv = getaddrinfo(NULL, server_port, &hints, &ai);
    if (rv == -1) {
        fsa_error(LOG_ERR, "getaddrinfo failed: %s", gai_strerror(rv));
        return -1;
    }

    /* loop until we can bind to a socket */
    int i;
    for (p = ai; p != NULL; p = p->ai_next) {
        if (p->ai_family == AF_INET) {
            i = 0; /* v4 index */
        }
        else {
            i = 1; /* v6 index */
        }

        if (listener_fds[i] > -1) {
            /* listener for this protocol already set */
            continue;
        }

        listener_fds[i] = socket(p->ai_family, p->ai_socktype, p->ai_protocol);
        if (listener_fds[i] == -1) {
            fsa_error(LOG_DEBUG, "socket failed: %s", strerror(errno));
            continue;
        }

        /* Get rid of address in use error, ignore errors */
        rv = setsockopt(listener_fds[i], SOL_SOCKET, SO_REUSEADDR,
                        &yes, sizeof(int));
        if (rv == -1) {
            fsa_error(LOG_WARNING, "setsockopt SO_REUSEADDR failed: %s",
                      strerror(errno));
        }

        if (p->ai_family == AF_INET6) {
            rv = setsockopt(listener_fds[i], IPPROTO_IPV6, IPV6_V6ONLY,
                            &yes, sizeof(int));
            if (rv == -1) {
                fsa_error(LOG_WARNING, "setsockopt IPPROTO_IPV6 failed: %s",
                          strerror(errno));
            }
        }

        /* attempt to bind to socket */
        rv = bind(listener_fds[i], p->ai_addr, p->ai_addrlen);
        if (rv == -1) {
            fsa_error(LOG_DEBUG, "server socket bind failed: %s",
                      strerror(errno));
            close(listener_fds[i]);
            listener_fds[i] = -1;
            continue;
        }
        fsa_error(LOG_DEBUG, "socket bind successful");

        /* start listening on bound socket */
        rv = listen(listener_fds[i], 5);
        if (rv == -1) {
            fsa_error(LOG_ERR,
                      "failed to listen on socket: %s", strerror(errno));
            close(listener_fds[i]);
            listener_fds[i] = -1;
            continue;
        }
        fsa_error(LOG_DEBUG, "socket listen successful");

        /* add listener to master set */
        fcntl(listener_fds[i], F_SETFD, FD_CLOEXEC);
        FD_SET(listener_fds[i], &master_read_fds);

        /* track largest file descriptor */
        if (listener_fds[i] > fd_max) {
            fd_max = listener_fds[i];
        }
    }

    /* done with addrinfo now */
    freeaddrinfo(ai);

    /* we failed to listen on anything */
    if (listener_fds[0] < 0 && listener_fds[1] < 0) {
        fsa_error(LOG_ERR, "failed to listen to any socket");
        return -1;
    }

    /* set up signal handlers */
    signal(SIGTERM, signal_handler);
    signal(SIGINT, signal_handler);

    return 0;
}

/* convenience function - logs error, and sends it to client */
static void send_error_message(int sock_fd, const char *msg)
{
    fsa_error(LOG_ERR, "%s", msg);

    int len, rv;
    unsigned char *response =
        fsap_encode_rsp_error((unsigned char *)msg, &len);

    if (response == NULL) {
        fsa_error(LOG_CRIT, "failed to encode error message");
        return;
    }

    rv = fsa_sendall(sock_fd, response, &len);
    free(response); /* done with response buffer */

    if (rv == -1) {
        fsa_error(LOG_ERR, "failed to send response to client: %s",
                  strerror(errno));
    }
}

/* tell client to expect n more messages */
static int send_expect_n(int sock_fd, int n)
{
    fsa_error(LOG_DEBUG, "telling client to expect %d more messages", n);

    int len, rv;
    unsigned char *msg = fsap_encode_rsp_expect_n(n, &len);
    if (msg == NULL) {
        fsa_error(LOG_CRIT, "failed to encode expect n message");
        return -1;
    }

    rv = fsa_sendall(sock_fd, msg, &len);
    free(msg); /* done with msg buffer */

    if (rv == -1) {
        fsa_error(LOG_CRIT, "failed to send response to client: %s",
                  strerror(errno));
    }
    else {
        fsa_error(LOG_DEBUG, "expect_n sent %d bytes", len);
    }

    return rv;
}

/* tell client to expect n more kb messages */
static int send_expect_n_kb(int sock_fd, int n, int max_kb_len)
{
    fsa_error(LOG_DEBUG, "telling client to expect %d more kb messages", n);

    int len, rv;
    unsigned char *msg = fsap_encode_rsp_expect_n_kb(n, max_kb_len, &len);
    if (msg == NULL) {
        fsa_error(LOG_CRIT, "failed to encode expect n kb message");
        return -1;
    }

    rv = fsa_sendall(sock_fd, msg, &len);
    free(msg); /* done with msg buffer */

    if (rv == -1) {
        fsa_error(LOG_CRIT, "failed to send response to client: %s",
                  strerror(errno));
    }
    else {
        fsa_error(LOG_DEBUG, "expect_n_kb sent %d bytes", len);
    }

    return rv;
}

/* accept new client connection, and add to master read fd set */
static void handle_new_connection(int l_index)
{
    struct sockaddr_storage remote_addr;
    socklen_t addr_len = sizeof(remote_addr);
    int new_fd;

    new_fd = accept(listener_fds[l_index],
                    (struct sockaddr *)&remote_addr,
                    &addr_len);
    if (new_fd == -1) {
        fsa_error(LOG_ERR, "accept failed: %s", strerror(errno));
        return;
    }

    /* add client to master fd set */
    FD_SET(new_fd, &master_read_fds);
    if (new_fd > fd_max) {
        fd_max = new_fd;
    }

    fsa_error(LOG_DEBUG, "new connection on fd %d", new_fd);
}

static void handle_cmd_get_kb_info_all(int client_fd)
{
    int rv, len;
    fsa_kb_info *ki;
    unsigned char *response;

    /* get info on all running/stopped kbs on this host */
    int err;
    ki = fsab_get_local_kb_info_all(&err);
    if (ki == NULL) {
        send_error_message(client_fd, "failed to get local kb info");
        return;
    }

    /* encode response to send back to client */
    response = fsap_encode_rsp_get_kb_info_all(ki, &len);
    fsa_kb_info_free(ki); /* done with kb info now */
    if (response == NULL) {
        send_error_message(client_fd, "failed to encode response");
        return;
    }

    fsa_error(LOG_DEBUG, "response size is %d bytes", len);

    /* send entire response back to client */
    rv = fsa_sendall(client_fd, response, &len);
    free(response); /* done with response buffer */
    if (rv == -1) {
        fsa_error(LOG_ERR, "failed to send response to client: %s",
                  strerror(errno));
        return;
    }

    fsa_error(LOG_DEBUG, "%d bytes sent to client", len);
}

/* get all information about a specific kb on this host, send to client */
static void handle_cmd_get_kb_info(int client_fd, uint16_t datasize)
{
    int rv, len, err;
    fsa_kb_info *ki;
    unsigned char *response;
    unsigned char *kb_name;

    kb_name = get_string_from_client(client_fd, datasize);
    if (kb_name == NULL) {
        /* errors already logged/handled */
        return;
    }

    /* should already have been checked by client */
    if (!fsa_is_valid_kb_name((const char *)kb_name)) {
        fsa_error(LOG_CRIT, "Invalid kb name received from client");
        send_error_message(client_fd, "kb name invalid");
        free(kb_name);
        return;
    }

    ki = fsab_get_local_kb_info(kb_name, &err);
    free(kb_name); /* done with kb_name */
    if (ki == NULL || err == ADM_ERR_KB_NOT_EXISTS) {
        send_error_message(client_fd, "failed to get local kb info");
        return;
    }

    /* encode message for client */
    response = fsap_encode_rsp_get_kb_info(ki, &len);
    fsa_kb_info_free(ki);
    if (response == NULL) {
        send_error_message(client_fd, "failed to encode kb info");
        return;
    }

    fsa_error(LOG_DEBUG, "response size is %d bytes", len);

    /* send entire response back to client */
    rv = fsa_sendall(client_fd, response, &len);
    free(response); /* done with response buffer */
    if (rv == -1) {
        fsa_error(LOG_ERR, "failed to send response to client: %s",
                  strerror(errno));
        return;
    }

    fsa_error(LOG_DEBUG, "%d bytes sent to client", len);
}

/* create a single kb */
static void handle_cmd_create_kb(int client_fd, uint16_t datasize)
{
    int rv, err;
    unsigned char *msg = NULL;
    int exit_val; /* exit value from 4s-backend-setup */

    /* unpack client data into buffer */
    unsigned char *buf = (unsigned char *)malloc(datasize);
    rv = recv_from_client(client_fd, buf, datasize);
    if (rv <= 0) {
        /* errors already logged/handled */
        free(buf);
        return;
    }

    /* parse buffer into kb setup args struct */
    fsa_kb_setup_args *ksargs = fsap_decode_cmd_create_kb(buf);
    fsa_error(LOG_DEBUG, "ksargs->name: %s", ksargs->name);
    fsa_error(LOG_DEBUG, "ksargs->node_id: %d", ksargs->node_id);
    fsa_error(LOG_DEBUG, "ksargs->cluster_size: %d", ksargs->cluster_size);
    fsa_error(LOG_DEBUG, "ksargs->num_segments: %d", ksargs->num_segments);
    fsa_error(LOG_DEBUG, "ksargs->mirror_segments: %d",ksargs->mirror_segments);
    fsa_error(LOG_DEBUG, "ksargs->model_files: %d", ksargs->model_files);
    fsa_error(LOG_DEBUG, "ksargs->delete_existing: %d",ksargs->delete_existing);

    /* should already have been checked by client */
    if (!fsa_is_valid_kb_name((const char *)ksargs->name)) {
        fsa_error(LOG_CRIT, "Invalid store name received from client");
        send_error_message(client_fd, "store name invalid");
        fsa_kb_setup_args_free(ksargs);
    }

    rv = fsab_create_local_kb(ksargs, &exit_val, &msg, &err);

    /* encode message for client */
    int len;
    unsigned char *response =
        fsap_encode_rsp_create_kb(err, ksargs->name, msg, &len);

    fsa_error(LOG_DEBUG, "sending err: %d, kb_name: %s, msg: %s",
              err, ksargs->name, msg);

    fsa_kb_setup_args_free(ksargs);
    free(msg);

    fsa_error(LOG_DEBUG, "response size is %d bytes", len);

    /* send entire response back to client */
    rv = fsa_sendall(client_fd, response, &len);
    free(response);
    if (rv == -1) {
        fsa_error(LOG_ERR, "failed to send response to client: %s",
                  strerror(errno));
        return;
    }

    fsa_error(LOG_DEBUG, "%d bytes sent to client", len);
}

/* delete a single kb */
static void handle_cmd_delete_kb(int client_fd, uint16_t datasize)
{
    int rv, err;
    unsigned char *msg = NULL;
    int exit_val; /* exit value from 4s-backend-delete */

    /* datasize = num chars in kb name */
    unsigned char *kb_name = get_string_from_client(client_fd, datasize);
    if (kb_name == NULL) {
        /* errors already logged/handled */
        return;
    }

    /* should already have been checked by client */
    if (!fsa_is_valid_kb_name((const char *)kb_name)) {
        fsa_error(LOG_CRIT, "Invalid kb name received from client");
        send_error_message(client_fd, "kb name invalid");
        free(kb_name);
        return;
    }

    rv = fsab_delete_local_kb(kb_name, &exit_val, &msg, &err);

    /* encode message for client */
    int len;
    unsigned char *response =
        fsap_encode_rsp_delete_kb(err, kb_name, msg, &len);

    fsa_error(LOG_DEBUG, "sending err: %d, kb_name: %s, msg: %s",
              err, kb_name, msg);

    free(msg);
    free(kb_name);

    fsa_error(LOG_DEBUG, "response size is %d bytes", len);

    /* send entire response back to client */
    rv = fsa_sendall(client_fd, response, &len);
    free(response); /* done with response buffer */
    if (rv == -1) {
        fsa_error(LOG_ERR, "failed to send response to client: %s",
                  strerror(errno));
        return;
    }

    fsa_error(LOG_DEBUG, "%d bytes sent to client", len);
}

static void start_or_stop_kb_all(int client_fd, int action, int force)
{
    int rv, err;
    int n_kbs = 0;
    int max_kb_len = 0;

    /* get all local kbs */
    fsa_kb_info *ki = fsab_get_local_kb_info_all(&err);
    if (ki == NULL) {
        if (err == 0) {
            /* no kbs on this host */
            send_expect_n_kb(client_fd, 0, 0);
        }
        else {
            send_error_message(client_fd, "failed to read store information");
        }
        return;
    }

    /* count number of kbs */
    for (fsa_kb_info *p = ki; p != NULL; p = p->next) {
        int kb_len = strlen((char *)p->name);
        if (kb_len > max_kb_len) {
            max_kb_len = kb_len;
        }
        n_kbs += 1;
    }

    /* tell client to expect a message for each */
    rv = send_expect_n_kb(client_fd, n_kbs, max_kb_len);
    if (rv == -1) {
        /* failed to send message to client, so give up */
        fsa_kb_info_free(ki);
        return;
    }

    int exit_val; /* ignored for now */
    unsigned char *msg = NULL; /* sent back to client */
    int return_val; /* sent back to client */

    int len; /* response length */
    unsigned char *response = NULL; /* response buffer */

    for (fsa_kb_info *p = ki; p != NULL; p = p->next) {
        if (action == STOP_STORES) {
            rv = fsab_stop_local_kb(p->name, force, &err);
            if (rv == 0)
                ki->status = KB_STATUS_STOPPED; 
        }
        else if (action == START_STORES) {
            rv = fsab_start_local_kb(p->name, &exit_val, &msg, &err);
        }

        if (rv < 0) {
            return_val = err;
        }
        else {
            return_val = ADM_ERR_OK;
        }

        /* encode message for client */
        if (action == STOP_STORES) {
            response =
                fsap_encode_rsp_stop_kb(return_val, p->name, msg, &len);
        }
        else if (action == START_STORES) {
            response =
                fsap_encode_rsp_start_kb(return_val, p->name, msg, &len);
        }

        if (msg != NULL) {
            free(msg);
            msg = NULL;
        }

        fsa_error(LOG_DEBUG, "response size is %d bytes", len);

        /* send entire response back to client */
        rv = fsa_sendall(client_fd, response, &len);
        free(response); /* done with response buffer */
        response = NULL;
        if (rv == -1) {
            fsa_error(LOG_ERR, "failed to send response to client: %s",
                      strerror(errno));
            continue;
        }

        fsa_error(LOG_DEBUG, "%d bytes sent to client", len);
    }

    fsa_kb_info_free(ki);
}

static void handle_cmd_stop_kb_all(int client_fd,int force)
{
    start_or_stop_kb_all(client_fd, STOP_STORES,force);
}

static void handle_cmd_start_kb_all(int client_fd, int force)
{
    start_or_stop_kb_all(client_fd, START_STORES, force);
}

/* start or stop a running kb */
static void start_or_stop_kb(int client_fd, uint16_t datasize, int action, int force)
{
    int rv, err;
    unsigned char *msg = NULL;
    int return_val; /* value to send back to client */

    /* datasize = num chars in kb name */
    unsigned char *kb_name = get_string_from_client(client_fd, datasize);
    if (kb_name == NULL) {
        /* errors already logged/handled */
        return;
    }

    if (action == STOP_STORES) {
        rv = fsab_stop_local_kb(kb_name, force, &err);
    }
    else {
        /* action == START_STORES */
        int exit_val; /* ignore command exit val for now */
        rv = fsab_start_local_kb(kb_name, &exit_val, &msg, &err);
    }

    if (rv < 0) {
        return_val = err;
    }
    else {
        return_val = ADM_ERR_OK;
    }

    /* encode message for client */
    int len;
    unsigned char *response;

    if (action == STOP_STORES) {
        response = fsap_encode_rsp_stop_kb(return_val, kb_name, msg, &len);
    }
    else {
        /* action == START_STORES */
        response = fsap_encode_rsp_start_kb(return_val, kb_name, msg, &len);
    }
    free(msg);
    free(kb_name);

    fsa_error(LOG_DEBUG, "response size is %d bytes", len);

    /* send entire response back to client */
    rv = fsa_sendall(client_fd, response, &len);
    free(response); /* done with response buffer */
    if (rv == -1) {
        fsa_error(LOG_ERR, "failed to send response to client: %s",
                  strerror(errno));
        return;
    }

    fsa_error(LOG_DEBUG, "%d bytes sent to client", len);
}

static void handle_cmd_stop_kb(int client_fd, uint16_t datasize, int force)
{
    start_or_stop_kb(client_fd, datasize, STOP_STORES, force);
}

static void handle_cmd_start_kb(int client_fd, uint16_t datasize)
{
    start_or_stop_kb(client_fd, datasize, START_STORES, 0);
}

/* receive header from client, work out what request they want */
static void handle_client_data(int client_fd)
{
    uint8_t cmdval; /* command client wants server to run */
    uint16_t datasize; /* size of command packet */
    int rv, nbytes;
    unsigned char header_buf[ADM_HEADER_LEN];

    /* block until we receive a full header */
    nbytes = recv_from_client(client_fd, header_buf, ADM_HEADER_LEN);
    if (nbytes <= 0) {
        return;
    }

    /* decode header data we received from client */
    rv = fsap_decode_header(header_buf, &cmdval, &datasize);
    if (rv == -1) {
        fsa_error(LOG_ERR, "unable to decode header sent by client");
        close(client_fd);
        FD_CLR(client_fd, &master_read_fds);
        return;
    }

    fsa_error(LOG_DEBUG, "got command: %d, datasize: %d", cmdval, datasize);

    /* work out which command the client is requesting */
    switch (cmdval) {
        case ADM_CMD_GET_KB_INFO_ALL:
            handle_cmd_get_kb_info_all(client_fd);
            break;
        case ADM_CMD_GET_KB_INFO:
            handle_cmd_get_kb_info(client_fd, datasize);
            break;
        case ADM_CMD_STOP_KB:
            handle_cmd_stop_kb(client_fd, datasize,0);
            break;
        case ADM_CMD_FSTOP_KB:
            handle_cmd_stop_kb(client_fd, datasize,1);
            break;
        case ADM_CMD_START_KB:
            handle_cmd_start_kb(client_fd, datasize);
            break;
        case ADM_CMD_STOP_KB_ALL:
            handle_cmd_stop_kb_all(client_fd,0);
            break;
        case ADM_CMD_FSTOP_KB_ALL:
            handle_cmd_stop_kb_all(client_fd,1);
            break;
        case ADM_CMD_START_KB_ALL:
            handle_cmd_start_kb_all(client_fd, 0);
            break;
        case ADM_CMD_DELETE_KB:
            handle_cmd_delete_kb(client_fd, datasize);
            break;
        case ADM_CMD_CREATE_KB:
            handle_cmd_create_kb(client_fd, datasize);
            break;
        default:
            fsa_error(LOG_ERR, "unknown client request");
            close(client_fd);
            FD_CLR(client_fd, &master_read_fds);
            break;
    }
}

/* Main server loop, uses select to listen/respond to requests */
static int server_loop(void)
{
    int rv, fd;

    while(1) {
        /* make sure listeneing server fd is in the set */
        read_fds = master_read_fds;

        /* block until we have new connection or data from client */
        rv = select(fd_max+1, &read_fds, NULL, NULL, NULL);
        if (rv == -1) {
            fsa_error(LOG_ERR, "select failed: %s", strerror(errno));
            return -1;
        }

        /* loop through all fds, check if any ready to be read */
        for (fd = 0; fd <= fd_max; fd++) {
            if (FD_ISSET(fd, &read_fds)) {
                if (fd == listener_fds[0]) {
                    /* new ipv4 client */
                    handle_new_connection(0); 
                }
                else if (fd == listener_fds[1]) {
                    /* new ipv6 client */
                    handle_new_connection(1); 
                }
                else {
                    /* client has data for us */
                    handle_client_data(fd);
                }
            }
        }
    }
}

int main(int argc, char **argv)
{
    int rv;

    /* Set program name globally */
    program_invocation_name = argv[0];
    program_invocation_short_name = basename(argv[0]);

    /* Log to stderr until (and if) we daemonize */
    fsa_log_to = ADM_LOG_TO_STDERR;
    fsa_log_level = ADM_LOG_LEVEL;

    /* parse command line args/opts into globals */
    rv = parse_cmdline_opts(argc, argv);
    if (rv == -1) {
        return EXIT_FAILURE;
    }

    /* check if debugging output turned on */
    if (debug_flag) {
        fsa_log_level = LOG_DEBUG;
    }

    /* handle simple commands (help/version) */
    if (help_flag) {
        print_help();
        return EXIT_SUCCESS;
    }

    if (version_flag) {
        print_version();
        return EXIT_SUCCESS;
    }

    /* make sure a valid port number is set/specified */
    rv = init_server_port();
    if (rv == -1) {
        return EXIT_FAILURE;
    }

    /* check that bin dir exists and is readable */
    rv = check_bin_dir();
    if (rv == -1) {
        return EXIT_FAILURE;        
    }

    /* daemonize if needed */
    if (daemonize_flag) {
        daemonize();
    }

    /* set up server listening socket */
    rv = setup_server();
    if (rv == -1) {
        return EXIT_FAILURE;
    }

    if (daemonize_flag) {
        /* make info message a bit clearer in syslog */
        fsa_error(LOG_INFO, "%s started on port %s",
                  program_invocation_short_name, server_port);
    }
    else {
        fsa_error(LOG_INFO, "started on port %s", server_port);
    }

    /* handle client connections and requests */
    rv = server_loop();
    if (rv == -1) {
        return EXIT_FAILURE;
    }

    return EXIT_SUCCESS;
}
