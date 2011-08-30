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

/* Name of executing program */
static char *progname = NULL;

/* Socket/network globals */
static fd_set master_read_fds;
static fd_set read_fds;
static int fd_max;
static int listener_fd;
/***** End of Server State *****/

static void print_usage(int listhelp) {
    printf("Usage: %s [OPTION]...\n", progname);

    if (listhelp) {
        printf("Try `%s --help' for more information.\n", progname);
    }
}

static void print_help(void)
{
    print_usage(0);

    printf(
"  -p, --port=PORT       specify port to listen on (default: %d)\n"
"  -D, --no-daemonize    do not daemonise\n"
"      --help            display this message and exit\n"
"      --verbose         display more detailed output\n"
"      --debug           display full debugging output\n"
"      --version         display version information and exit\n",
        FS_ADMIND_PORT
    );
}

static void print_version(void)
{
    printf("%s, built for 4store %s\n", progname, GIT_REV);
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
        {NULL,              0,                  NULL,               0}
    };

    while(1) {
        opt_index = 0;
        c = getopt_long(argc, argv, "Dp:", long_opts, &opt_index);

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
                    progname, FS_CONFIG_FILE
                );
            }
            else if (errno == ERANGE) {
                fprintf(
                    stderr,
                    "%s: port number out of range 0-65535 in %s",
                    progname, FS_CONFIG_FILE
                );
            }
            else {
                fprintf(
                    stderr,
                    "%s: unknown error reading port from config file at %s\n",
                    progname, FS_CONFIG_FILE
                );
            }
            return -1;
        }
    }
    else {
        /* cport has been specified on command line */

        if (!fsa_is_int(cport)) {
            fprintf(stderr, "%s: non-numeric port specified on command line\n",
                    progname);
            return -1;
        }

        port = atoi(cport);
        if (port < 0 || port > 65535) {
            fprintf(stderr, "%s: port number %d out of range 0-65535\n",
                    progname, port);
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
    fsa_error(LOG_INFO, "Received %s (%d) signal", strsignal(sig), sig);

    switch (sig) {
        case SIGTERM:
            FD_CLR(listener_fd, &master_read_fds);
            close(listener_fd);
            fsa_error(LOG_INFO, "%s shutdown cleanly",
                      program_invocation_short_name);
            fsp_syslog_disable();
            exit(EXIT_SUCCESS);
            break;
        default:
            fsa_error(LOG_DEBUG, "Signal %s (%d) unhandled",
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
    for (p = ai; p != NULL; p = p->ai_next) {
        listener_fd = socket(p->ai_family, p->ai_socktype, p->ai_protocol);
        if (listener_fd == -1) {
            fsa_error(LOG_DEBUG, "socket failed: %s", strerror(errno));
            continue;
        }

        /* Get rid of address in use error, ignore errors */
        rv = setsockopt(listener_fd, SOL_SOCKET, SO_REUSEADDR,
                        &yes, sizeof(int));
        if (rv == -1) {
            fsa_error(LOG_WARNING, "setsockopt SO_REUSEADDR failed: %s",
                      strerror(errno));
        }

        /* attempt to bind to socket */
        rv = bind(listener_fd, p->ai_addr, p->ai_addrlen);
        if (rv == -1) {
            fsa_error(LOG_DEBUG, "server socket bind failed: %s",
                      strerror(errno));
            close(listener_fd);
            continue;
        }

        /* found something to bind to, so break out of loop */
        fsa_error(LOG_DEBUG, "socket bind successful");
        break;
    }

    /* done with addrinfo now */
    freeaddrinfo(ai);

    /* if NULL, it means we failed to bind to anything */
    if (p == NULL) {
        fsa_error(LOG_ERR, "failed to bind to any socket");
        return -1;
    }

    /* start listening on bound socket */
    rv = listen(listener_fd, 5);
    if (rv == -1) {
        fsa_error(LOG_ERR, "failed to listen on socket: %s", strerror(errno));
        return -1;
    }

    /* add listener to master set */
    fcntl(listener_fd, F_SETFD, FD_CLOEXEC);
    FD_SET(listener_fd, &master_read_fds);

    /* track largest file descriptor */
    fd_max = listener_fd;

    /* set up signal handlers */
    signal(SIGTERM, signal_handler);

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


/* accept new client connection, and add to master read fd set */
static void handle_new_connection(void)
{
    struct sockaddr_storage remote_addr;
    socklen_t addr_len = sizeof(remote_addr);
    int new_fd;

    new_fd = accept(listener_fd, (struct sockaddr *)&remote_addr, &addr_len);
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
    ki = fsab_get_local_kb_info_all();
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
    int rv, len;
    fsa_kb_info *ki;
    unsigned char *response;
    unsigned char *kb_name;

    kb_name = get_string_from_client(client_fd, datasize);
    if (kb_name == NULL) {
        /* errors already logged/handled */
        return;
    }

    ki = fsab_get_local_kb_info(kb_name);
    free(kb_name); /* done with kb_name */
    if (ki == NULL) {
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

static void start_or_stop_kb_all(int client_fd, int action)
{
    int rv, err;
    int n_kbs = 0;

    /* get all local kbs */
    fsa_kb_info *ki = fsab_get_local_kb_info_all();
    if (ki == NULL) {
        send_error_message(client_fd, "unable to find any stores");
        return;
    }

    /* count number of kbs */
    for (fsa_kb_info *p = ki; p != NULL; p = p->next) {
        n_kbs += 1;
    }

    /* tell client to expect a message for each */
    rv = send_expect_n(client_fd, n_kbs);
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
            rv = fsab_stop_local_kb(p->name, &err);
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

static void handle_cmd_stop_kb_all(int client_fd)
{
    start_or_stop_kb_all(client_fd, STOP_STORES);
}

static void handle_cmd_start_kb_all(int client_fd)
{
    start_or_stop_kb_all(client_fd, START_STORES);
}

/* start or stop a running kb */
static void start_or_stop_kb(int client_fd, uint16_t datasize, int action)
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
        rv = fsab_stop_local_kb(kb_name, &err);
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

static void handle_cmd_stop_kb(int client_fd, uint16_t datasize)
{
    start_or_stop_kb(client_fd, datasize, STOP_STORES);
}

static void handle_cmd_start_kb(int client_fd, uint16_t datasize)
{
    start_or_stop_kb(client_fd, datasize, START_STORES);
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
            handle_cmd_stop_kb(client_fd, datasize);
            break;
        case ADM_CMD_START_KB:
            handle_cmd_start_kb(client_fd, datasize);
            break;
        case ADM_CMD_STOP_KB_ALL:
            handle_cmd_stop_kb_all(client_fd);
            break;
        case ADM_CMD_START_KB_ALL:
            handle_cmd_start_kb_all(client_fd);
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
                if (fd == listener_fd) {
                    /* new client is connecting */
                    handle_new_connection(); 
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

    /* daemonize if needed */
    if (daemonize_flag) {
        daemonize();
    }

    /* set up server listening socket */
    rv = setup_server();
    if (rv == -1) {
        return EXIT_FAILURE;
    }

    /* handle client connections and requests */
    rv = server_loop();
    if (rv == -1) {
        return EXIT_FAILURE;
    }

    return EXIT_SUCCESS;
}
