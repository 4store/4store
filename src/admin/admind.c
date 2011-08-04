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

#include "admin_protocol.h"
#include "admin_backend.h"

/***** Server State *****/
/* Fields initialised by parse_cmdline_opts() */
static int verbose_flag = 0;
static int help_flag = 0;
static int version_flag = 0;
static int daemonize_flag = 1;
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

/* Perform any global setup operations here */
static void setup_logging(void)
{
    /* Enable logging to syslog and set default level */
    fsp_syslog_enable();
    setlogmask(LOG_UPTO(LOG_DEBUG));
}

/* Close syslog and exit with given code */
static void cleanup_logging(void)
{
    fsp_syslog_disable();
}

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
        fs_error(LOG_ERR, "fork() error starting daemon: %s",
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
        fs_error(LOG_ERR, "setsid() error starting daemon: %s",
                 strerror(errno));
        exit(EXIT_FAILURE);
    }

    /* fork again to separate from parent permanently */
    fork_from_parent();

    /* change working directory somewhere known */
    if (chdir("/") == -1) {
        fs_error(LOG_ERR, "chdir failed: %s", strerror(errno));
    }

    /* cleanly close syslog */
    cleanup_logging();

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

    /* reopen syslog */
    setup_logging();
}

/* receive data from client, clear from master fd set on error/hangup */
static int recv_from_client(int client_fd, unsigned char *buf, int len)
{
    int nbytes = recv(client_fd, buf, len, MSG_WAITALL);
    if (nbytes <= 0) {
        if (nbytes == 0) {
            fs_error(LOG_DEBUG, "client on fd %d hung up", client_fd);
        }
        else {
            fs_error(LOG_ERR, "error receiving from client: %s",
                     strerror(errno));
        }

        /* cleanup client fd */
        close(client_fd);
        FD_CLR(client_fd, &master_read_fds);
    }

    return nbytes;
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
        fs_error(LOG_ERR, "getaddrinfo failed: %s", gai_strerror(rv));
        return -1;
    }

    /* loop until we can bind to a socket */
    for (p = ai; p != NULL; p = p->ai_next) {
        listener_fd = socket(p->ai_family, p->ai_socktype, p->ai_protocol);
        if (listener_fd == -1) {
            fs_error(LOG_DEBUG, "socket failed: %s", strerror(errno));
            continue;
        }

        /* Get rid of address in use error, ignore errors */
        rv = setsockopt(listener_fd, SOL_SOCKET, SO_REUSEADDR,
                        &yes, sizeof(int));
        if (rv == -1) {
            fs_error(LOG_WARNING, "setsockopt SO_REUSEADDR failed: %s",
                     strerror(errno));
        }

        /* attempt to bind to socket */
        rv = bind(listener_fd, p->ai_addr, p->ai_addrlen);
        if (rv == -1) {
            fs_error(LOG_DEBUG, "server socket bind failed: %s",
                     strerror(errno));
            close(listener_fd);
            continue;
        }

        /* found something to bind to, so break out of loop */
        fs_error(LOG_DEBUG, "socket bind successful");
        break;
    }

    /* done with addrinfo now */
    freeaddrinfo(ai);

    /* if NULL, it means we failed to bind to anything */
    if (p == NULL) {
        fs_error(LOG_ERR, "failed to bind to any socket");
        return -1;
    }

    /* start listening on bound socket */
    rv = listen(listener_fd, 5);
    if (rv == -1) {
        fs_error(LOG_ERR, "failed to listen on socket: %s", strerror(errno));
        return -1;
    }

    /* add listener to master set */
    FD_SET(listener_fd, &master_read_fds);

    /* track largest file descriptor */
    fd_max = listener_fd;

    return 0;
}

/* convenience function - logs error, and sends it to client */
static void send_error_message(int sock_fd, const char *msg)
{
    fs_error(LOG_ERR, msg);

    int len, rv;
    unsigned char *response = fsap_encode_rsp_error(msg, &len);
    if (response == NULL) {
        fs_error(LOG_CRIT, "failed to encode error message");
        return;
    }

    rv = fsa_sendall(sock_fd, response, &len);
    free(response); /* done with response buffer */

    if (rv == -1) {
        fs_error(LOG_ERR, "failed to send response to client: %s",
                 strerror(errno));
    }
}


/* accept new client connection, and add to master read fd set */
static void handle_new_connection(void)
{
    struct sockaddr_storage remote_addr;
    socklen_t addr_len = sizeof(remote_addr);
    int new_fd;

    new_fd = accept(listener_fd, (struct sockaddr *)&remote_addr, &addr_len);
    if (new_fd == -1) {
        fs_error(LOG_ERR, "accept failed: %s", strerror(errno));
        return;
    }

    /* add client to master fd set */
    FD_SET(new_fd, &master_read_fds);
    if (new_fd > fd_max) {
        fd_max = new_fd;
    }

    fs_error(LOG_DEBUG, "new connection on fd %d", new_fd);
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

    fs_error(LOG_DEBUG, "response size is %d bytes", len);

    /* send entire response back to client */
    rv = fsa_sendall(client_fd, response, &len);
    free(response); /* done with response buffer */
    if (rv == -1) {
        fs_error(LOG_ERR, "failed to send response to client: %s",
                 strerror(errno));
        return;
    }

    fs_error(LOG_DEBUG, "%d bytes sent to client", len);
}

/* get all information about a specific kb on this host, send to client */
static void handle_cmd_get_kb_info(int client_fd, uint16_t datasize)
{
    int rv, len, nbytes;
    fsa_kb_info *ki;
    unsigned char *response;
    unsigned char *kb_name;

    /* datasize = num chars in kb name */
    kb_name = (unsigned char *)malloc(datasize + 1);

    /* get kb name from client */
    nbytes = recv_from_client(client_fd, kb_name, datasize);
    if (nbytes <= 0) {
        /* errors already logged/handled */
        free(kb_name);
        return;
    }
    kb_name[datasize] = '\0';

    ki = fsab_get_local_kb_info((char *)kb_name);
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

    fs_error(LOG_DEBUG, "response size is %d bytes", len);

    /* send entire response back to client */
    rv = fsa_sendall(client_fd, response, &len);
    free(response); /* done with response buffer */
    if (rv == -1) {
        fs_error(LOG_ERR, "failed to send response to client: %s",
                 strerror(errno));
        return;
    }

    fs_error(LOG_DEBUG, "%d bytes sent to client", len);
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
        fs_error(LOG_ERR, "unable to decode header sent by client");
        close(client_fd);
        FD_CLR(client_fd, &master_read_fds);
        return;
    }

    fs_error(LOG_DEBUG, "got command: %d, datasize: %d", cmdval, datasize);

    /* work out which command the client is requesting */
    switch (cmdval) {
        case ADM_CMD_GET_KB_INFO_ALL:
            handle_cmd_get_kb_info_all(client_fd);
            break;
        case ADM_CMD_GET_KB_INFO:
            handle_cmd_get_kb_info(client_fd, datasize);
            break;
        default:
            fs_error(LOG_ERR, "unknown client request");
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
            fs_error(LOG_ERR, "select failed: %s", strerror(errno));
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
    progname = basename(argv[0]);

    /* parse command line args/opts into globals */
    rv = parse_cmdline_opts(argc, argv);
    if (rv == -1) {
        return EXIT_FAILURE;
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

    /* Enable syslog and log level (errors to stderr so far) */
    setup_logging();

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
