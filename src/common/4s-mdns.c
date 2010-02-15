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
*/
/*
 *  Copyright 2006 Nick Lamb for Garlik.com
 */

#include "4s-internals.h"
#include "error.h"

#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <glib.h>

#define SERVICE_TYPE "_4store._tcp"

#if defined(USE_AVAHI)

#include <net/if.h>
#include <avahi-client/client.h>
#include <avahi-client/publish.h>
#include <avahi-client/lookup.h>
#include <avahi-common/simple-watch.h>
#include <avahi-common/error.h>
#include <avahi-common/address.h>
#include <avahi-glib/glib-watch.h>

/* globals for client */
static int unresolved = 0, all_for_now = 0, found = 0;

static AvahiSimplePoll *spoll = NULL;

/* typedef for server */
typedef struct {
  int established;
  char *service;
  int port;
  char *kb_name;
  char *segments;
  AvahiGLibPoll *poll;
} fsp_mdns_state;

static void client_callback_frontend (AvahiClient *client,
                                      AvahiClientState state, void *userdata)
{
 /* FIXME what if anything should we do when Avahi goes away ? */
}

static void group_callback (AvahiEntryGroup *group,
                            AvahiEntryGroupState state, void *userdata)
{
  fsp_mdns_state *ms = (fsp_mdns_state *) userdata;

  switch (state) {
    case AVAHI_ENTRY_GROUP_ESTABLISHED:
      ms->established = 1;
      break;
    case AVAHI_ENTRY_GROUP_FAILURE:
      ms->established = -1;
      break;
    default:
      break;
  } 
}

static void resolve_callback (AvahiServiceResolver *browser,
                              AvahiIfIndex interface,
                              AvahiProtocol protocol,
                              AvahiResolverEvent event,
                              const char *name,
                              const char *type,
                              const char *domain,
                              const char *host_name,
                              const AvahiAddress *address,
                              uint16_t port,
                              AvahiStringList *txt,
                              AvahiLookupResultFlags flags,
                              void *userdata)
{
  fsp_link *link = (fsp_link *) userdata;
  /* address + % + interface */
  char addr[AVAHI_ADDRESS_STR_MAX + IF_NAMESIZE + 1];

  unresolved--;
  if (event == AVAHI_RESOLVER_FOUND) {
    AvahiStringList *txt_kb_name = avahi_string_list_find(txt, "kb");
    char *key, *value;
    avahi_string_list_get_pair(txt_kb_name, &key, &value, NULL);
    if (strcmp(link->kb_name, value)) {
      free(key); free(value);
      return;
    }
    free(key); free(value);

    txt_kb_name = avahi_string_list_find(txt, "segments");
    avahi_string_list_get_pair(txt_kb_name, &key, &value, NULL);

    int segments = atoi(value);
    free(key); free(value);

    avahi_address_snprint(addr, sizeof(addr), address);
    if (address->proto == AVAHI_PROTO_INET6) {
      /* Avahi doesn't understand about link-local IPv6 properly, cope */
      /* 1111 1110 is not as precise as it could be but good enough for us */
      if (address->data.ipv6.address[0] == 0xfe && interface >= 0) {
        int slen = strlen(addr);
        addr[slen] = '%';
        if_indextoname(interface, addr + slen + 1);
      }
    }
    found += fsp_add_backend (link, addr, port, segments);
  }
}

static void browse_callback (AvahiServiceBrowser *browser,
                             AvahiIfIndex interface,
                             AvahiProtocol protocol, 
                             AvahiBrowserEvent event,
                             const char *name,
                             const char *type,
                             const char *domain,
                             AvahiLookupResultFlags flags,
                             void *userdata)
{
  AvahiClient *client = avahi_service_browser_get_client(browser);

  switch (event) {
  case AVAHI_BROWSER_NEW:
    unresolved++;
    avahi_service_resolver_new(client, interface, protocol, name, type,
                               domain, AVAHI_PROTO_UNSPEC, 0,
                               resolve_callback, userdata);
    break;

  case AVAHI_BROWSER_ALL_FOR_NOW:
    all_for_now = 1;
    break;

  case AVAHI_BROWSER_CACHE_EXHAUSTED:
    break;

  default:
    /* FIXME */
    break;
  }
}

void fsp_mdns_setup_frontend (fsp_link *link)
{
  int avahi_error;

  spoll = avahi_simple_poll_new();
  const AvahiPoll *poll_api = avahi_simple_poll_get(spoll);
  AvahiClient *avahi_client = avahi_client_new(poll_api, AVAHI_CLIENT_NO_FAIL,
                                               client_callback_frontend, NULL,
                                               &avahi_error);
  if (!avahi_client) {
    link_error(LOG_WARNING, "while creating Avahi client: %s",
               avahi_strerror(avahi_error));
    return;
  }

  AvahiServiceBrowser *avahi_browser = 
        avahi_service_browser_new(avahi_client, AVAHI_IF_UNSPEC,
                                  AVAHI_PROTO_UNSPEC, SERVICE_TYPE, NULL, 0,
                                  browse_callback, (void *) link);
  if (!avahi_browser) {
    link_error(LOG_WARNING, "while creating Avahi browser: %s",
               avahi_strerror(avahi_client_errno(avahi_client)));
    return;
  }

  all_for_now = 0; found = 0;
  int done = 0;
  do {
    avahi_simple_poll_iterate(spoll, -1);
    done = (link->segments && found >= link->segments);
  } while (unresolved > 0 || (!done && !all_for_now));

  link->avahi_browser = avahi_browser;
  link->avahi_client = avahi_client;
}

int fsp_mdns_retry_frontend (fsp_link *link, int msecs)
{
  struct timeval now;

  gettimeofday(&now, NULL);
  long long from = now.tv_sec * 1000000 + now.tv_usec;
  
  do {
    int ret = avahi_simple_poll_iterate(spoll, msecs);
    if (ret) return ret;
    gettimeofday(&now, NULL);
    long long until = now.tv_sec * 1000000 + now.tv_usec;
    msecs -= (until - from) / 1000;
    if (msecs < 1) return 0;
  } while (unresolved > 0 || (found < link->segments));

  link_error(LOG_INFO, "found more backend nodes with %dms remaining", msecs);
  return 0;
}

void fsp_mdns_cleanup_frontend (fsp_link *link)
{
   avahi_service_browser_free((AvahiServiceBrowser *) link->avahi_browser);
   link->avahi_browser = NULL;
   avahi_client_free((AvahiClient *) link->avahi_client);
   link->avahi_client = NULL;

   avahi_simple_poll_free(spoll);
   spoll = NULL;
}

static void create_services (AvahiClient *client, fsp_mdns_state *ms)
{
  int avahi_error;

  AvahiEntryGroup* avahi_group = avahi_entry_group_new(client,
                                                       group_callback, ms);
  avahi_error = avahi_entry_group_add_service(avahi_group, AVAHI_IF_UNSPEC,
                                              AVAHI_PROTO_UNSPEC, 0,
                                              ms->service, SERVICE_TYPE, NULL,
                                              NULL, ms->port,
                                              ms->kb_name, ms->segments, NULL);

  if (avahi_error == AVAHI_ERR_COLLISION) {
    fs_error(LOG_ERR, "there is already a backend running for KB %s", ms->kb_name);
    ms->established = -1;
    return;
  }

  if (avahi_error) {
    fs_error(LOG_WARNING, "while adding mDNS service for KB %s: %s",
             ms->kb_name, avahi_strerror(avahi_error));
  }

  avahi_error = avahi_entry_group_commit(avahi_group);
  if (avahi_error) {
    fs_error(LOG_WARNING, "while commiting mDNS group for KB %s: %s",
             ms->kb_name, avahi_strerror(avahi_error));
    ms->established = -1;
  }
}

static void client_callback_backend (AvahiClient *client,
                                     AvahiClientState state, void *userdata)
{
  fsp_mdns_state *ms = (fsp_mdns_state *) userdata;

  switch (state) {
    case AVAHI_CLIENT_FAILURE:
      ms->established = 0;
      int avahi_error = avahi_client_errno(client);
      if (avahi_error == AVAHI_ERR_DISCONNECTED) {
        fs_error(LOG_INFO, "reconnecting to Avahi for %s", ms->kb_name);
        const AvahiPoll *poll_api = avahi_glib_poll_get(ms->poll);
        client = avahi_client_new(poll_api, AVAHI_CLIENT_NO_FAIL,
                                  client_callback_backend, ms, NULL);
      } else {
        fs_error(LOG_WARNING, "client failed: %s", avahi_strerror(avahi_error));
      }
      break;
    case AVAHI_CLIENT_S_RUNNING:
      create_services(client, (fsp_mdns_state *) userdata);
      break;
    default:
      break;
  }
}

void fsp_mdns_setup_backend (uint16_t port, const char *kb_name,
                               int segments)
{
  int avahi_error;
  char host_name[HOST_NAME_MAX + 1];
  gethostname(host_name, HOST_NAME_MAX);

  fsp_mdns_state *ms = g_malloc0(sizeof(fsp_mdns_state));
  ms->kb_name = g_strdup_printf("kb=%s", kb_name);
  ms->segments = g_strdup_printf("segments=%d", segments);
  ms->service = g_strdup_printf("%s-%s", host_name, kb_name);
  ms->port = port;
  
  ms->poll = avahi_glib_poll_new(NULL, G_PRIORITY_DEFAULT);

  const AvahiPoll *poll_api = avahi_glib_poll_get(ms->poll);
  AvahiClient *avahi_client = avahi_client_new(poll_api, 0,
                                               client_callback_backend,
                                               ms, &avahi_error);
  if (!avahi_client) {
    fs_error(LOG_WARNING, "while creating Avahi client for KB %s: %s", kb_name,
             avahi_strerror(avahi_error));

    avahi_glib_poll_free(ms->poll);
    g_free(ms->kb_name);
    g_free(ms->segments);
    g_free(ms->service);
    g_free(ms);
    return;
  }

  do {
    g_main_context_iteration(NULL, TRUE);
  } while (!ms->established);

  if (ms->established != 1) {
    avahi_error = avahi_client_errno(avahi_client);
    fs_error(LOG_WARNING, "couldn't establish mDNS service for KB %s: %s",
             kb_name, avahi_strerror(avahi_error));
    exit(1); /* this is fatal */
  }

  return;
}

#elif defined(USE_DNS_SD)

#include <errno.h>
#include <dns_sd.h>
#include <limits.h>
#include <net/if.h>
#include <sys/select.h>

#include "error.h"

#ifndef HOST_NAME_MAX
#define HOST_NAME_MAX _POSIX_HOST_NAME_MAX
#endif

static int found = 0;

/* Use apple-style DNS-SD library */

static char *get_txt_string(const unsigned char *txt, int txtlen, char *key)
{
    int pos = 0;
    while (pos < txtlen) {
        int len = txt[pos++];
        if (strncmp((char *)txt+pos, key, strlen(key)) == 0) {
            char *ret = malloc(len - strlen(key));
            strncpy(ret, (char *)txt+pos + strlen(key) + 1, len - strlen(key) - 1);
            ret[len - strlen(key) - 1] = '\0';

            return ret;
        }
    }

    return NULL;
}

static void resolve_reply
    (DNSServiceRef sdRef, DNSServiceFlags flags, uint32_t interface_index,
     DNSServiceErrorType error_code, const char *fullname,
     const char *hosttarget, uint16_t noport, uint16_t txt_len,
     const unsigned char *txt, void *context)
{
    fsp_link *link = context;
    int port = ntohs(noport);

    char *kb = get_txt_string(txt, txt_len, "kb");
    if (strcmp(link->kb_name, kb) != 0) {
        return;
    }
    free(kb);
    char *segments_str = get_txt_string(txt, txt_len, "segments");
    int segments = atoi(segments_str);
    free(segments_str);
//printf("@@ resolved %s:%d (%d)\n", hosttarget, port, segments);
    int oldfound = found;
    /* sometimes we get these odd looking .members.mac.com addresses on OSX,
     * they don't seem to work, so lets skip them */
    if (!strstr(hosttarget, ".members.mac.com")) {
//printf("@@ adding %s:%d (%d)\n", hosttarget, port, segments);
        found += fsp_add_backend(link, hosttarget, port, segments);
//printf("@@ found = %d\n", found);
    }
    /* if the address we got didn't help then try again */
    if (oldfound == found && found < segments) {
        link->try_dns_again = 1;
    }
}

static void browse_reply
    (DNSServiceRef inref, DNSServiceFlags flags, uint32_t interfaceIndex,
     DNSServiceErrorType errorCode, const char *service_name,
     const char *regtype, const char *domain, void *context)
{
    fsp_link *link = context;

//printf("@@ found service_name=%s, if=%d, err=%d\n", service_name, interfaceIndex, errorCode);
    DNSServiceRef ref;
    if (DNSServiceResolve(&ref, 0, interfaceIndex, service_name, regtype,
            domain, resolve_reply, context) != kDNSServiceErr_NoError) {
        fs_error(LOG_ERR, "failed to resolve %s: %s", service_name,
                 strerror(errno));

        return;
    }
    const int rfd = DNSServiceRefSockFD(ref);
    fd_set resp_fds;
    retry:;
    link->try_dns_again = 0;
    FD_ZERO(&resp_fds);
    FD_SET(rfd, &resp_fds);
    struct timeval timeout = { .tv_sec = 10, .tv_usec = 0 };
    int sel = select(rfd+1, &resp_fds, NULL, NULL, &timeout);
    if (sel == 1) {
        DNSServiceProcessResult(ref);
    } else if (sel == -1) {
        fs_error(LOG_ERR, "select failed: %s", strerror(errno));
    } else if (sel == 0) {
        fs_error(LOG_INFO, "waiting for mDNS response");
    } else {
        fs_error(LOG_ERR, "select returned %d", sel);
    }
    if (link->try_dns_again) {
        goto retry;
    }
    DNSServiceRefDeallocate(ref);
}

void fsp_mdns_setup_frontend(fsp_link *link)
{
  DNSServiceRef ref;

  if (DNSServiceBrowse(&ref, 0, 0, SERVICE_TYPE, NULL, browse_reply, link) !=
      kDNSServiceErr_NoError) {
    fs_error(LOG_ERR, "failed to browse for "SERVICE_TYPE": %s",
	     strerror(errno));

    return;
  }

  const int bfd = DNSServiceRefSockFD(ref);
//printf("@@ bfd=%d\n", bfd);
  fd_set browse_fds;
  do {
    FD_ZERO(&browse_fds);
    FD_SET(bfd, &browse_fds);
    struct timeval timeout = { .tv_sec = 2, .tv_usec = 0 };
    int sel = select(bfd+1, &browse_fds, NULL, NULL, &timeout);
    if (sel == 1) {
      if (DNSServiceProcessResult(ref) != kDNSServiceErr_NoError) {
        fs_error(LOG_ERR, "failed to process result for "SERVICE_TYPE": %s",
             strerror(errno));
        return;
      }
    } else if (sel == -1) {
      fs_error(LOG_ERR, "select faild: %s", strerror(errno));
    } else if (sel == 0) {
      fs_error(LOG_INFO, "timed out waiting for mDNS response");
    }
  } while (link->segments == 0 || found < link->segments);
  DNSServiceRefDeallocate(ref);

  return;
}

int fsp_mdns_retry_frontend (fsp_link *link, int msecs)
{
  return 1;
}

void fsp_mdns_cleanup_frontend (fsp_link *link)
{
  return;
}

void fsp_mdns_setup_backend(uint16_t port, const char *kb_name, int segments)
{
  char host_name[HOST_NAME_MAX + 1];
  gethostname(host_name, HOST_NAME_MAX);

  char *service = g_strdup_printf("%s-%s", host_name, kb_name);
  unsigned char kblen = 3 + strlen(kb_name);
  char *segs = g_strdup_printf("segments=%d", segments);
  char *txt_record = g_strdup_printf("%ckb=%s%c%s", kblen, kb_name, (char)strlen(segs), segs);

  DNSServiceRef ref;
  if (DNSServiceRegister(&ref, 0, 0, service, SERVICE_TYPE, NULL, NULL, htons(port), strlen(txt_record), txt_record, NULL, NULL) != kDNSServiceErr_NoError) {
    fs_error(LOG_ERR, "failed to register mDNS advert: %s", strerror(errno));
  }
  g_free(service);
  g_free(segs);
  g_free(txt_record);

  return;
}

#else

/* fallback if there's no MDNS library */

void fsp_mdns_setup_frontend (fsp_link *link)
{
  fsp_add_backend(link, "127.0.0.1", FS_DEFAULT_PORT, 0); /* try loopback */
  return;
}

int fsp_mdns_retry_frontend (fsp_link *link, int msecs)
{
  /* this version used when USE_AVAHI is not defined */

  return 1;
}

void fsp_mdns_cleanup_frontend (fsp_link *link)
{
  /* this version used when USE_AVAHI is not defined */

  return;
}


void fsp_mdns_setup_backend (uint16_t port, const char *kb_name, int segments)
{
  /* this version used when USE_AVAHI is not defined */

  return;
}

#endif

/* vi:set expandtab sts=4 sw=4: */
