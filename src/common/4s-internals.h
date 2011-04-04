#include "4store-config.h"
#include "4store.h"
#include "server.h"
#include "params.h"

#include <unistd.h>
#include <stdint.h>
#include <syslog.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>

#include <glib.h>

/* anyway this stuff is opaque, don't look inside the box */

#define FS_MAX_NODES 32

#ifdef FS_MD5
#define FS_PROTO_VER_MINOR 0x80
#endif
#ifdef FS_CRC64
#define FS_PROTO_VER_MINOR 0x81
#endif
#ifdef FS_UMAC
#define FS_PROTO_VER_MINOR 0x82
#endif

#define FS_DEFAULT_PORT 6734

struct fsp_link_struct {
  const char *kb_name;
  fsp_hash_enum hash_type;
  unsigned char hash[16];
  int servers;
  const char *addrs[FS_MAX_NODES];
  uint16_t ports[FS_MAX_NODES];

  int segments;
  int groups[FS_MAX_SEGMENTS];
  int socks[FS_MAX_SEGMENTS];
  int socks1[FS_MAX_SEGMENTS];
  int socks2[FS_MAX_SEGMENTS]; /* for failover */
  long long tics[FS_MAX_SEGMENTS];
  GStaticMutex mutex[FS_MAX_SEGMENTS];
  const char *features;
  int hit_limits;
#if defined(USE_AVAHI)
  void *avahi_browser;
  void *avahi_client;
#elif defined(USE_DNS_SD)
  int try_dns_again;
#endif
};

/* common functions */

void default_hints(struct addrinfo *hints);
unsigned char *message_recv(int sock, fs_segment *segment, unsigned int *length); /* free result pls */

int fsp_add_backend (fsp_link *link, const char *addr, uint16_t port, int segments);

int fsp_ver_fixup (fsp_link *link, int sock);

void fsp_mdns_setup_backend (uint16_t port, const char *kb_name, int segments);
int fsp_mdns_retry_frontend (fsp_link *link, int msecs);
void fsp_mdns_cleanup_frontend (fsp_link *link);
void fsp_mdns_setup_frontend (fsp_link *link);

GHashTable * fs_hash_bnids(void);

/* functions used by g_hash_table_* */
guint fs_rid_hash(gconstpointer p);
gboolean fs_rid_equal(gconstpointer va, gconstpointer vb);
