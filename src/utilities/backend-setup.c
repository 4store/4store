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
 *  Copyright (C) 2006 Steve Harris for Garlik
 */

#include <errno.h>
#include <stdio.h>
#include <ctype.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <glib.h>
#include <getopt.h>
#include <locale.h>
#include <unistd.h>
#include <libgen.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>

#include "../common/uuid.h"
#include "../common/4store.h"
#include "../common/error.h"
#include "../common/params.h"
#include "../common/md5.h"
#include "../backend/backend.h"
#include "../backend/metadata.h"

typedef struct {
  char *name;
  char *password;
  int node;
  int cluster;
  int segments;
  int mirror;
  int model_files;
} kbconfig;

void create_dir(kbconfig *config);
void setup_metadata(kbconfig *config);
void erase_db(kbconfig *config);

static int verbosity = 0;
static int dummy = 0;

#define LOG(l, a...) { if (verbosity >= l) printf(a); }

int primary_segment(kbconfig *config, int segment)
{
    return (segment % config->cluster) == config->node;
}

int mirror_segment(kbconfig *config, int segment)
{
    if (config->cluster < 2 || config->mirror == 0) return FALSE;

    int offset = ((segment / config->cluster) % (config->cluster - 1)) + 1;
    return ((segment + offset) % config->cluster) == config->node;
}

int uses_segment(kbconfig *config, int segment)
{
    return primary_segment(config, segment) | mirror_segment(config, segment);
}

int main(int argc, char *argv[])
{
    char *optstring = "nv";
    int help = 0;
    int c, opt_index = 0;
    kbconfig config = {
        .name = NULL,
        .password = NULL,
        .node = 0,
        .cluster = 1,
        .segments = 2,
        .mirror = 0,
        .model_files = 0,
    };

    static struct option long_options[] = {
        { "help", 0, 0, 'h' },
        { "version", 0, 0, 'V' },
        { "verbose", 0, 0, 'v' },
        { "mirror", 0, 0, 'm' },
        { "model-files", 0, 0, 'f' },
        { "print-only", 0, 0, 'n' },
        { "node", 1, 0, 'N' },
        { "cluster", 1, 0, 'C' },
        { "segments", 1, 0, 'S' },
        { "password", 1, 0, 'P' },
        { 0, 0, 0, 0 }
    };

    setlocale(LC_ALL, NULL);
    int help_return = 1;

    while ((c = getopt_long (argc, argv, optstring, long_options, &opt_index)) != -1) {
	if (c == 'v') {
	    verbosity++;
	} else if (c == 'm') {
	    config.mirror = 1;
	} else if (c == 'f') {
	    config.model_files = 1;
	} else if (c == 'n') {
	    dummy = 1;
	} else if (c == 'N') {
	    config.node = atoi(optarg);
	} else if (c == 'C') {
	    config.cluster = atoi(optarg);
	} else if (c == 'S') {
	    config.segments = atoi(optarg);
	} else if (c == 'P') {
	    config.password = optarg;
	} else if (c == 'h') {
	    help = 1;
	    help_return = 0;
	} else if (c == 'V') {
	    printf("%s, built for 4store %s\n", basename(argv[0]), GIT_REV);
	    exit(0);
	} else {
	    help++;
	}
    }

    if (optind == argc - 1) {
	config.name = argv[optind];
    }

    if (config.segments < 1 || config.node < 0 ||
        config.node >= config.cluster || config.name == NULL) {
        help = 1;
    }

    if (help) {
        fprintf(stdout, "%s, built for 4store %s\n", basename(argv[0]), GIT_REV);
        fprintf(stdout, "Usage: %s [-v] --node <id> --cluster <size>\n\t--segments <seg> [--password <pw>] <kbname>\n", basename(argv[0]));
        fprintf(stdout, "   --node <id>       node id 0 ... cluster-1\n");
        fprintf(stdout, "   --cluster <size>  number of nodes in cluster\n");
        fprintf(stdout, "   --segments <seg>  number of segments in cluster\n");
        fprintf(stdout, "   --password <pw>   password for authentication\n");
        fprintf(stdout, "   -m, --mirror      mirror segments\n");
        fprintf(stdout, "   --model-files     use a file per-model (for large models)\n");
        fprintf(stdout, "   -v, --verbose     increase verbosity\n");
        fprintf(stdout, "   -n, --print-only  dont execute commands, just show\n");
        fprintf(stdout, "This command creates KBs, if the KB already exists, its contents are lost.\n");
        return help_return;
    }

    /* check segments is a power of 2 */
    for (int s= config.segments; s > 0; s = s >> 1) {
      if ((s & 1) == 1 && s != 1) {
         fprintf(stderr, "Number of segments must be a power of 2\n");
         return 1;
      }
    }

    fsp_syslog_enable(); /* from now on errors are logged */

    for (int k= 0; k < strlen(config.name); ++k) {
      if (!index(FS_LEGAL_KB_CHARS, config.name[k])) {
        fs_error(LOG_ERR, "character '%c' not allowed in KB name %s", config.name[k], config.name);
        return 1;
      }
    }

    fs_backend *be = fs_backend_init(config.name, FS_BACKEND_NO_OPEN);
    create_dir(&config);

    erase_db(&config);
    fs_error(LOG_INFO, "erased files for KB %s", config.name);

    setup_metadata(&config);

    fs_backend_fini(be);

    return 0;
}

void create_dir(kbconfig *config)
{
    char *tmp;

    LOG(1, "Creating data directory");
    tmp = g_strdup_printf("%s/%s", FS_STORE_ROOT, config->name);
    if (mkdir(tmp, 0755)) {
	if (errno != EEXIST) {
	    fprintf(stderr, "Failed to create store directory '%s': %s\n", 
		tmp, strerror(errno));
	    exit(1);
	}
    }
    g_free(tmp);

    LOG(1, "Create segment directories");
    for (int i=0; i<config->segments; i++) {
	if (!uses_segment(config, i)) continue;
	tmp = g_strdup_printf("%s/%s/%04x", FS_STORE_ROOT, config->name, i);
	if (mkdir(tmp, 0755)) {
	    if (errno != EEXIST) {
		fprintf(stderr, "Failed to create store directory '%s': %s\n", 
		    tmp, strerror(errno));
		exit(1);
	    }
	}
	g_free(tmp);
	fs_backend *be = fs_backend_init(config->name, FS_BACKEND_NO_OPEN);
	if (!be) {
	    fprintf(stderr, "Failed to open backed\n");
	    exit(1);
	}
	fs_backend_open_files(be, i, O_CREAT | O_RDWR | O_TRUNC, FS_OPEN_ALL);
	fs_backend_cleanup_files(be);
	fs_backend_close_files(be, i);
	/* create directories for model indexes */
	tmp = g_strdup_printf("%s/%s/%04x/m", FS_STORE_ROOT,
			      config->name, i);
	mkdir(tmp, 0755);
	g_free(tmp);
	fs_backend_fini(be);
    }
}

void erase_db(kbconfig *config)
{
    for (int i=0; i < config->segments; i++) {
        if (!uses_segment(config, i)) continue;
	// TODO cleaner delete method, but this is only an admin operation
        char *command = g_strdup_printf("rm -rf %s/%s/", FS_STORE_ROOT, config->name);
	system(command);
	g_free(command);
    }
    create_dir(config);
}

void setup_metadata(kbconfig *config)
{
    LOG(1, "Writing metadata.\n");

    fs_metadata *md = fs_metadata_open(config->name);
    fs_metadata_clear(md);
    fs_metadata_set(md, FS_MD_NAME, config->name);
    fs_metadata_set(md, FS_MD_HASHFUNC, FS_HASH);
    fs_metadata_set(md, FS_MD_STORE, "native");
    fs_metadata_set(md, FS_MD_MODEL_DATA, "true");
    if (config->model_files) {
        fs_metadata_set(md, FS_MD_MODEL_FILES, "true");
    } else {
        fs_metadata_set(md, FS_MD_MODEL_FILES, "false");
    }
    fs_metadata_set(md, FS_MD_CODE_VERSION, GIT_REV);
    for (int seg = 0; seg < config->segments; seg++) {
        if (primary_segment(config, seg))
	    fs_metadata_add_int(md, FS_MD_SEGMENT_P, seg);
        if (mirror_segment(config, seg))
	    fs_metadata_add_int(md, FS_MD_SEGMENT_M, seg);
    }

    /* Generate store UUID for skolemisation */
#if defined(USE_LINUX_UUID)
    uuid_t uu;
    uuid_string_t uus;
    uuid_generate(uu);
    uuid_unparse(uu, uus);
#elif defined(USE_BSD_UUID)
    uuid_t uu;
    char *uus = NULL;
    int status = -1;
    uuid_create(&uu, &status);
    if (status) { fs_error(LOG_ERR, "bad return from uuid_create"); exit(1); }
    uuid_to_string(&uu, &uus, &status);
    if (status || uus == NULL) { fs_error(LOG_ERR, "bad return from uuid_to_string"); exit(1); }
#elif defined(USE_OSSP_UUID)
    uuid_t *uu = NULL;
    char *uus = NULL;
    if (uuid_create(&uu)) { fs_error(LOG_ERR, "bad return from uuid_create"); exit(1); }
    if (uuid_make(uu, UUID_MAKE_V1)) { fs_error(LOG_ERR, "bad return from uuid_make"); exit(1); }
    if (uuid_export(uu, UUID_FMT_STR, &uus, NULL) || uus == NULL) { fs_error(LOG_ERR, "bad return from uuid_export"); exit(1); }
#endif
    fs_metadata_add(md, FS_MD_UUID, uus);
#if defined(USE_OSSP_UUID)
    uuid_destroy(uu);
#endif

    unsigned char stage1[20], stage2[16];
    char hash[33] = "none";
    int now = 0;
    if (config->password) {
        md5_state_t md5;
        char *pw = g_strdup_printf("%s:%s", config->name, config->password);

        /* stage1 will contain the 4 byte Unix time_t value as a salt ... */
        now = time(NULL);
        memcpy(stage1, &now, sizeof(now));

        /* ... followed by the on-wire 16 byte MD5 auth string */
	md5_init(&md5);
        md5_append(&md5, (md5_byte_t *) pw, strlen(pw));
        md5_finish(&md5, stage1 + 4);

        /* now use MD5 on all 20 bytes and store both the salt and the hash */
	md5_init(&md5);
        md5_append(&md5, stage1, sizeof(stage1));
        md5_finish(&md5, stage2);

        sprintf(hash, "%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x",
                stage2[0], stage2[1], stage2[2], stage2[3], stage2[4], stage2[5],
	        stage2[6], stage2[7], stage2[8], stage2[9], stage2[10], stage2[11],
                stage2[12], stage2[13], stage2[14], stage2[15]);

        g_free(pw);
    }

    fs_metadata_add_int(md, FS_MD_VERSION, FS_CURRENT_TABLE_VERSION);
    fs_metadata_add_int(md, FS_MD_SEGMENTS, config->segments);
    fs_metadata_add_int(md, FS_MD_SALT, now);
    fs_metadata_add_int(md, FS_MD_BNODE, 1);
    fs_metadata_add(md, FS_MD_HASH, hash);
    fs_metadata_flush(md);
    fs_metadata_close(md);

    fs_error(LOG_INFO, "created RDF metadata for KB %s", config->name);
}

/* vi:set ts=8 sts=4 sw=4: */
