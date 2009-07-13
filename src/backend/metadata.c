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

#include <fcntl.h>
#include <unistd.h>
#include <stdlib.h>
#include <errno.h>
#include <string.h>
#include <raptor.h>
#include <glib.h>
#include <syslog.h>

#include "common/params.h"
#include "common/error.h"
#include "common/datatypes.h"
#include "metadata.h"

struct m_entry {
    char *key;
    char *val;
};

struct _fs_metadata {
    int size;
    int length;
    char *uri;
    struct m_entry *entries;
};

static void parse_stmt(void *user_data, const raptor_statement *statement)
{
    fs_metadata *m = (fs_metadata *)user_data;

    char *pred = (char *) raptor_uri_as_string((raptor_uri *)
                                               statement->predicate);
    char *obj;
    if (statement->object_type == RAPTOR_IDENTIFIER_TYPE_LITERAL ||
        statement->object_type == RAPTOR_IDENTIFIER_TYPE_XML_LITERAL) {
        obj = (char *)statement->object;
    } else {
        obj = (char *)raptor_uri_as_string((raptor_uri *)
                                            statement->object);
    }

    fs_metadata_add(m, pred, obj);
}

fs_metadata *fs_metadata_open(const char *kb)
{
    fs_metadata *m = calloc(1, sizeof(fs_metadata));
    m->size = 16;
    m->length = 0;
    m->entries = calloc(m->size, sizeof(struct m_entry));
    m->uri = g_strdup_printf("file://"FS_MD_FILE, kb);

    int fd;
    if ((fd = open(m->uri + 7, FS_O_NOATIME | O_CREAT, FS_FILE_MODE)) == -1) {
        fs_error(LOG_CRIT, "failed to touch metadata file %s: %s",
            m->uri, strerror(errno));

        return NULL;
    }
    close(fd);
    raptor_init();
    raptor_parser *rdf_parser = raptor_new_parser("turtle");
    raptor_set_statement_handler(rdf_parser, m, parse_stmt);
    char *uri = strdup(m->uri);
    raptor_uri *ruri = raptor_new_uri((unsigned char *) uri);
    raptor_uri *muri = raptor_new_uri((unsigned char *) uri);
    free(uri);
    if (raptor_parse_uri(rdf_parser, ruri, muri)) {
        fs_error(LOG_ERR, "failed to parse metadata file “%s”", m->uri);

        return NULL;
    }
    raptor_free_parser(rdf_parser);
    raptor_free_uri(ruri);
    raptor_free_uri(muri);

    return m;
}

int fs_metadata_clear(fs_metadata *m)
{
    for (int e=0; e < m->length; e++) {
        g_free(m->entries[e].key);
        m->entries[e].key = NULL;
        g_free(m->entries[e].val);
        m->entries[e].val = NULL;
    }

    m->length = 0;

    return 0; 
}

int fs_metadata_set(fs_metadata *m, const char *key, const char *val)
{
    for (int e=0; e < m->length; e++) {
        if (!strcmp(m->entries[e].key, key)) {
            g_free(m->entries[e].val);
            m->entries[e].val = g_strdup(val);

            return 0;
        }
    }

    return fs_metadata_add(m, key, val);
}

int fs_metadata_set_int(fs_metadata *m, const char *key, long long int val)
{
    for (int e=0; e < m->length; e++) {
        if (!strcmp(m->entries[e].key, key)) {
            g_free(m->entries[e].val);
            m->entries[e].val = g_strdup_printf("%lld", val);

            return 0;
        }
    }

    return fs_metadata_add_int(m, key, val);
}

int fs_metadata_add(fs_metadata *m, const char *key, const char *val)
{
    if (m->length >= m->size-1) {
        m->size *= 2;
        m->entries = realloc(m->entries, m->size * sizeof(struct m_entry));
        memset(m->entries + m->size / 2, 0, m->size * sizeof(struct m_entry) / 2);
    }

    m->entries[m->length].key = g_strdup(key);
    m->entries[m->length].val = g_strdup(val);

    m->length++;

    return 0;
}

int fs_metadata_add_int(fs_metadata *m, const char *key, long long int val)
{
    if (m->length >= m->size-1) {
        m->size *= 2;
        m->entries = realloc(m->entries, m->size * sizeof(struct m_entry));
        memset(m->entries + m->size / 2, 0, m->size * sizeof(struct m_entry) / 2);
    }

    m->entries[m->length].key = g_strdup(key);
    m->entries[m->length].val = g_strdup_printf("%lld", val);

    m->length++;

    return 0;
}

const char *fs_metadata_get_string(fs_metadata *m, const char *prop,
                                   const char *def)
{
    for (int e=0; e < m->length; e++) {
        if (!strcmp(m->entries[e].key, prop)) {
            return m->entries[e].val;
        }
    }

    return def;
}

long long int fs_metadata_get_int(fs_metadata *m, const char *prop, long long int def)
{
    for (int e=0; e < m->length; e++) {
        if (!strcmp(m->entries[e].key, prop)) {
            return atoll(m->entries[e].val);
        }
    }

    return def;
}

fs_rid_vector *fs_metadata_get_int_vector(fs_metadata *m, const char *prop)
{
    fs_rid_vector *rv = fs_rid_vector_new(0);

    for (int e=0; e < m->length; e++) {
        if (!strcmp(m->entries[e].key, prop)) {
            fs_rid_vector_append(rv, atoll(m->entries[e].val));
        }
    }

    return rv;
}

int fs_metadata_get_bool(fs_metadata *m, const char *prop, int def)
{
    for (int e=0; e < m->length; e++) {
        if (!strcmp(m->entries[e].key, prop)) {
            return !strcmp(m->entries[e].val, "true");
        }
    }

    return def;
}

int fs_metadata_flush(fs_metadata *m)
{
    raptor_serializer *ser = raptor_new_serializer("turtle");
    if (!ser) {
        fs_error(LOG_CRIT, "cannot create turtle serialiser for metadata");

        return 1;
    }
    raptor_serialize_start_to_filename(ser, m->uri+7);

    raptor_statement st;
    st.subject_type = RAPTOR_IDENTIFIER_TYPE_RESOURCE;
    st.predicate_type = RAPTOR_IDENTIFIER_TYPE_RESOURCE;
    st.object_type = RAPTOR_IDENTIFIER_TYPE_LITERAL;
    st.object_literal_datatype = NULL;
    st.object_literal_language = NULL;

    for (int e=0; e < m->length; e++) {
        st.subject = "";
        st.predicate = m->entries[e].key;
        st.object = m->entries[e].val;
        raptor_serialize_statement(ser, &st);
    }

    raptor_serialize_end(ser);
    raptor_free_serializer(ser);

    return 0;
}

void fs_metadata_close(fs_metadata *m)
{
    if (!m) return;

    g_free(m->uri);
    for (int e=0; e < m->length; e++) {
        g_free(m->entries[e].key);
        g_free(m->entries[e].val);
    }
    free(m->entries);
    free(m);
}

/* vi:set expandtab sts=4 sw=4: */
