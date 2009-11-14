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

#include "common/4store.h"
#include "common/hash.h"
#include "common/error.h"
#include "common/params.h"
#include "common/md5.h"
#include "backend.h"
#include "backend-intl.h"
#include "query-backend.h"
#include "import-backend.h"
#include "disk-space.h"

#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <errno.h>
#include <glib.h>
#include <fcntl.h>
#include <getopt.h>
#include <sys/file.h>
#include <sys/types.h>
#include <sys/stat.h>

#include "lock.h"

#define PAD " "

//static const char feature_string[] = PAD "no-o-index freq" PAD;
static const char feature_string[] = PAD "no-o-index" PAD;

static unsigned char *handle_insert_resource(fs_backend *be, fs_segment segment,
                                               unsigned int length,
                                               unsigned char *content)
{
  if (segment > be->segments) {
    fs_error(LOG_ERR, "invalid segment number: %d", segment);
  }

  if (length < sizeof(fs_rid) * 4) {
    fs_error(LOG_ERR, "insert_resource(%d) too short", segment);
    /* can't reply - semi-async */
  }

  int k, count;
  unsigned char *record = content + 8;

  memcpy(&count, content, sizeof (count));
  fs_resource *resources = calloc(count, sizeof(fs_resource));
  
  for (k = 0; k < count; ++k) {
    unsigned int offset;
    memcpy(&resources[k].rid, record, sizeof(fs_rid));
    memcpy(&resources[k].attr, record + 8, sizeof(fs_rid));
    memcpy(&offset, record + 16, sizeof(offset));
    resources[k].lex = (char *) record + 20;
    if (offset > length) {
      fs_error(LOG_ERR, "insert_resource(%d) recv'd invalid offset", segment);
      break;
    }
    record += offset;
    length -= offset;
  }

  fs_res_import(be, segment, count, resources);
  free(resources);

  return NULL; /* no reply - semi-async */
}

static unsigned char * handle_commit_resource (fs_backend *be, fs_segment segment,
                                               unsigned int length,
                                               unsigned char *content)
{
  if (segment > be->segments) {
    fs_error(LOG_ERR, "invalid segment number: %d", segment);
    return fsp_error_new(segment, "invalid segment number");
  }

  if (length > 0) {
    fs_error(LOG_ERR, "commit_resource(%d) extraneous content", segment);
    return fsp_error_new(segment, "extraneous content");
  }

  fs_res_import_commit(be, segment, 1);

  return message_new(FS_DONE_OK, segment, 0);
}


static unsigned char * handle_insert_quad (fs_backend *be, fs_segment segment,
                                             unsigned int length,
                                             unsigned char *content)
{
  if (segment > be->segments) {
    fs_error(LOG_ERR, "invalid segment number: %d", segment);
    return fsp_error_new(segment, "invalid segment number");
  }

  if (length < 40) {
    fs_error(LOG_ERR, "insert_quad(%d) too short", segment);
    return fsp_error_new(segment, "too short");
  }

  int flags;
  int count = (length - 8) / 32;
  fs_rid (*buffer)[4] = (fs_rid (*)[4]) (content + 8);

  memcpy(&flags, content, sizeof (flags));
  int ret = fs_quad_import(be, segment, flags, count, buffer);
  if (ret) {
    fs_error(LOG_ERR, "insert_quad(%d) failed", segment);
    return fsp_error_new(segment, "quad insert failed");
  }

  return NULL; /* no reply - semi-async */
}

static unsigned char * handle_commit_quad (fs_backend *be, fs_segment segment,
                                             unsigned int length,
                                             unsigned char *content)
{
  if (segment > be->segments) {
    fs_error(LOG_ERR, "invalid segment number: %d", segment);
    return fsp_error_new(segment, "invalid segment number");
  }

  if (length != sizeof(int)) {
    fs_error(LOG_ERR, "commit_quad(%d) missing flags", segment);
    return fsp_error_new(segment, "missing flags");
  }

  int flags;

  memcpy(&flags, content, sizeof (flags));
  int ret = fs_quad_import_commit(be, segment, flags, 1);
  if (ret) {
    fs_error(LOG_ERR, "commit_quad(%d) failed", segment);
    return fsp_error_new(segment, "quad commit failed");
  }

  return message_new(FS_DONE_OK, segment, 0);
}

static unsigned char * handle_price (fs_backend *be, fs_segment segment,
                                     unsigned int length,
                                     unsigned char *content)
{
  if (segment > be->segments) {
    fs_error(LOG_ERR, "invalid segment number: %d", segment);
    return fsp_error_new(segment, "invalid segment number");
  }

  if (length < 24) {
    fs_error(LOG_ERR, "price(%d) much too short", segment);
    return fsp_error_new(segment, "much too short");
  }

  unsigned char *reply;
  unsigned long long int rows = 0;
  fs_rid_vector models, subjects, predicates, objects;
  unsigned int flags, value;

  memcpy(&flags, content, sizeof (flags));
  memcpy(&value, content + 4, sizeof (models.length));
  models.size = models.length = value / 8;
  memcpy(&value, content + 8, sizeof (subjects.length));
  subjects.size = subjects.length = value / 8;
  memcpy(&value, content + 12, sizeof (predicates.length));
  predicates.size = predicates.length = value / 8;
  memcpy(&value, content + 16, sizeof (objects.length));
  objects.size = objects.length = value / 8;
  content += 24;

  if (length < (models.size + subjects.size + predicates.size + objects.size) * 8 + 24) {
    fs_error(LOG_ERR, "price(%d) too short", segment);
    return fsp_error_new(segment, "too short");
  }

  models.data = (fs_rid *) content;
  content += models.length * 8;

  subjects.data = (fs_rid *) content;
  content += subjects.length * 8;

  predicates.data = (fs_rid *) content;
  content += predicates.length * 8;

  objects.data = (fs_rid *) content;

  rows = fs_bind_price(be, segment, flags, &models, &subjects, &predicates, &objects);

  reply = message_new(FS_ESTIMATED_ROWS, segment, 8);
  memcpy(reply + FS_HEADER, &rows, sizeof(rows));
  return reply;
}

static unsigned char * handle_delete_models (fs_backend *be, fs_segment segment,
                                             unsigned int length,
                                             unsigned char *content)
{
  if (segment > be->segments) {
    fs_error(LOG_ERR, "invalid segment number: %d", segment);
    return fsp_error_new(segment, "invalid segment number");
  }

  if (length < sizeof(fs_rid)) {
    fs_error(LOG_ERR, "delete_models(%d) missing model RIDs", segment);
    return fsp_error_new(segment, "missing model RIDs");
  }

  fs_rid_vector models;
  models.size = models.length  = length / sizeof(fs_rid);
  models.data = (fs_rid *) content;

  fs_delete_models(be, segment, &models);

  return message_new(FS_DONE_OK, segment, 0);
}

static unsigned char * handle_new_models (fs_backend *be, fs_segment segment,
                                          unsigned int length,
                                          unsigned char *content)
{
  if (segment > be->segments) {
    fs_error(LOG_ERR, "invalid segment number: %d", segment);
    return fsp_error_new(segment, "invalid segment number");
  }

  if (length < sizeof(fs_rid)) {
    fs_error(LOG_ERR, "new_models(%d) missing model RIDs", segment);
    return fsp_error_new(segment, "missing model RIDs");
  }

  fs_rid *models = (fs_rid *) content;

  int invalid_count = 0;
  for (int k= 0; k < (length / sizeof(fs_rid)); ++k) {
    if (FS_IS_URI(models[k])) {
      fs_backend_model_set_usage(be, segment, models[k], 0);
    } else {
      invalid_count++;
    }
  }
  fs_mhash_flush(be->models);

  if (invalid_count > 0) {
    return fsp_error_new(segment, "one or more model RIDs is not a URI");
  }
  return message_new(FS_DONE_OK, segment, 0);
}

static unsigned char * handle_start_import (fs_backend *be, fs_segment segment,
                                            unsigned int length,
                                            unsigned char *content)
{
  if (segment > be->segments) {
    fs_error(LOG_ERR, "invalid segment number: %d", segment);
    return fsp_error_new(segment, "invalid segment number");
  }

  if (length > 0) {
    fs_error(LOG_ERR, "start_import(%d) extraneous content", segment);
    return fsp_error_new(segment, "extraneous content");
  }

  float free_space = fs_free_disk(fs_backend_get_kb(be));
  if (free_space < be->min_free) {
    fs_error(LOG_ERR, "segment %d only has %.1f%% free space", segment, free_space);
    return fsp_error_new(segment, "low disk space");
  }

  fs_start_import(be, segment);

  return message_new(FS_DONE_OK, segment, 0);
}

static unsigned char * handle_stop_import (fs_backend *be, fs_segment segment,
                                           unsigned int length,
                                           unsigned char *content)
{
  if (segment > be->segments) {
    fs_error(LOG_ERR, "invalid segment number: %d", segment);
    return fsp_error_new(segment, "invalid segment number");
  }

  if (length > 0) {
    fs_error(LOG_ERR, "stop_import(%d) extraneous content", segment);
    return fsp_error_new(segment, "extraneous content");
  }

  int ret = fs_stop_import(be, segment);

  if (ret) {
    return fsp_error_new(segment, "insert failed");
  }

  return message_new(FS_DONE_OK, segment, 0);
}

static unsigned char * handle_delete_quads (fs_backend *be, fs_segment segment,
                                          unsigned int length,
                                          unsigned char *content)
{
  if (segment > be->segments) {
    fs_error(LOG_ERR, "invalid segment number: %d", segment);
    return fsp_error_new(segment, "invalid segment number");
  }

  if (length < 32) {
    fs_error(LOG_ERR, "bind_limit(%d) much too short", segment);
    return fsp_error_new(segment, "much too short");
  }

  fs_rid_vector models, subjects, predicates, objects;
  unsigned int value;

  memcpy(&value, content, sizeof (models.length));
  models.size = models.length = value / 8;
  subjects.size = subjects.length = value / 8;
  predicates.size = predicates.length = value / 8;
  objects.size = objects.length = value / 8;
  content += 4;

  if (length < (models.size + subjects.size + predicates.size + objects.size) * 8 + 4) {
    fs_error(LOG_ERR, "bind_limit(%d) too short", segment);
    return fsp_error_new(segment, "too short");
  }

  models.data = (fs_rid *) content;
  content += models.length * 8;

  subjects.data = (fs_rid *) content;
  content += subjects.length * 8;

  predicates.data = (fs_rid *) content;
  content += predicates.length * 8;

  objects.data = (fs_rid *) content;

  fs_rid_vector *args[4] = { &models, &subjects, &predicates, &objects };
  fs_delete_quads(be, args);
  /* FIXME, should check return value */

  return message_new(FS_DONE_OK, 0, 0);
}

static unsigned char * handle_get_size_reverse (fs_backend *be, fs_segment segment,
                                             unsigned int length,
                                             unsigned char *content)
{
  if (segment > be->segments) {
    fs_error(LOG_ERR, "invalid segment number: %d", segment);
    return fsp_error_new(segment, "invalid segment number");
  }

  if (length > 0) {
    fs_error(LOG_ERR, "get_data_size(%d) extraneous content", segment);
    return fsp_error_new(segment, "extraneous content");
  }

  fs_data_size size = fs_get_data_size(be, segment);
  unsigned char *reply =  message_new(FS_SIZE_REVERSE, segment, sizeof(size.quads_sr));

  memcpy(reply + FS_HEADER, &size.quads_sr, sizeof(size.quads_sr));
  return reply;
}

static unsigned char * handle_get_quad_freq (fs_backend *be, fs_segment segment,
                                             unsigned int length,
                                             unsigned char *content)
{
  if (segment > be->segments) {
    fs_error(LOG_ERR, "invalid segment number: %d", segment);
    return fsp_error_new(segment, "invalid segment number");
  }

  if (length != 8) {
    fs_error(LOG_ERR, "get_quad_freq(%d) wrong length %u", segment, length);
    return fsp_error_new(segment, "wrong length");
  }

  int index, count;
  memcpy(&index, content, sizeof(int));
  memcpy(&count, content + sizeof(int), sizeof(int));

  const char *filename;

  /* this branch has no frequency data */
  filename = "";
  int fd = open(filename, FS_O_NOATIME | O_RDONLY, 0);
  unsigned char *reply =  message_new(FS_QUAD_FREQ, segment, count * sizeof(fs_quad_freq));
  //ssize_t bytes = read(fd, reply + FS_HEADER, count * sizeof(fs_quad_freq));
  ssize_t bytes = 0;

  unsigned int *l = (unsigned int *) (reply + 4);
  *l = bytes;
  close(fd);

  return reply;
}

static unsigned char * handle_choose_segment (fs_backend *be, fs_segment segment,
                                              unsigned int length,
                                              unsigned char *content)
{
  if (fs_backend_open_files(be, segment, O_RDWR | O_CREAT, FS_OPEN_ALL)) {
    fs_error(LOG_ERR, "failed to open files for segment %d", segment);

    return fsp_error_new(segment, "cannot open indexes");
  }

  return message_new(FS_DONE_OK, 0, 0);
}

static unsigned char * handle_get_data_size (fs_backend *be, fs_segment segment,
                                             unsigned int length,
                                             unsigned char *content)
{
  if (segment > be->segments) {
    fs_error(LOG_ERR, "invalid segment number: %d", segment);
    return fsp_error_new(segment, "invalid segment number");
  }

  if (length > 0) {
    fs_error(LOG_ERR, "get_data_size(%d) extraneous content", segment);
    return fsp_error_new(segment, "extraneous content");
  }

  fs_data_size size = fs_get_data_size(be, segment);

  fs_old_data_size old_size;
  old_size.quads_s = size.quads_s;
  old_size.quads_o = size.quads_o;
  old_size.resources = size.resources;
  old_size.models_s = size.models_s;
  old_size.models_o = size.models_o;
  unsigned char *reply =  message_new(FS_SIZE, segment, sizeof(old_size));

  memcpy(reply + FS_HEADER, &old_size, sizeof(old_size));
  return reply;
}

static unsigned char * handle_get_import_times (fs_backend *be,
                                                fs_segment segment,
                                                unsigned int length,
                                                unsigned char *content)
{
  if (segment > be->segments) {
    fs_error(LOG_ERR, "invalid segment number: %d", segment);
    return fsp_error_new(segment, "invalid segment number");
  }

  if (length > 0) {
    fs_error(LOG_ERR, "get_import_times(%d) extraneous content", segment);
    return fsp_error_new(segment, "extraneous content");
  }

  fs_import_timing timing = fs_get_import_times(be, segment);
  unsigned char *reply =  message_new(FS_IMPORT_TIMES, segment, sizeof(timing));

  memcpy(reply + FS_HEADER, &timing, sizeof(timing));
  return reply;
}

static unsigned char * handle_segments (fs_backend *be,
                                        fs_segment segment,
                                        unsigned int length,
                                        unsigned char *content)
{
  if (segment != 0) {
    fs_error(LOG_ERR, "segments(%d) should only be sent with segment = 0", segment);
    return fsp_error_new(segment, "only to be sent to segment = 0");
  }

  if (length > 0) {
    fs_error(LOG_ERR, "segments(%d) extraneous content", segment);
    return fsp_error_new(segment, "extraneous content");
  }

  int segments[be->segments];
  int count = fs_segments(be, segments);
  unsigned char *reply =  message_new(FS_SEGMENT_LIST, be->segments, sizeof(int) * count);

  memcpy(reply + FS_HEADER, segments, sizeof(int) * count);
  return reply;
}

static unsigned char * handle_node_segments (fs_backend *be,
                                             fs_segment segment,
                                             unsigned int length,
                                             unsigned char *content)
{
  if (segment != 0) {
    fs_error(LOG_ERR, "segments(%d) should only be sent with segment = 0", segment);
    return fsp_error_new(segment, "only to be sent to segment = 0");
  }

  if (length > 0) {
    fs_error(LOG_ERR, "segments(%d) extraneous content", segment);
    return fsp_error_new(segment, "extraneous content");
  }

  char segments[be->segments];
  fs_node_segments(be, segments);
  unsigned char *reply =  message_new(FS_NODE_SEGMENT_LIST, be->segments, be->segments);

  memcpy(reply + FS_HEADER, segments, be->segments);
  return reply;
}

static unsigned char * handle_get_query_times (fs_backend *be,
                                                fs_segment segment,
                                                unsigned int length,
                                                unsigned char *content)
{
  if (segment > be->segments) {
    fs_error(LOG_ERR, "invalid segment number: %d", segment);
    return fsp_error_new(segment, "invalid segment number");
  }

  if (length > 0) {
    fs_error(LOG_ERR, "get_query_times(%d) extraneous content", segment);
    return fsp_error_new(segment, "extraneous content");
  }

  fs_query_timing timing = fs_get_query_times(be, segment);
  unsigned char *reply =  message_new(FS_QUERY_TIMES, segment, sizeof(timing));

  memcpy(reply + FS_HEADER, &timing, sizeof(timing));
  return reply;
}

static unsigned char * handle_bind_limit (fs_backend *be, fs_segment segment,
                                          unsigned int length,
                                          unsigned char *content)
{
  unsigned char *reply;

  if (segment > be->segments) {
    fs_error(LOG_ERR, "invalid segment number: %d", segment);
    return fsp_error_new(segment, "invalid segment number");
  }

  if (length < 32) {
    fs_error(LOG_ERR, "bind_limit(%d) much too short", segment);
    return fsp_error_new(segment, "much too short");
  }

  fs_rid_vector models, subjects, predicates, objects;
  unsigned int flags, value;
  int offset, limit;

  memcpy(&flags, content, sizeof (flags));
  memcpy(&offset, content + 4, sizeof (offset));
  memcpy(&limit, content + 8, sizeof (limit));

  memcpy(&value, content + 12, sizeof (models.length));
  models.size = models.length = value / 8;
  memcpy(&value, content + 16, sizeof (subjects.length));
  subjects.size = subjects.length = value / 8;
  memcpy(&value, content + 20, sizeof (predicates.length));
  predicates.size = predicates.length = value / 8;
  memcpy(&value, content + 24, sizeof (objects.length));
  objects.size = objects.length = value / 8;
  content += 32;

  if (length < (models.size + subjects.size + predicates.size + objects.size) * 8 + 32) {
    fs_error(LOG_ERR, "bind_limit(%d) too short", segment);
    return fsp_error_new(segment, "too short");
  }

  models.data = (fs_rid *) content;
  content += models.length * 8;

  subjects.data = (fs_rid *) content;
  content += subjects.length * 8;

  predicates.data = (fs_rid *) content;
  content += predicates.length * 8;

  objects.data = (fs_rid *) content;

  fs_rid_vector **bindings;

  bindings = fs_bind(be, segment, flags,
                     &models, &subjects, &predicates, &objects, offset, limit);

  int k, cols = 0;
  for (k = 0; k < 4; ++k) {
    if (flags & 1 << k) cols++;
  }

  if (bindings == NULL) {
    /* NULL => no match */
    reply = message_new(FS_NO_MATCH, segment, 0);
    cols = 0;
  } else if (cols == 0) {
    /* Zero columns => match with no binding */
    reply = message_new(FS_BIND_LIST, segment, 0);
  } else {
    /* otherwise return bindings */
    reply = message_new(FS_BIND_LIST, segment, bindings[0]->length * 8 * cols);
    unsigned char *data = reply + FS_HEADER;

    for (k= 0; k < cols; ++k) {
      memcpy(data, bindings[k]->data, bindings[k]->length * 8);
      data += bindings[k]->length * 8;
    }
  }

  for (k = 0; k < cols; ++k) {
    fs_rid_vector_free(bindings[k]);
  }
  free(bindings);

  return reply;
}

static unsigned char * handle_reverse_bind (fs_backend *be, fs_segment segment,
                                            unsigned int length,
                                            unsigned char *content)
{
  unsigned char *reply;

  if (segment > be->segments) {
    fs_error(LOG_ERR, "invalid segment number: %d", segment);
    return fsp_error_new(segment, "invalid segment number");
  }

  if (length < 32) {
    fs_error(LOG_ERR, "reverse_bind(%d) much too short", segment);
    return fsp_error_new(segment, "much too short");
  }

  fs_rid_vector models, subjects, predicates, objects;
  unsigned int flags, value;
  int offset, limit;

  memcpy(&flags, content, sizeof (flags));
  memcpy(&offset, content + 4, sizeof (offset));
  memcpy(&limit, content + 8, sizeof (limit));

  memcpy(&value, content + 12, sizeof (models.length));
  models.size = models.length = value / 8;
  memcpy(&value, content + 16, sizeof (subjects.length));
  subjects.size = subjects.length = value / 8;
  memcpy(&value, content + 20, sizeof (predicates.length));
  predicates.size = predicates.length = value / 8;
  memcpy(&value, content + 24, sizeof (objects.length));
  objects.size = objects.length = value / 8;
  content += 32;

  if (length < (models.size + subjects.size + predicates.size + objects.size) * 8 + 32) {
    fs_error(LOG_ERR, "bind_limit(%d) too short", segment);
    return fsp_error_new(segment, "too short");
  }

  models.data = (fs_rid *) content;
  content += models.length * 8;

  subjects.data = (fs_rid *) content;
  content += subjects.length * 8;

  predicates.data = (fs_rid *) content;
  content += predicates.length * 8;

  objects.data = (fs_rid *) content;

  fs_rid_vector **bindings;
  bindings = fs_reverse_bind(be, segment, flags, &models, &subjects, &predicates, &objects, offset, limit);

  int k, cols = 0;
  for (k = 0; k < 4; ++k) {
    if (flags & 1 << k) cols++;
  }

  if (bindings == NULL) {
    /* NULL => no match */
    reply = message_new(FS_NO_MATCH, segment, 0);
    cols = 0;
  } else if (cols == 0) {
    /* Zero columns => match with no binding */
    reply = message_new(FS_BIND_LIST, segment, 0);
  } else if (!bindings[0]) {
    /* this probably shouldn't happen, but it does */
    reply = message_new(FS_NO_MATCH, segment, 0);
    cols = 0;
  } else {
    /* otherwise return bindings */
    reply = message_new(FS_BIND_LIST, segment, bindings[0]->length * 8 * cols);
    unsigned char *data = reply + FS_HEADER;

    for (k= 0; k < cols; ++k) {
      memcpy(data, bindings[k]->data, bindings[k]->length * 8);
      data += bindings[k]->length * 8;
    }
  }

  for (k = 0; k < cols; ++k) {
    fs_rid_vector_free(bindings[k]);
  }
  free(bindings);

  return reply;
}

static unsigned char * handle_bind_first (fs_backend *be, fs_segment segment,
                                          unsigned int length,
                                          unsigned char *content)
{
  unsigned char *reply;

  if (segment > be->segments) {
    fs_error(LOG_ERR, "invalid segment number: %d", segment);
    return fsp_error_new(segment, "invalid segment number");
  }

  if (length < 32) {
    fs_error(LOG_ERR, "bind_first(%d) much too short", segment);
    return fsp_error_new(segment, "much too short");
  }

  fs_rid_vector models, subjects, predicates, objects;
  unsigned int flags, value;
  int count;

  memcpy(&flags, content, sizeof (flags));
  memcpy(&count, content + 4, sizeof (count));
  memcpy(&value, content + 12, sizeof (models.length));
  models.size = models.length = value / 8;
  memcpy(&value, content + 16, sizeof (subjects.length));
  subjects.size = subjects.length = value / 8;
  memcpy(&value, content + 20, sizeof (predicates.length));
  predicates.size = predicates.length = value / 8;
  memcpy(&value, content + 24, sizeof (objects.length));
  objects.size = objects.length = value / 8;
  content += 32;

  if (length < (models.size + subjects.size + predicates.size + objects.size) * 8 + 32) {
    fs_error(LOG_ERR, "bind_first(%d) too short", segment);
    return fsp_error_new(segment, "too short");
  }

  models.data = (fs_rid *) content;
  content += models.length * 8;

  subjects.data = (fs_rid *) content;
  content += subjects.length * 8;

  predicates.data = (fs_rid *) content;
  content += predicates.length * 8;

  objects.data = (fs_rid *) content;

  fs_rid_vector **bindings;
  bindings = fs_bind_first(be, segment, flags, &models, &subjects,
                           &predicates, &objects, count);

  int k, cols = 0;
  for (k = 0; k < 4; ++k) {
    if (flags & 1 << k) cols++;
  }

  if (bindings == NULL) {
    /* NULL => no match */
    reply = message_new(FS_NO_MATCH, segment, 0);
    cols = 0;
  } else if (cols == 0) {
    /* Zero columns => match with no binding */
    reply = message_new(FS_BIND_LIST, segment, 0);
  } else {
    /* otherwise return bindings */
    reply = message_new(FS_BIND_LIST, segment, bindings[0]->length * 8 * cols);
    unsigned char *data = reply + FS_HEADER;

    for (k= 0; k < cols; ++k) {
      memcpy(data, bindings[k]->data, bindings[k]->length * 8);
      data += bindings[k]->length * 8;
    }
  }

  for (k = 0; k < cols; ++k) {
    fs_rid_vector_free(bindings[k]);
  }
  free(bindings);

  return reply;
}

static unsigned char * handle_bind_next (fs_backend *be, fs_segment segment,
                                          unsigned int length,
                                          unsigned char *content)
{
  unsigned char *reply;

  if (segment > be->segments) {
    fs_error(LOG_ERR, "invalid segment number: %d", segment);
    return fsp_error_new(segment, "invalid segment number");
  }

  if (length != 8) {
    fs_error(LOG_ERR, "bind_next(%d) wrong length %u", segment, length);
    return fsp_error_new(segment, "wrong length");
  }

  unsigned int flags;
  int count;

  memcpy(&flags, content, sizeof (flags));
  memcpy(&count, content + 4, sizeof (count));

  fs_rid_vector **bindings;
  bindings = fs_bind_next(be, segment, flags, count);

  int k, cols = 0;
  for (k = 0; k < 4; ++k) {
    if (flags & 1 << k) cols++;
  }

  if (bindings == NULL) {
    /* NULL => no match */
    reply = message_new(FS_NO_MATCH, segment, 0);
    cols = 0;
  } else if (cols == 0) {
    /* Zero columns => match with no binding */
    reply = message_new(FS_BIND_LIST, segment, 0);
  } else {
    /* otherwise return bindings */
    reply = message_new(FS_BIND_LIST, segment, bindings[0]->length * 8 * cols);
    unsigned char *data = reply + FS_HEADER;

    for (k= 0; k < cols; ++k) {
      memcpy(data, bindings[k]->data, bindings[k]->length * 8);
      data += bindings[k]->length * 8;
    }
  }

  for (k = 0; k < cols; ++k) {
    fs_rid_vector_free(bindings[k]);
  }
  free(bindings);

  return reply;
}

static unsigned char * handle_bind_done (fs_backend *be, fs_segment segment,
                                           unsigned int length,
                                           unsigned char *content)
{
  if (segment > be->segments) {
    fs_error(LOG_ERR, "invalid segment number: %d", segment);
    return fsp_error_new(segment, "invalid segment number");
  }

  if (length > 0) {
    fs_error(LOG_ERR, "bind_done(%d) extraneous content", segment);
    return fsp_error_new(segment, "extraneous content");
  }

  fs_bind_done(be, segment);

  return message_new(FS_DONE_OK, segment, 0);
}

static unsigned char * handle_resolve_attr (fs_backend *be, fs_segment segment,
                                            unsigned int length,
                                            unsigned char *content)
{
  unsigned int count = length / sizeof(fs_rid);

  if (segment > be->segments) {
    fs_error(LOG_ERR, "invalid segment number: %d", segment);
    return fsp_error_new(segment, "invalid segment number");
  }

  if (length < sizeof(fs_rid)) {
    fs_error(LOG_ERR, "resolve_attr(%d) too short", segment);
    return fsp_error_new(segment, "too short");
  }

  unsigned char *reply;

  fs_rid_vector v;
  fs_resource resources[count];
  v.size = v.length = count; 
  v.data = (fs_rid *) content;

  fs_resolve(be, segment, &v, resources);

  unsigned int k, serial_length = 0;
  for (k = 0; k < count; ++k) {
    if (resources[k].lex) {
      serial_length+= ((28 + strlen(resources[k].lex)) / 8);
    } else {
      serial_length+= 3;
    }
  }

  reply = message_new(FS_RESOURCE_ATTR_LIST, segment, 8 * serial_length);
  unsigned char *record = reply + FS_HEADER;

  for (k = 0; k < count; ++k) {
    unsigned int one_length;
    if (resources[k].lex) {
      one_length = ((28 + strlen(resources[k].lex)) / 8) * 8;
    } else {
      one_length = 24;
    }
    memcpy(record, &(resources[k].rid), sizeof (fs_rid));
    memcpy(record + 8, &(resources[k].attr), sizeof (fs_rid));
    memcpy(record + 16, &one_length, sizeof(one_length));

/* ASCII NUL is used to terminate strings on the wire */
    if (resources[k].lex) {
      strcpy((char *) record + 20, resources[k].lex);
    } else {
      *(record + 20) = '\0';
    }
    record += one_length;
  }

  return reply;
}

static unsigned char * handle_bnode_alloc (fs_backend *be,
                                           fs_segment segment,
                                           unsigned int length,
                                           unsigned char *content)
{
  if (segment != 0) {
    fs_error(LOG_ERR, "bnode_alloc(%d) should only be sent to segment zero", segment);
    return fsp_error_new(segment, "only to be sent to segment zero");
  }

  if (length != sizeof(int)) {
    fs_error(LOG_ERR, "bnode_alloc(%d) missing count", segment);
    return fsp_error_new(segment, "missing count");
  }

  int count;

  memcpy(&count, content, sizeof(count));
  unsigned char *reply = message_new(FS_BNODE_RANGE, 0, 16);
  unsigned char *range = reply + FS_HEADER;
  fs_bnode_alloc(be, count, (fs_rid *) range, (fs_rid *) (range + sizeof(fs_rid)));
  
  return reply;
}

static unsigned char * handle_auth (fs_backend *be,
                                    fs_segment segment,
                                    unsigned int length,
                                    unsigned char *content)
{
  md5_state_t md5;

  if (length <= 16) {
    fs_error(LOG_ERR, "auth(%d) missing kbname", segment);
    return fsp_error_new(segment, "missing kbname");
  }

  if (strncmp(be->db_name, (char *) content + 16, length - 16)) {
    fs_error(LOG_ERR, "auth(%d) wrong kbname", segment);
    return fsp_error_new(segment, "wrong kbname");
  }

  if (be->salt) {
    unsigned char data[20], hash[16];
    char string[33];
    memcpy(data, (&be->salt), sizeof(be->salt));

    memcpy(data + 4, content, 16);
    md5_init(&md5);
    md5_append(&md5, data, sizeof(data));
    md5_finish(&md5, hash);
    sprintf(string, "%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x",
            hash[0], hash[1], hash[2], hash[3], hash[4], hash[5], hash[6], hash[7],
            hash[8], hash[9], hash[10], hash[11], hash[12], hash[13], hash[14], hash[15]);

    if (strcmp(string, be->hash)) {
      fs_error(LOG_ERR, "auth(%d) invalid password", segment);
      return fsp_error_new(segment, "invalid password");
    }
  }
  
  /* in-band signal, string of features */
  unsigned char *reply = message_new(FS_DONE_OK, 0, sizeof(feature_string));
  memcpy(reply + FS_HEADER, feature_string, sizeof(feature_string));
  return reply;
}

static unsigned char * handle_transaction (fs_backend *be,
                                    fs_segment segment,
                                    unsigned int length,
                                    unsigned char *content)
{
  if (length != 1) {
    fs_error(LOG_ERR, "transaction(%d), missing flag", segment);
    return fsp_error_new(segment, "missing flag");
  }

  int ret = fs_backend_transaction(be, segment, *content);

  if (ret == 2) {
    if (*content == FS_TRANS_BEGIN) {
      return fsp_error_new(segment, "transaction already begun");
   } else {
      return fsp_error_new(segment, "transaction not begun");
   }
  } else if (ret) {
    return fsp_error_new(segment, "transaction error");
  }

  return message_new(FS_DONE_OK, 0, 0);
}

static unsigned char * handle_lock (fs_backend *be,
                                    fs_segment segment,
                                    unsigned int length,
                                    unsigned char *content)
{
  return fsp_error_new(segment, "radix backend does not support/require locking");
}

static unsigned char * handle_unlock (fs_backend *be,
                                      fs_segment segment,
                                      unsigned int length,
                                      unsigned char *content)
{
  /* this is currently a NOOP, the COMMIT phase closes the lock implicitly, so
   * theres no need to here */

  return message_new(FS_DONE_OK, 0, 0);
}

static int segment_count (fs_backend *be)
{
  return be->segments;
}

fsp_backend native_backend = {
  .price = handle_price,
  .insert_resource = handle_insert_resource,
  .commit_resource = handle_commit_resource,
  .delete_models = handle_delete_models,
  .delete_quads = handle_delete_quads,
  .new_models = handle_new_models,
  .start_import = handle_start_import,
  .stop_import = handle_stop_import,
  .get_data_size = handle_get_data_size,
  .get_import_times = handle_get_import_times,
  .insert_quad = handle_insert_quad,
  .commit_quad = handle_commit_quad,
  .segments = handle_segments,
  .get_query_times = handle_get_query_times,
  .bind_limit = handle_bind_limit,
  .resolve_attr = handle_resolve_attr,
  .bnode_alloc = handle_bnode_alloc,
  .auth = handle_auth,
  .bind_first = handle_bind_first,
  .bind_next = handle_bind_next,
  .bind_done = handle_bind_done,
  .open = fs_backend_init,
  .close = fs_backend_fini,
  .segment_count = segment_count,
  .transaction = handle_transaction,
  .node_segments = handle_node_segments,
  .reverse_bind = handle_reverse_bind,
  .lock = handle_lock,
  .unlock = handle_unlock,
  .get_size_reverse = handle_get_size_reverse,
  .get_quad_freq = handle_get_quad_freq,
  .choose_segment = handle_choose_segment,
};


int main (int argc, char *argv[])
{
  char *kb_name;
  int daemon = 1;
  int help = 0;
  float disk_limit = 1.0;

  fsp_syslog_enable();

  int c, opt_index=0;
  static const char *optstr = "Dl:";
  static struct option longopt[] = {
    { "daemon", 0, 0, 'D' },
    { "limit", 1, 0, 'l' },
    { 0, 0, 0, 0 }
  };

  if (getenv("FS_DISK_LIMIT")) {
    disk_limit = atof(getenv("FS_DISK_LIMIT"));
  } else if (getenv("DISK_LIMIT")) {
    disk_limit = atof(getenv("DISK_LIMIT"));
  }

  while (( c = getopt_long(argc, argv, optstr, longopt, &opt_index)) != -1) {
    switch (c) {
    case 'D':
      daemon = 0;
      break;
    case 'l':
      disk_limit = atof(optarg);
      break;
    default:
      help = 1;
      break;
    }
  }

  if (optind != argc - 1) {
    help = 1;
  }

  if (help) {
    fprintf(stderr, "%s revision %s\n", argv[0], FS_BACKEND_VER);
    fprintf(stderr, "Usage: %s [-D,--deamon] [-l,--limit min-free-space] <kbname>\n", argv[0]);
    fprintf(stderr, "       env. var. FS_DISK_LIMIT also controls min free disk\n");
    return 1;
  }

  kb_name = argv[argc - 1];

  if (fs_lock_kb(kb_name)) {
    return 1;
  }

  fsp_serve(kb_name, &native_backend, daemon, disk_limit);

  return 2; /* fsp_serve returns only if there is an error */
}
