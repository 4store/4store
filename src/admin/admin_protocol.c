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

#include <string.h>
#include <stdlib.h>
#include <stdint.h>
#include <errno.h>

#include "admin_protocol.h"

/* returns empty packet with headers set */
static unsigned char *init_packet(int cmd, int len)
{
    /* constant header bytes */
    static char* header = "AC";
    static uint8_t vers = (uint8_t)ADM_PROTO_VERS;

    int n;         /* bytes to write */
    int total = 0; /* total bytes written */

    uint8_t adm_cmd = (uint8_t)cmd;    /* command as 1 byte */
    uint16_t data_len = (uint16_t)len; /* length of data as 2 bytes */

    int buf_len = ADM_HEADER_LEN + len;
    unsigned char *buf = (unsigned char *)malloc(buf_len);
    if (buf == NULL) {
        errno = ENOMEM;
        return NULL;
    }

    unsigned char *p = buf; /* current location in buffer */

    n = ADM_H_LEN;
    memcpy(p, header, n);
    p += n;
    total += n;

    n = ADM_H_VERS_LEN;
    memcpy(p, &vers, n);
    p += n;
    total += n;

    n = ADM_H_CMD_LEN;
    memcpy(p, &adm_cmd, n);
    p += n;
    total += n;

    n = ADM_H_DL_LEN;
    memcpy(p, &data_len, n);
    p += n;
    total += n;

    return buf;
}

static unsigned char *fsap_encode_cmd_no_params(int cmd)
{
    return init_packet(cmd, 0);
}

int fsap_decode_header(const unsigned char *buf, uint8_t *cmd, uint16_t *size)
{
    const unsigned char *p = buf;

    /* check that header starts with "AC" */
    if (p[0] != 'A' || p[1] != 'C') {
        errno = ADM_ERR_PROTO;
        return -1;
    }
    p += ADM_H_LEN;

    /* check that matching protocol versions are used */
    if (*p != ADM_PROTO_VERS) {
        errno = ADM_ERR_PROTO_VERS;
        return -1;
    }
    p += ADM_H_VERS_LEN;

    memcpy(cmd, p, ADM_H_CMD_LEN);
    p += ADM_H_CMD_LEN;

    memcpy(size, p, ADM_H_DL_LEN);
    p += ADM_H_DL_LEN;

    return 0;
}

unsigned char *fsap_encode_cmd_get_kb_port(const char *kb_name, int *len)
{
    int data_len = strlen(kb_name);
    unsigned char *buf = init_packet(ADM_CMD_GET_KB_PORT, data_len);
    unsigned char *p = buf;
    p += ADM_HEADER_LEN; /* move to start of data section */

    memcpy(p, kb_name, data_len);
    *len = data_len + ADM_HEADER_LEN;
    return buf;
}

unsigned char *fsap_encode_rsp_get_kb_port(int port)
{
    uint16_t port_num = (uint16_t)port;
    int data_len = sizeof(uint16_t);
    unsigned char *buf = init_packet(ADM_RSP_GET_KB_PORT, data_len);
    unsigned char *p = buf;
    p += ADM_HEADER_LEN;

    memcpy(p, &port_num, data_len);
    return buf;
}

unsigned char *fsap_encode_cmd_get_kb_info(const char *kb_name, int *len)
{
    int data_len = strlen(kb_name);
    unsigned char *buf = init_packet(ADM_CMD_GET_KB_INFO, data_len);
    unsigned char *p = buf;
    p += ADM_HEADER_LEN;

    memcpy(p, kb_name, data_len);
    *len = data_len + ADM_HEADER_LEN;
    return buf;
}

fsa_kb_info *fsap_decode_rsp_get_kb_info(const unsigned char *buf)
{
    return fsap_decode_rsp_get_kb_info_all(buf);
}

unsigned char *fsap_encode_cmd_get_kb_info_all(int *len)
{
    *len = ADM_HEADER_LEN;
    return fsap_encode_cmd_no_params(ADM_CMD_GET_KB_INFO_ALL);
}

fsa_kb_info *fsap_decode_rsp_get_kb_info_all(const unsigned char *buf)
{
    const unsigned char *p;
    uint8_t n_entries;
    uint8_t name_len;
    fsa_kb_info *first_ki = NULL;
    fsa_kb_info *ki;

    p = buf;

    memcpy(&n_entries, p, 1);
    p += 1;

    for (int i = 0; i < n_entries; i++) {
        /* create new kb_info obj, unpack fields into it */
        ki = fsa_kb_info_new();

        memcpy(&name_len, p, 1);
        p += 1;

        ki->name = (unsigned char *)malloc((name_len+1) * sizeof(char));
        memcpy(ki->name, p, name_len);
        ki->name[name_len] = '\0';
        p += name_len;

        memcpy(&(ki->pid), p, 4);
        p += 4;

        memcpy(&(ki->port), p, 2);
        p += 2;

        memcpy(&(ki->status), p, 1);
        p += 1;

        memcpy(&(ki->num_segments), p, 1);
        p += 1;

        memcpy(&(ki->p_segments_len), p, 1);
        p += 1;

        ki->p_segments_data = (uint8_t *)malloc(ki->p_segments_len);
        memcpy(ki->p_segments_data, p, ki->p_segments_len);
        p += ki->p_segments_len;

        ki->next = first_ki;
        first_ki = ki;
    }

    return first_ki;
}

/* send to indicate that a generic client/server error occured */
unsigned char *fsap_encode_rsp_error(const char *msg, int *len)
{
    int data_len = strlen(msg);
    unsigned char *buf = init_packet(ADM_RSP_ERROR, data_len);
    unsigned char *p = buf;
    p += ADM_HEADER_LEN;

    memcpy(p, msg, data_len);
    *len = data_len + ADM_HEADER_LEN;
    return buf;
}

unsigned char *fsap_encode_rsp_get_kb_info(const fsa_kb_info *ki, int *len)
{
    uint8_t cmd;
    unsigned char *buf, *p;

    /* use same response format as kb_info_all... */
    buf = fsap_encode_rsp_get_kb_info_all(ki, len);

    /* ...but set command to different value */
    cmd = ADM_RSP_GET_KB_INFO;
    p = buf + ADM_H_LEN + ADM_H_VERS_LEN;
    memcpy(p, &cmd, ADM_H_CMD_LEN);

    return buf;
}

unsigned char *fsap_encode_rsp_get_kb_info_all(const fsa_kb_info *ki, int *len)
{
    int data_len = 1; /* number of kb entries, 0-255 */
    const fsa_kb_info *cur_ki;
    uint8_t n_entries = 0;
    unsigned char *buf, *p;

    uint8_t name_len;

    /* name_len name pid port status num_segments p_segments_len segments 
     * 1        *    4   2    1      1            1              *       */
    int base_entry_size = 10;
    
    /* loop through once to find size and number of entries */
    for (cur_ki = ki; cur_ki != NULL; cur_ki = cur_ki->next) {
        n_entries += 1;
        data_len += base_entry_size;
        data_len += strlen((char *)cur_ki->name);
        data_len += cur_ki->p_segments_len * sizeof(uint8_t);
    }

    /* create and fill buffer */
    buf = init_packet(ADM_RSP_GET_KB_INFO_ALL, data_len);
    p = buf;
    p += ADM_HEADER_LEN; /* move to start of data section */

    memcpy(p, &n_entries, 1);
    p += 1;

    for (cur_ki = ki; cur_ki != NULL; cur_ki = cur_ki->next) {
        name_len = (uint8_t)strlen((char *)cur_ki->name); 

        memcpy(p, &name_len, 1);
        p += 1;

        memcpy(p, cur_ki->name, name_len);
        p += name_len;

        memcpy(p, &(cur_ki->pid), 4);
        p += 4;

        memcpy(p, &(cur_ki->port), 2);
        p += 2;

        memcpy(p, &(cur_ki->status), 1);
        p += 1;

        memcpy(p, &(cur_ki->num_segments), 1);
        p += 1;

        memcpy(p, &(cur_ki->p_segments_len), 1);
        p += 1;

        memcpy(p, cur_ki->p_segments_data, cur_ki->p_segments_len);
        p += cur_ki->p_segments_len;
    }

    *len = data_len + ADM_HEADER_LEN;
    return buf;
}
