#ifndef ADMIN_PROTOCOL_H
#define ADMIN_PROTOCOL_H

#include "admin_common.h"

/* generic encoders/decoders */
int fsap_decode_header(const unsigned char *buf, uint8_t *cmd, uint16_t *size);

unsigned char *fsap_encode_rsp_expect_n(int n, int *len);
int fsap_decode_rsp_expect_n(const unsigned char *buf);
unsigned char *fsap_encode_rsp_expect_n_kb(int n, int kb_len, int *len);
int fsap_decode_rsp_expect_n_kb(const unsigned char *buf, int *kb_name_len);

unsigned char *fsap_encode_rsp_error(const unsigned char *msg, int *len);
unsigned char *fsap_decode_rsp_error(const unsigned char *buf, int len);

/* get info kbs */
unsigned char *fsap_encode_cmd_get_kb_info(const unsigned char *kb_name,
                                           int *len);
unsigned char *fsap_encode_rsp_get_kb_info(const fsa_kb_info *ki, int *len);
fsa_kb_info *fsap_decode_rsp_get_kb_info(const unsigned char *buf);

unsigned char *fsap_encode_cmd_get_kb_info_all(int *len);
unsigned char *fsap_encode_rsp_get_kb_info_all(const fsa_kb_info *ki,
                                               int *len);
fsa_kb_info *fsap_decode_rsp_get_kb_info_all(const unsigned char *buf);

/* stop kbs */
unsigned char *fsap_encode_cmd_stop_kb_all(int *len,int force);
unsigned char *fsap_encode_cmd_stop_kb(const unsigned char *kb_name,
                                       int *len, int force);
unsigned char *fsap_encode_rsp_stop_kb(int retval,
                                       const unsigned char *kb_name,
                                       const unsigned char *msg,
                                       int *len);
fsa_kb_response *fsap_decode_rsp_stop_kb(const unsigned char *buf);

/* start kbs */
unsigned char *fsap_encode_cmd_start_kb_all(int *len);
unsigned char *fsap_encode_cmd_start_kb(const unsigned char *kb_name,
                                        int *len);
unsigned char *fsap_encode_rsp_start_kb(int retval,
                                        const unsigned char *kb_name,
                                        const unsigned char *msg,
                                        int *len);
fsa_kb_response *fsap_decode_rsp_start_kb(const unsigned char *buf);

/* delete kbs */
unsigned char *fsap_encode_cmd_delete_kb(const unsigned char *kb_name,
                                         int *len);
unsigned char *fsap_encode_rsp_delete_kb(int retval,
                                         const unsigned char *kb_name,
                                         const unsigned char *msg,
                                         int *len);
fsa_kb_response *fsap_decode_rsp_delete_kb(const unsigned char *buf);

/* create kb */
unsigned char *fsap_encode_cmd_create_kb(const fsa_kb_setup_args *ksargs,
                                         int *len);
fsa_kb_setup_args *fsap_decode_cmd_create_kb(const unsigned char *buf);
unsigned char *fsap_encode_rsp_create_kb(int retval,
                                         const unsigned char *kb_name,
                                         const unsigned char *msg,
                                         int *len);
fsa_kb_response *fsap_decode_rsp_create_kb(const unsigned char *buf);

#endif
