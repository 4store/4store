#ifndef ADMIN_PROTOCOL_H
#define ADMIN_PROTOCOL_H

#include "admin_common.h"

int fsap_decode_header(const unsigned char *buf, uint8_t *cmd, uint16_t *size);

unsigned char *fsap_encode_rsp_expect_n(int n, int *len);
int fsap_decode_rsp_expect_n(const unsigned char *buf);

unsigned char *fsap_encode_cmd_get_kb_info(const unsigned char *kb_name,
                                           int *len);
unsigned char *fsap_encode_rsp_get_kb_info(const fsa_kb_info *ki, int *len);
fsa_kb_info *fsap_decode_rsp_get_kb_info(const unsigned char *buf);

unsigned char *fsap_encode_cmd_get_kb_info_all(int *len);
unsigned char *fsap_encode_rsp_get_kb_info_all(const fsa_kb_info *ki,
                                               int *len);
fsa_kb_info *fsap_decode_rsp_get_kb_info_all(const unsigned char *buf);

unsigned char *fsap_encode_cmd_stop_kb_all(int *len);
unsigned char *fsap_encode_cmd_stop_kb(const unsigned char *kb_name,
                                       int *len);
unsigned char *fsap_encode_rsp_stop_kb(int retval,
                                       const unsigned char *kb_name,
                                       const unsigned char *msg,
                                       int *len);
fsa_kb_response *fsap_decode_rsp_stop_kb(const unsigned char *buf);

unsigned char *fsap_encode_cmd_start_kb_all(int *len);
unsigned char *fsap_encode_cmd_start_kb(const unsigned char *kb_name,
                                        int *len);
unsigned char *fsap_encode_rsp_start_kb(int retval,
                                        const unsigned char *kb_name,
                                        const unsigned char *msg,
                                        int *len);
fsa_kb_response *fsap_decode_rsp_start_kb(const unsigned char *buf);

unsigned char *fsap_encode_rsp_error(const unsigned char *msg, int *len);
unsigned char *fsap_decode_rsp_error(const unsigned char *buf, int len);

#endif
