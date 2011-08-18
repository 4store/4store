#ifndef ADMIN_PROTOCOL_H
#define ADMIN_PROTOCOL_H

#include "admin_common.h"

int fsap_decode_header(const unsigned char *buf, uint8_t *cmd, uint16_t *size);

unsigned char *fsap_encode_cmd_get_kb_info(const char *kb_name, int *len);
unsigned char *fsap_encode_rsp_get_kb_info(const fsa_kb_info *ki, int *len);
fsa_kb_info *fsap_decode_rsp_get_kb_info(const unsigned char *buf);

unsigned char *fsap_encode_cmd_get_kb_info_all(int *len);
unsigned char *fsap_encode_rsp_get_kb_info_all(const fsa_kb_info *ki,
                                               int *len);
fsa_kb_info *fsap_decode_rsp_get_kb_info_all(const unsigned char *buf);

unsigned char *fsap_encode_cmd_stop_kb(const char *kb_name, int *len);
unsigned char *fsap_encode_rsp_stop_kb(int status, int *len);
int fsap_decode_rsp_stop_kb(unsigned char *buf);

unsigned char *fsap_encode_cmd_start_kb(const char *kb_name, int *len);
unsigned char *fsap_encode_rsp_start_kb(int status, int *len);
int fsap_decode_rsp_start_kb(unsigned char *buf);

unsigned char *fsap_encode_rsp_error(const char *msg, int *len);
char *fsap_decode_rsp_error(unsigned char *buf, int len);

#endif
