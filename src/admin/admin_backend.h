#ifndef ADMIN_BACKEND_H
#define ADMIN_BACKEND_H

int fsab_kb_info_init(fsa_kb_info *ki, const unsigned char *kb_name, int *err);
fsa_kb_info *fsab_get_local_kb_info(const unsigned char *kb_name, int *err);
fsa_kb_info *fsab_get_local_kb_info_all(int *err);
int fsab_stop_local_kb(const unsigned char *kb_name, int *err);
int fsab_stop_local_kb_all();
int fsab_start_local_kb(const unsigned char *kb_name, int *exit_val,
                        unsigned char **output, int *err);
int fsab_delete_local_kb(const unsigned char *kb_name, int *exit_val,
                         unsigned char **output, int *err);

#endif
