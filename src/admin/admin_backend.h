#ifndef ADMIN_BACKEND_H
#define ADMIN_BACKEND_H

int fsab_kb_info_init(fsa_kb_info *ki, const char *kb_name);
fsa_kb_info *fsab_get_local_kb_info(const char *kb_name);
fsa_kb_info *fsab_get_local_kb_info_all();
int fsab_stop_local_kb(const char *kb_name, int *err);
int fsab_stop_local_kb_all();

#endif
