export ldfdarwin := $(shell (uname | grep Darwin > /dev/null) && echo "-bind_at_load -multiply_defined suppress")
export ldflinux := $(shell (uname | grep Linux > /dev/null) && echo "-rdynamic")
