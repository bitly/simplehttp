#ifndef _SIMPLEHTTP_OPTIONS_H
#define _SIMPLEHTTP_OPTIONS_H

enum required_option {
    OPT_REQUIRED = 1,
    OPT_OPTIONAL = 0
};

int option_parse_command_line(int argc, char **argv);
int option_define_int(const char *option_name, int required, int default_val, int *dest, int(*cb)(int value), const char *help);
int option_define_str(const char *option_name, int required, const char *default_val, char **dest, int(*cb)(char *value), const char *help);
int option_define_bool(const char *option_name, int required, int default_val, int *dest, int(*cb)(int value), const char *help);
int option_define_char(const char *option_name, int required, char default_val, char *dest, int(*cb)(char value), const char *help);

void option_help();
int option_get_int(const char *option_name);
char *option_get_str(const char *option_name);
char option_get_char(const char *option_name);

void free_options();

#endif
