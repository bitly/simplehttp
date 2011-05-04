#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <libgen.h> // for basename()

#include "options.h"
#include "uthash.h"

enum option_type {
    OPT_BOOL = 1,
    OPT_STR = 2,    
    OPT_INT = 3,
    OPT_FLOAT = 4,
    OPT_CHAR = 5,
};

struct Option {
    char *option_name;
    int option_type;
    int required;
    int found;
    char *value_str;
    int value_int;
    char value_char;
    
    int default_int;
    char *default_str;
    char default_char;
    int *dest_int;
    char **dest_str;
    char *dest_char;
    int(*cb_int)(int value);
    int(*cb_str)(char *value);
    int(*cb_char)(char value);
    
    char *help;
    UT_hash_handle hh;
};

struct Option *option_list = NULL;
char *option_process_name = NULL;

int format_option_name(char *option_name);
int option_sort(struct Option *a, struct Option *b)
{
    return strcmp(a->option_name, b->option_name);
}

int help_cb(int value) {
    option_help();
    free_options();
    return 0;
}

/*
handle the following option formats
-p {value} -p={value} (for single character option_list)
--port {value} --port={value} (for longer option_list)
--debug (implicit true)
--debug=true|false

@return 1 when parsing was successful. 0 when not
*/
int option_parse_command_line(int argc, char **argv) {
    int i;
    char *option_str;
    char *option_name;
    size_t option_name_len;
    char *value;
    struct Option *option, *tmp_option;
    
    // lazily define help option
    HASH_FIND_STR(option_list, "help", option);
    if (!option) {
        option_define_bool("help", OPT_OPTIONAL, 0, NULL, help_cb, "list usage");
    }
    
    option_process_name=basename(argv[0]);
    for (i=1; i < argc; i++) {
        // fprintf(stdout, "DEBUG: option %d : %s\n", i, argv[i]);
        option_str = argv[i];
        // find the option_name
        if (strncmp(option_str, "--", 2) != 0) {
            fprintf(stderr, "ERROR: invalid argument \"%s\"\n", option_str);
            return 0;
        }
        option_name = strchr(option_str, '-');
        option_name++;
        option_name = strchr(option_name, '-');
        option_name++;
        value = strchr(option_name, '=');
        option_name_len = strlen(option_str) - (option_name - option_str);
        if (value != NULL) {
            option_name_len -= strlen(value);
            *value = '\0';
            value++;
        }
        if (format_option_name(option_name)) {
            fprintf(stderr, "ERROR: unknown option \"--%s\"\n", option_name); // option_str ?
            return 0;
        }
        HASH_FIND(hh, option_list, option_name, option_name_len, option);
        if (!option) {
            fprintf(stderr, "ERROR: unknown option \"--%s\"\n", option_name); // option_str ?
            return 0;
        }
        if (option->option_type != OPT_BOOL && value == NULL) {
            fprintf(stderr, "ERROR: missing argument for \"--%s\"", option_name);
            return 0;
        }
        
        // TODO: strip quotes from value
        
        switch(option->option_type) {
        case OPT_CHAR:
            if (strlen(value) != 1) {
                fprintf(stderr, "ERROR: argument for --%s must be a single character (got %s)\n", option_name, value);
                return 0;
            }
            option->value_char = value[0];
            if (option->cb_char) {
                if (!option->cb_char(option->value_char)) {
                    return 0;
                }
            }
            if (option->dest_char) {
                *(option->dest_char) = option->value_char;
            }
            break;
        case OPT_STR:
            option->value_str = value;
            if (option->cb_str) {
                if(!option->cb_str(value)){
                    return 0;
                }
            }
            if (option->dest_str) {
                *(option->dest_str) = strdup(value);
            }
            break;
        case OPT_BOOL:
            if (value == NULL) {
                option->value_int = 1;
            } else if (strcasecmp(value, "false") == 0) {
                option->value_int = 0;
            } else if (strcasecmp(value, "true") == 0) {
                option->value_int = 1;
            } else {
                fprintf(stderr, "ERROR: unknown value for --%s (%s). should be \"true\" or \"false\"\n", option->option_name, value);
                return 0;
            }
            if (option->cb_int) {
                if(!option->cb_int(option->value_int)) {
                    return 0;
                };
            }
            if (option->dest_int) {
                *(option->dest_int) = option->value_int;
            }
            break;
        case OPT_INT:
            option->value_int = atoi(value);
            if (option->cb_int) {
                if(!option->cb_int(option->value_int)) {
                    return 0;
                }
            }
            if (option->dest_int) {
                *(option->dest_int) = option->value_int;
            }
            break;
        }
        option->found++;
    }
    
    // check for not found entries
    HASH_ITER(hh, option_list, option, tmp_option) {
        if (option->required == OPT_REQUIRED && option->found == 0) {
            fprintf(stderr, "ERROR: required option --%s not present\n", option->option_name);
            fprintf(stderr, "       for a complete list of options use --help\n");
            return 0;
        }
    }
    return 1;
}

/* 
@returns -1 if option not found or not defined
*/
int option_get_int(const char *option_name) {
    struct Option *option;
    char *tmp_option_name = strdup(option_name);
    if (format_option_name(tmp_option_name)) {
        free(tmp_option_name);
        return -1;
    }
    HASH_FIND_STR(option_list, tmp_option_name, option);
    free(tmp_option_name);
    if (!option) {
        return -1;
    }
    if (option->option_type != OPT_INT && option->option_type != OPT_BOOL) {return -1;}
    if (option->found) {
        return option->value_int;
    }
    return option->default_int;
}

char *option_get_str(const char *option_name) {
    struct Option *option;
    char *tmp_option_name = strdup(option_name);
    if (format_option_name(tmp_option_name)) {
        free(tmp_option_name);
        return NULL;
    }
    HASH_FIND_STR(option_list, tmp_option_name, option);
    free(tmp_option_name);
    if (!option) {return NULL;}
    if (option->option_type != OPT_STR) {return NULL;}
    if (option->found) {
        return option->value_str;
    }
    return option->default_str;
}

char option_get_char(const char *option_name) {
    struct Option *option;
    char *tmp_option_name = strdup(option_name);
    if (format_option_name(tmp_option_name)) {
        free(tmp_option_name);
        return '\0';
    }
    HASH_FIND_STR(option_list, tmp_option_name, option);
    free(tmp_option_name);
    if (!option) {return '\0';}
    if (option->option_type != OPT_CHAR) {return '\0';}
    if (option->found) {
        return option->value_char;
    }
    return option->default_char;
}

struct Option *new_option(const char *option_name, int required, const char *help){
    struct Option *option;
    char *tmp_option_name = strdup(option_name);
    if (format_option_name(tmp_option_name)) {
        fprintf(stderr, "ERROR: option %s is invalid\n", option_name);
        free(tmp_option_name);
        return NULL;
    }

    HASH_FIND_STR(option_list, tmp_option_name, option);
    if (option){
        fprintf(stderr, "ERROR: option %s is already defined\n", tmp_option_name);
        return NULL;
    }
    option = malloc(sizeof(struct Option));
    option->option_name = tmp_option_name;
    option->required = required;
    option->found = 0;
    option->value_str = NULL;
    option->value_int = 0;
    option->value_char = '\0';
    option->cb_int = NULL;
    option->default_int = 0;
    option->dest_int = NULL;
    option->cb_str = NULL;
    option->default_str = NULL;
    option->dest_str = NULL;
    option->cb_char = NULL;
    option->default_char = 0;
    option->dest_char = NULL;
    option->help = NULL;
    if (help) {
        option->help = strdup(help);
    }
    //fprintf(stdout, "adding option %s to option_list %p\n", option->option_name, option);
    HASH_ADD_KEYPTR(hh, option_list, option->option_name, strlen(option->option_name), option);
    return option;
}

int option_define_int(const char *option_name, int required, int default_val, int *dest, int(*cb)(int value), const char *help) {
    struct Option *option = new_option(option_name, required, help);
    if (!option) {return -1;}
    option->option_type = OPT_INT;
    option->default_int = default_val;
    option->dest_int = dest;
    option->cb_int = cb;
    return 1;
}

int option_define_str(const char *option_name, int required, char *default_val, char **dest, int(*cb)(char *value), const char *help) {
    struct Option *option = new_option(option_name, required, help);
    if (!option) {return -1;}
    option->option_type = OPT_STR;
    if (default_val) {
        option->default_str = strdup(default_val);
    }
    option->dest_str = dest;
    option->cb_str = cb;
    return 1;
}

int option_define_bool(const char *option_name, int required, int default_val, int *dest, int(*cb)(int value), const char *help) {
    struct Option *option = new_option(option_name, required, help);
    if (!option) {return -1;}
    option->option_type = OPT_BOOL;
    option->default_int = default_val;
    option->dest_int = dest;
    option->cb_int = cb;
    return 1;
}

int option_define_char(const char *option_name, int required, char default_val, char *dest, int(*cb)(char value), const char *help) {
    struct Option *option = new_option(option_name, required, help);
    if (!option) {return -1;}
    option->option_type = OPT_CHAR;
    option->default_char = default_val;
    option->dest_char = dest;
    option->cb_char = cb;
    return 1;
}

void option_help() {
    struct Option *option, *tmp_option;
    char buffer[1024] = {'\0'};
    
    // lazily define help option
    HASH_FIND_STR(option_list, "help", option);
    if (!option) {
        option_define_bool("help", OPT_OPTIONAL, 0, NULL, help_cb, "list usage");
    }
    
    fprintf(stdout, "\n");
    fprintf(stdout, "%s accepts the following options, listed alphabetically.\n\n", option_process_name);
    fprintf(stdout, "OPTIONS\n");
    
    HASH_SORT(option_list, option_sort);
    HASH_ITER(hh, option_list, option, tmp_option) {
        switch(option->option_type) {
        case OPT_STR:
            sprintf(buffer, "--%s=<str>", option->option_name);
            break;
        case OPT_CHAR:
            sprintf(buffer, "--%s=<char>", option->option_name);
            break;
        case OPT_INT:
            sprintf(buffer, "--%s=<int>", option->option_name);
            break;
        case OPT_BOOL:
            if (option->default_int != 0) {
                sprintf(buffer, "--%s=True|False", option->option_name);
            } else {
                sprintf(buffer, "--%s", option->option_name);
            }
            break;
        }
        fprintf(stdout, "  %-22s", buffer);
        if (option->help) {
            fprintf(stdout, " %s", option->help);
        }
        fprintf(stdout, "\n");
        switch(option->option_type) {
        case OPT_CHAR:
            if (option->default_char) {
                fprintf(stdout, "%25sdefault:%c\n", "", option->default_char);
            }
            break;
        case OPT_STR:
            if (option->default_str) {
                if (isatty(fileno(stdout))) {
                    fprintf(stdout, "%25sdefault: %c[1m%s%c[0m\n", "", 27, option->default_str, 27);
                } else {
                    fprintf(stdout, "%25sdefault: %s\n", "", option->default_str);
                }
            }
            break;
        case OPT_INT:
            if (option->default_int) {
                fprintf(stdout, "%25sdefault: %d\n", "", option->default_int);
            }
            break;
        default:
            break;
        }
    }
    fprintf(stdout, "\n");
}

void free_options() {
    struct Option *option, *tmp_option;
    HASH_ITER(hh, option_list, option, tmp_option) {
        HASH_DELETE(hh, option_list, option);
        free(option->option_name);
        free(option->help);
        free(option->default_str);
        free(option);
    }
}

int format_option_name(char *option_name) {
    char *ptr = option_name;
    char *end = ptr + strlen(option_name);
    while(ptr <= end && *ptr != '\0') {
        if (*ptr >= 'A' && *ptr <= 'Z') {
            *ptr += 32;
        } else if (*ptr == '_') {
            *ptr = '-';
        } else if (*ptr < 48 && *ptr != 45){
            fprintf(stderr, "invalid char %c\n", *ptr);
            return 1;
        }
        ptr++;
    }
    return 0;
}
