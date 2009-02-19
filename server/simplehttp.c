#include "simplehttp.h"

int 
get_uid(char *user)
{
    int retcode;
    struct passwd *pw;

    pw = getpwnam(user);
    if (pw == NULL) {
        retcode = -1;
    } else {
        retcode = (unsigned) pw->pw_uid;
    }
    return retcode;
}

int 
get_gid(char *group)
{
    int retcode;
    struct group *grent;

    grent = getgrnam(group);
    if (grent == NULL) {
        retcode = -1;
    } else {
        retcode = grent->gr_gid;
    }
    return retcode;
}

int 
get_user_gid(char *user)
{
    int retcode;
    struct passwd *pw;

    pw = getpwnam(user);
    if (pw == NULL) {
        retcode = -1;
    } else {
        retcode = pw->pw_gid;
    }
    return retcode;
}

void
generic_request_handler(struct evhttp_request *req, void *arg)
{
    struct evbuffer *evb = evbuffer_new();
    
    fprintf(stderr, "Request for %s from %s\n", req->uri, req->remote_host);

    evbuffer_add_printf(evb, "OK");
    evhttp_send_reply(req, HTTP_OK, "", evb);
    evbuffer_free(evb);
}

int main(int argc, char **argv)
{
    uid_t uid = 0;
    gid_t gid = 0;
    char *address;
    char *root = NULL;
    char *garg = NULL;
    char *uarg = NULL;
    int debug = 0;
    int daemon = 0;
    int port, ch, errno;
    pid_t pid, sid;
    struct evhttp *httpd;
    
    address = "0.0.0.0";
    port = 8080;
    
    while ((ch = getopt(argc, argv, "a:p:d:D:r:u:g:")) != -1) {
        switch (ch) {
        case 'a':
            address = optarg;
            break;
        case 'p':
            port = atoi(optarg);
            break;
        case 'd':
            debug = 1;
            break;
        case 'r':
            root = optarg;
            break;
        case 'D':
            daemon = 1;
            break;
        case 'g':
            garg = optarg;
            break;
        case 'u':
            uarg = optarg;
            break;
        }
    }
    
    if (daemon) {
	    pid = fork();
	    if (pid < 0) {
			exit(EXIT_FAILURE);
	    } else if (pid > 0) {
			exit(EXIT_SUCCESS);
	    }

	    umask(0);
	    sid = setsid();
	    if (sid < 0) {
	    	exit(EXIT_FAILURE);
	    }
	}
   
    if (uarg != NULL) {
        uid = get_uid(uarg);
        gid = get_user_gid(uarg);
        if (uid < 0) {
            uid = atoi(uarg);
            uarg = NULL;
        }
        if (uid == 0) {
            err(1, "invalid user");
        }
    }
    if (garg != NULL) {
        gid = get_gid(garg);
        if (gid < 0) {
            gid = atoi(garg);
            if (gid == 0) {
                err(1, "invalid group");
            }
        }
    }
   
    if (root != NULL) {
        if (chroot(root) != 0) {
            err(1, strerror(errno));
        }
        if (chdir("/") != 0) {
            err(1, strerror(errno));
        }
    }
    
    if (getuid() == 0) {
        if (uarg != NULL) {
            if (initgroups(uarg, (int) gid) != 0) {
                err(1, "initgroups() failed");
            }
        } else {
            if (setgroups(0, NULL) != 0) {
                err(1, "setgroups() failed");
            }
        }
        if (gid != getgid() && setgid(gid) != 0) {
            err(1, "setgid() failed");
        }
        if (setuid(uid) != 0) {
            err(1, "setuid() failed");
        }
    }
    
    event_init();
    httpd = evhttp_start(address, port);
    evhttp_set_gencb(httpd, generic_request_handler, NULL);
    event_dispatch();

    /* Not reached in this code as it is now. */
    evhttp_free(httpd);

    return 0;
}