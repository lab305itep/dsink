#ifndef DSINK_H
#define DSINK_H

#include <stdio.h>
#include <unistd.h>

#define MAXCON	10
#define MAXWFD	70
#define MAXSTR	1024
#define MBYTE	0x100000
#define DEFCONFIG	"general.conf"
#define SSH	"/usr/bin/ssh"
#define SHELL	"/bin/sh"
#define TXT_BOLDRED	"\033[1;31m"
#define TXT_BOLDGREEN	"\033[1;32m"
#define TXT_BOLDYELLOW	"\033[1;33m"
#define TXT_BOLDBLUE	"\033[1;34m"
#define TXT_NORMAL	"\033[0;39m"
#define TXT_INFO	TXT_BOLDGREEN  " INFO  " TXT_NORMAL
#define TXT_WARN	TXT_BOLDBLUE   " WARN  " TXT_NORMAL
#define TXT_ERROR	TXT_BOLDYELLOW " ERROR " TXT_NORMAL
#define TXT_FATAL	TXT_BOLDRED    " FATAL " TXT_NORMAL
#define DEF_DATAFILE	"defsink.dat"

/*				Types and declarations		*/
void Log(const char *msg, ...) __attribute__ ((format (printf, 1, 2)));
void SendScript(FILE *f, const char *script);
pid_t StartProcess(char *cmd);

struct con_struct {
	int fd;
	int ip;
	char *buf;
	int len;
	long long cnt;
	struct rec_header_struct *header;
};
struct pipe_struct {
	int fd[2];
	FILE* f;
};

struct slave_struct {
	pid_t PID;
	struct pipe_struct in;
	struct pipe_struct out;
	struct pipe_struct err;		
};

#endif
