/*
	Simple data sink code for kserver
*/
#define _FILE_OFFSET_BITS 64
#include <arpa/inet.h>
#include <ctype.h>
#include <libconfig.h>
#include <netinet/in.h>
#include <readline/history.h>
#include <readline/readline.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>
#include "recformat.h"

#define MAXCON	10
#define MAXSTR	1024
#define MBYTE	0x100000
#define DEFCONFIG	"general.conf"

/*				Types and declarations		*/
pid_t StartProcess(char *cmd);
struct con_struct {
	int fd;
	int ip;
	char *buf;
	int len;
	long long cnt;
	struct rec_header_struct *header;
};

/*				Global variables		*/
struct cfg_struct {
	int Port;			// Data port 0xA336
	char MyName[MAXSTR];		// The server host name
	char SlaveList[MAXCON][MAXSTR];	// Crate host names list
	int NSlaves;			// number of crates
	char SlaveCMD[MAXSTR];		// Command to start slave
	int TriggerMasterCrate;		// Trigger master crate (index in slave list)
	int TriggerMasterModule;	// Trigger master module
	char LogFile[MAXSTR];		// dsink log file name
	char LogTermCMD[MAXSTR];	// start log view in a separate window
	char DataDir[MAXSTR];		// directory to write data
	char InitScript[MAXSTR];	// initialize modules
	char StartScript[MAXSTR];	// put vme into acquire mode. Agruments: server, port
	char StopScript[MAXSTR];	// stop acquire mode
	char InhibitScript[MAXSTR];	// arguments: module number and set/cl
} Config;

struct run_struct {
	FILE *fLog;
	pid_t fLogPID;
	int fdPort;
	FILE *fSlave[MAXCON];
	int fdSlave[MAXCON];
	con_struct Con[MAXCON];
	int NCon;
	int iStop;
} Run;

/*				Functions			*/
/*	Initialize data connection port				*/
int BindPort(void)
{
	int fd;
	struct sockaddr_in name;
	
	fd = socket (PF_INET, SOCK_STREAM, 0);
	if (fd < 0) {
		printf("Can not create socket.: %m\n");
		return fd;
	}
	name.sin_family = AF_INET;
	name.sin_port = htons(Config.Port);
	name.sin_addr.s_addr = htonl(INADDR_ANY);
	if (bind(fd, (struct sockaddr *)&name, sizeof(name)) < 0) {
		printf("Can not bind to port %d: %m\n", Config.Port);
		close(fd);
		return -1;
	}
	
	if (listen(fd, MAXCON) < 0) {
		printf("Can not listen to port %d: %m\n", Config.Port);
		close(fd);
		return -1;
	}
	
	return fd;
}

/*	Open client data connection to the server		*/
void OpenCon(int fd, con_struct *con)
{
	struct sockaddr_in addr;
	socklen_t len;
	int irc;
	
	memset(con, 0, sizeof(con_struct));
	len = sizeof(addr);
	irc = accept(fd, (struct sockaddr *)&addr, &len);
	if (irc < 0) {
		printf("Connection accept error: %m\n");
		return;
	}
	con->buf = (char *)malloc(MBYTE);
	if (!con->buf) {
		printf("Can not allocate buffer of %d bytes: %m\n", MBYTE);
		close(irc);
		return;
	}
	con->header = (struct rec_header_struct *) con->buf;
	con->fd = irc;
	con->ip = addr.sin_addr.s_addr;
	printf("connection from %s accepted\n", inet_ntoa(addr.sin_addr));
}

/*	Ignore new connection if too many. Should never happen.	*/
void DropCon(int fd)
{
	struct sockaddr_in addr;
	socklen_t len;
	int irc;
	
	len = sizeof(addr);
	irc = accept(fd, (struct sockaddr *)&addr, &len);
	if (irc < 0) {
		printf("Connection accept error: %m\n");
		return;
	}
	printf("Too many connections dropping from %s\n", inet_ntoa(addr.sin_addr));
	close(irc);
}

/*	Clean Con array from closed connections			*/
void CleanCon(void)
{
	int i, j;
	for (j=0; j<Run.NCon; j++) if (Run.Con[j].fd < 0) {	
		for (i=j; i<Run.NCon-1; i++) memcpy(&Run.Con[i], &Run.Con[i+1], sizeof(Run.Con[0]));
		Run.NCon--;
	}
}

/*	Get Data						*/
void GetAndWrite(int num)
{
	int irc;
	struct con_struct *con;
	
	con = &Run.Con[num];
	if (con->len == 0) {	// getting length
		irc = read(con->fd, con->buf, sizeof(int));
		if (irc != sizeof(int) || con->header->len < sizeof(struct rec_header_struct) || con->header->len > MBYTE) {
			fprintf(Run.fLog, "DSINK: Connection closed or stream data error irc = %d  len = %d from %X %m\n", irc, con->header->len, con->ip);
			free(con->buf);
			close(con->fd);
			con->fd = -1;
			return;
		}
		con->len = irc;
	} else {		// getting body
		irc = read(con->fd, &con->buf[con->len], con->header->len - con->len);
		if (irc <= 0) {
			fprintf(Run.fLog, "DSINK: Connection closed unexpectingly or stream data error irc = %d at %X %m\n", irc, con->ip);
			free(con->buf);
			close(con->fd);
			con->fd = -1;
			return;
		}
		con->len += irc;
		if (con->len  == con->header->len) {
			if (con->header->ip != 0x7F000001) {
				fprintf(Run.fLog, "DSINK: Wrong data signature %X - sychronization lost ? @%X\n", con->header->ip, con->ip);
				free(con->buf);
				close(con->fd);
				con->fd = -1;
				return;
			}
			con->header->ip = con->ip;
//			ProcessData(con->buf, con->len);
			con->len = 0;
			con->cnt += con->header->len;
			return;
		}
	}
	return;
}

/*	Open log file and xterm							*/
int OpenLog(void)
{
	char cmd[MAXSTR];

	Run.fLog = fopen(Config.LogFile, "at");
	if (!Run.fLog) {
		printf("Can not open log-file: %s\n", Config.LogFile);
		return -10;
	}
	snprintf(cmd, MAXSTR, Config.LogTermCMD, Config.LogFile);
	Run.fLogPID = StartProcess(cmd);
	if (Run.fLogPID < 0) return -11;
	return 0;
}

/*	Open command connection to VME crates and start uwfdtool in them	*/
int OpenSlaves(void)
{
	int i;
	char cmd[MAXSTR];

	for (i=0; i<Config.NSlaves; i++) {
		snprintf(cmd, MAXSTR, "ssh %s \"%s\"", Config.SlaveList[i], Config.SlaveCMD);
		Run.fSlave[i] = popen(cmd, "r");
		if (!Run.fSlave[i]) {
			fprintf(Run.fLog, "Can not do ssh to %s: %s (%m)\n", Config.SlaveList[i], cmd);
			return -10;
		}
		Run.fdSlave[i] = fileno(Run.fSlave[i]);
		if (Run.fdSlave[i] == -1) {
			fprintf(Run.fLog, "Can not get fd of ssh to %s\n", Config.SlaveList[i]);
			return -21;			
		}
	}
	return 0;
}

/*	Close command connections						*/
void CloseSlaves(void)
{
	int i;
	for (i=0; i<Config.NSlaves; i++) if (Run.fSlave[i]) {
		fprintf(Run.fSlave[i], "q\n");
		fprintf(Run.fSlave[i], "q\n");
		pclose(Run.fSlave[i]);
	}
}

/*	Read configuration file							*/
int ReadConf(char *fname)
{
	config_t cnf;
	int tmp, i;
	char *stmp;
	char *tok;

	memset(&Config, 0, sizeof(Config));
	config_init(&cnf);
	if (config_read_file(&cnf, fname) != CONFIG_TRUE) {
        	printf("Configuration error in file %s at line %d: %s\n", 
        		fname, config_error_line(&cnf), config_error_text(&cnf));
            	return -10;
    	}
//	int Port;			// Data port 0xA336
	Config.Port = (config_lookup_int(&cnf, "Sink.Port", &tmp)) ? tmp : 0xA336;
//	char MyName[MAXSTR];		// The server host name
	strncpy(Config.MyName, (config_lookup_string(&cnf, "Sink.MyName", (const char **) &stmp)) ? stmp : "dserver.danss.local", MAXSTR);
//	char SlaveList[MAXCON][MAXSTR];	// Crate host names list
	if (!config_lookup_string(&cnf, "Sink.SlaveList", (const char **) &stmp)) stmp = (char *)"vme01.danss.local vme02.danss.local vme03.danss.local vme04.danss.local";
	tok = strtok(stmp, " \t,");
	for (i=0; i<MAXCON; i++) {
		if (!tok || !tok[0]) break;
		strncpy(Config.SlaveList[i], tok, MAXSTR);
		tok = strtok(NULL, " \t,");
	}
	Config.NSlaves = i;
	if (!Config.NSlaves) {
		printf("Bad configuration %s - no VME crates defined.\n", fname);
		return -11;
	} 
//	char SlaveCMD[MAXSTR];		// Command to start slave
	strncpy(Config.SlaveCMD, (config_lookup_string(&cnf, "Sink.SlaveCMD", (const char **) &stmp)) ? stmp : (char *)"cd bin;./uwfdtool", MAXSTR);
//	char TriggerMaster[MAXSTR];	// Trigger master module and crate (index in slave list)
	if (!config_lookup_string(&cnf, "Sink.TriggerMaster", (const char **) &stmp)) stmp = (char *)"0:1";
	tok = strtok(stmp, " \t,:");
	Config.TriggerMasterCrate = strtol(tok, NULL, 0);
	if (Config.TriggerMasterCrate < 0 || Config.TriggerMasterCrate >= Config.NSlaves) {
		printf("Config.TriggerMasterCrate = %d out of range in file %s\n", Config.TriggerMasterCrate, fname);
		return -12;
	}
	tok = strtok(NULL, " \t,:");
	Config.TriggerMasterModule = (tok) ? strtol(tok, NULL, 0) : 1;
//	char LogFile[MAXSTR];		// dsink log file name
	strncpy(Config.LogFile, (config_lookup_string(&cnf, "Sink.LogFile", (const char **) &stmp)) ? stmp : "dsink.log", MAXSTR);
//	char LogTermCMD[MAXSTR];	// start log view in a separate window
	strncpy(Config.LogTermCMD, (config_lookup_string(&cnf, "Sink.LogTermCMD", (const char **) &stmp)) ? stmp : "xterm -geometry 240x23 -title DSINK_Log -e tail -f dsink.log", MAXSTR);
//	char DataDir[MAXSTR];		// directory to write data
	strncpy(Config.DataDir, (config_lookup_string(&cnf, "Sink.DataDir", (const char **) &stmp)) ? stmp : "data", MAXSTR);
//	char InitScript[MAXSTR];	// initialize modules
	strncpy(Config.InitScript, (config_lookup_string(&cnf, "Sink.InitScript", (const char **) &stmp)) ? stmp : "p * main.bin;w 500;i general.conf", MAXSTR);
//	char StartScript[MAXSTR];	// put vme into acquire mode. Agruments: server, port
	strncpy(Config.StartScript, (config_lookup_string(&cnf, "Sink.StartScript", (const char **) &stmp)) ? stmp : "y * * %s:%d", MAXSTR);
//	char StopScript[MAXSTR];	// stop acquire mode
	strncpy(Config.StopScript, (config_lookup_string(&cnf, "Sink.StopScript", (const char **) &stmp)) ? stmp : "q", MAXSTR);
//	char InhibitScript[MAXSTR];	// arguments: module number and set/cl
	strncpy(Config.InhibitScript, (config_lookup_string(&cnf, "Sink.InhibitScript", (const char **) &stmp)) ? stmp : "m %d %d", MAXSTR);

	return 0;
}

/*	Spawn a new process and return its PID					*/
pid_t StartProcess(char *cmd)
{
	pid_t pid;
	
	pid = fork();
	if (!pid) {
		execl("/bin/bash", "/bin/bash", "-c", cmd, NULL);
		printf("Wrong return\n");
		return -20;
	} else if (pid < 0) {
		printf("Can not execute %s\n", cmd);
		return pid;
	}
	return pid;
}

/*	Print Help								*/
void Help(void)
{
	printf("Usage: dsink\n");
	printf("Commands:\n");
	printf("cmd <vme> <uwfdtool command> - send command to vme crate;\n");
	printf("file [<file_name>] - set file to write data;\n");
	printf("info - print statistics;\n");
	printf("init - init vme modules;\n");
	printf("list - list connected slaves;\n");
	printf("quit - Quit;\n");
	printf("start/stop - start/stop data taking;\n");
}

/*	Get text output from slave and send it to log				*/
void GetFromSlave(int i)
{
	char str[MAXSTR];
	fgets(str, MAXSTR, Run.fSlave[i]);
	fprintf(Run.fLog, "%s: %s", Config.SlaveList[i], str);
}

/*	Process commands							*/
static void ProcessCmd(char *cmd)
{
	
}

/*	The main								*/
int main(int argc, char **argv)
{
	fd_set set;
	struct timeval tm;
	int i, irc;
	time_t theTime;
	
	memset(&Run, 0, sizeof(Run));
	read_history(".dsink_history");

	if (ReadConf((char *)DEFCONFIG)) goto MyExit;
	if (OpenLog()) goto MyExit;
	theTime = time(NULL);
	fprintf(Run.fLog, "%sDSINK started.\n", ctime(&theTime));

	Run.fdPort = BindPort();
	if (Run.fdPort < 0) goto MyExit;	

	if (OpenSlaves()) goto MyExit;

	rl_callback_handler_install("DSINK >", ProcessCmd);	

	tm.tv_sec = 1;		// 1 s
	tm.tv_usec = 0;

//		Event loop
	while (!Run.iStop) {

		FD_ZERO(&set);
		FD_SET(fileno (rl_instream), &set);
		FD_SET(Run.fdPort, &set);
		for (i=0; i<Run.NCon; i++) FD_SET(Run.Con[i].fd, &set);
		for (i=0; i<Config.NSlaves; i++) FD_SET(Run.fdSlave[i], &set);

		irc = select(FD_SETSIZE, &set, NULL, NULL, &tm);
		if (irc < 0) continue;

		if (FD_ISSET(fileno (rl_instream), &set)) rl_callback_read_char ();
		if (FD_ISSET(Run.fdPort, &set)) {
			if (Run.NCon < MAXCON) {
				OpenCon(Run.fdPort, &Run.Con[Run.NCon]);
				if (Run.Con[Run.NCon].fd > 0) Run.NCon++;
			} else {
				DropCon(Run.fdPort);
			}
		}
		for (i=0; i<Run.NCon; i++) if (FD_ISSET(Run.Con[i].fd, &set)) GetAndWrite(i);
		for (i=0; i<Config.NSlaves; i++) if (FD_ISSET(Run.fdSlave[i], &set)) GetFromSlave(i);
		CleanCon();
		fflush(Run.fLog);
	}
MyExit:
	
	for (i=0; i<Run.NCon; i++) {
		free(Run.Con[i].buf);
		close(Run.Con[i].fd);
	}

	CloseSlaves();
	if (Run.fdPort)	close(Run.fdPort);
	sleep(2);
	if (Run.fLog) {
		fclose(Run.fLog);
		kill(Run.fLogPID, SIGINT);
	}
	rl_callback_handler_remove ();
	write_history(".dsink_history");	
	return 0;
}

