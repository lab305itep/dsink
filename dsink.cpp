/*
	Simple data sink code for dserver
	SvirLex, ITEP, 2016
*/
#define _FILE_OFFSET_BITS 64
#include <arpa/inet.h>
#include <ctype.h>
#include <errno.h>
#include <libconfig.h>
#include <netdb.h>
#include <netinet/in.h>
#include <readline/history.h>
#include <readline/readline.h>
#include <signal.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>
#include "dsink.h"
#include "dmodule.h"
#include "recformat.h"

/*				Global variables		*/
struct cfg_struct {
	int Port;			// Data port 0xA336
	char MyName[MAXSTR];		// The server host name
	int UDPPort;			// UDP port  0xA230
	char UDPHost[MAXSTR];		// Host to send UDP data
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
	char InhibitScript[MAXSTR];	// Inhibit triggers
	char EnableScript[MAXSTR];	// Enables triggers
	int MaxEvent;			// Size of Event cache 
} Config;

struct run_struct {
	FILE *fLog;			// Log file
	pid_t fLogPID;			// Log file broser PID
	FILE *fData;			// Data file
	char fDataName[MAXSTR];		// Data file name
	struct rec_header_struct fHead;	// record header to write data file
	int fdUDP;			// udp socket to send data for analysis
	void *wData;			// mememory for write data
	int wDataSize;			// size of wData 
	int iTempData;			// Signature of temporary file
	int fdPort;			// Port for input data connections
	struct slave_struct Slave[MAXCON];	// Slave VME connections
	con_struct Con[MAXCON];		// Data connections
	int NCon;			// Number of data connections
	int iStop;			// Quit flag
	int iRun;			// DAQ running flag
	int Initialized;		// VME modules initialized
	Dmodule *WFD[MAXWFD];		// class WFD modules for data processing
	struct event_struct *Evt;	// Pointer to event cache
	struct event_struct *EvtCopy;	// Pointer to event cache copy (for rotation)
	int lTokenBase;			// long token of the first even in the cache
	int TypeStat[8];		// record types statistics
	int RecStat[2];			// events/selftriggers in the output file
} Run;

/*				Functions			*/
/*	Add record to the event					*/
void Add2Event(int num, struct blkinfo_struct *info)
{
	int k;
	char *ptr;
	int new_len;

	// Find our event buffer
	k = info->lToken - Run.lTokenBase;
	if (k < 0) {
		Log(TXT_ERROR "DSINK: Internal error - negative shift in Event cache: long token = %d token base = %d Module %d.\n", 
			info->lToken, Run.lTokenBase, num);
		return;
	}
	if (k >= Config.MaxEvent) {
		Log(TXT_ERROR "DSINK: Event cache of %d events looks too small: long token = %d token base = %d Module %d.\n", 
			Config.MaxEvent, info->lToken, Run.lTokenBase, num);
		return;
	}
	// Check memory
	new_len = ((info->data[0] & 0x1FF) + 1) * sizeof(short);
	if (new_len + Run.Evt[k].len > Run.Evt[k].size) {
		ptr = (char *)realloc(Run.Evt[k].data, Run.Evt[k].size + MCHUNK);
		if (!ptr) {
			Log(TXT_ERROR "DSINK: Out of memory: %m\n");
			return;
		}
		Run.Evt[k].data = ptr;
		Run.Evt[k].size += MCHUNK;
	}
	// Put module number in the place of token - 12 LS bits of data[1]
	info->data[1] &= 0xF000;
	info->data[1] |= num & 0xFFF;
	// Store the data
	memcpy(Run.Evt[k].data + Run.Evt[k].len, info->data, new_len);
	Run.Evt[k].len += new_len;
}

/*	Initialize data connection port				*/
int BindPort(void)
{
	int fd;
	struct sockaddr_in name;
	
	fd = socket (PF_INET, SOCK_STREAM, 0);
	if (fd < 0) {
		printf(TXT_FATAL "DSINK: Can not create socket.: %m\n");
		return fd;
	}
	name.sin_family = AF_INET;
	name.sin_port = htons(Config.Port);
	name.sin_addr.s_addr = htonl(INADDR_ANY);
	if (bind(fd, (struct sockaddr *)&name, sizeof(name)) < 0) {
		printf(TXT_FATAL "DSINK: Can not bind to port %d: %m\n", Config.Port);
		close(fd);
		return -1;
	}
	
	if (listen(fd, MAXCON) < 0) {
		printf(TXT_FATAL "DSINK: Can not listen to port %d: %m\n", Config.Port);
		close(fd);
		return -1;
	}
	
	return fd;
}

/*	Find mimimal delimiter 					*/
void CheckReadyEvents(void)
{
	int lD;
	int i;

	lD = 0x7FFFFFFF;	// large positive number
	for (i=0; i<MAXWFD; i++) if (Run.WFD[i] && Run.WFD[i]->GetLongDelim() < lD) lD = Run.WFD[i]->GetLongDelim();
	FlushEvents(lD);
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

/*	Close command connections						*/
void CloseSlaves(void)
{
	int i;
	for (i=0; i<Config.NSlaves; i++) if (Run.Slave[i].in.f) {
		if (Run.Slave[i].PID) {
			fprintf(Run.Slave[i].in.f, "q\n");
			fprintf(Run.Slave[i].in.f, "q\n");
		}
		fclose(Run.Slave[i].in.f);
		if (Run.Slave[i].out.f) fclose(Run.Slave[i].out.f);
		if (Run.Slave[i].err.f) fclose(Run.Slave[i].err.f);
	}
	sleep(1);
	for (i=0; i<Config.NSlaves; i++) 
		if (Run.Slave[i].PID && !waitpid(Run.Slave[i].PID, NULL, WNOHANG)) kill(Run.Slave[i].PID, SIGTERM);
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
		Log(TXT_ERROR "DSINK: Connection accept error: %m\n");
		return;
	}
	Log(TXT_WARN "DSINK: Too many connections dropping from %s\n", inet_ntoa(addr.sin_addr));
	close(irc);
}

/*	Save to disk all events with long token less than lToken	
	lToken = -1 - all events					*/
void FlushEvents(int lToken)
{
	int LastEvent;
	int i;
	
	if (lToken == -1) {
		for (LastEvent = 0; LastEvent < Config.MaxEvent; LastEvent++) if (!Run.Evt[LastEvent].len) break;
	} else {
		LastEvent = lToken - 128 - Run.lTokenBase;
		if (LastEvent <= 0) return;	// nothing to do
		if (LastEvent > Config.MaxEvent) {
			Log(TXT_ERROR "DSINK: Internal logic error: lToken(%d) - lTokenBase(%d) > MaxEvent(%d).\n",
				lToken, Run.lTokenBase, Config.MaxEvent);
			LastEvent = Config.MaxEvent;
		}
	}
	// Write events
	for (i=0; i<LastEvent; i++) {
		if (Run.Evt[i].len) {
			WriteEvent(Run.lTokenBase + i, &Run.Evt[i]);
			Run.Evt[i].len = 0;
		} else {
			Log(TXT_WARN "DSINK: Empty event with long token %d [arg=%d LastEvent=%d Base=%d]\n", 
				Run.lTokenBase + i, lToken, LastEvent, Run.lTokenBase);
		}
	}
	// Rotate cache
	if (LastEvent < Config.MaxEvent) {
		memcpy(Run.EvtCopy, Run.Evt, Config.MaxEvent * sizeof(struct event_struct));
		memcpy(Run.Evt, &Run.EvtCopy[LastEvent], (Config.MaxEvent - LastEvent) * sizeof(struct event_struct));
		memcpy(&Run.Evt[Config.MaxEvent - LastEvent], Run.EvtCopy, LastEvent * sizeof(struct event_struct));
	}
	Run.lTokenBase += LastEvent;
}

/*	Get Data							*/
void GetAndWrite(int num)
{
	int irc;
	struct con_struct *con;
	
	con = &Run.Con[num];
	if (con->len == 0) {	// getting length
		irc = read(con->fd, con->buf, sizeof(int));
		if (irc == 0) {
			Log(TXT_INFO "DSINK: Connection closed from %s\n", My_inet_ntoa(con->ip));
			free(con->buf);
			close(con->fd);
			con->fd = -1;
			return;			
		}
		if (irc != sizeof(int) || con->header->len < sizeof(struct rec_header_struct) || con->header->len > MBYTE) {
			Log(TXT_ERROR "DSINK: Connection closed or stream data error irc = %d  len = %d from %s %m\n", 
				irc, con->header->len, My_inet_ntoa(con->ip));
			free(con->buf);
			close(con->fd);
			con->fd = -1;
			return;
		}
		con->len = irc;
	} else {		// getting body
		irc = read(con->fd, &con->buf[con->len], con->header->len - con->len);
		if (irc <= 0) {
			Log(TXT_ERROR "DSINK: Connection closed unexpectingly or stream data error irc = %d at %s %m\n", irc, My_inet_ntoa(con->ip));
			free(con->buf);
			close(con->fd);
			con->fd = -1;
			return;
		}
		con->len += irc;
		if (con->len  == con->header->len) {
			if (con->header->ip != INADDR_LOOPBACK) {
				Log(TXT_ERROR "DSINK: Wrong data signature %X - sychronization lost ? @ %s\n", con->header->ip, My_inet_ntoa(con->ip));
				free(con->buf);
				close(con->fd);
				con->fd = -1;
				return;
			}
			con->header->ip = con->ip;
			if (con->BlkCnt >= 0 && con->header->cnt != con->BlkCnt + 1) con->ErrCnt++;
			con->BlkCnt = con->header->cnt;
			con->len = 0;
			con->cnt += con->header->len;
			ProcessData(con->buf);
			return;
		}
	}
	return;
}

/*	Get text output from slave and send it to log				*/
void GetFromSlave(char *name, struct pipe_struct *p)
{
	char c;
	c = getc(p->f);
	if (c == '\0' || c == '\n') {
		p->buf[p->wptr] = '\n';
		p->buf[p->wptr+1] = '\0';
	} else if (p->wptr == MAXSTR - 3) {
		p->buf[p->wptr] = c;
		p->buf[p->wptr+1] = '\n';
		p->buf[p->wptr+2] = '\0';		
	} else {
		p->buf[p->wptr] = c;
		p->wptr++;
		return;
	}
	Log(TXT_INFO "%s: %s", name, p->buf);
	p->wptr = 0;
}

/*	Print Help								*/
void Help(void)
{
	printf("Usage: dsink\n");
	printf("Commands:\n");
	printf("\tcmd <vme>|* <uwfdtool command> - send command to vme crate;\n");
	printf("\tfile [<file_name>] - set file to write data;\n");
	printf("\thelp - print this message;\n");
	printf("\tinfo - print statistics;\n");
	printf("\tinit - init vme modules;\n");
	printf("\tlist - list connected slaves;\n");
	printf("\tquit - Quit;\n");
	printf("\tstart/stop - start/stop data taking;\n");
	printf("To write data file example.dat:\n");
	printf("\tinit\n");
	printf("\tfile example.dat\n");
	printf("\tstart\n");
	printf("\t\t... DAQ started - wait for some data to be collected...\n");
	printf("\tstop\n");
	printf("You can omit init if you don't want system (re)initialization.\n");
}

void Info(void)
{
	int i, j;
	int *cnt;
	long BlkCnt;
	int flag;
	const char type_names[8][8] = {"SELF", "MAST", "TRIG", "RAW ", "HIST", "SYNC", "RSRV", "RSRV"};

	printf("\nSystem Initialization %s done.\n", (Run.Initialized) ? "" : TXT_BOLDRED "not" TXT_NORMAL);
	if (Run.fData && !Run.iTempData) printf("File: %s\n\t%Ld bytes / %d records: %d events + %d SelfTriggers\n", 
		Run.fDataName, ftello(Run.fData), Run.fHead.cnt, Run.RecStat[0], Run.RecStat[1]);
	printf("Modules: ");
	for (i=0; i<MAXWFD; i++) if (Run.WFD[i]) printf("%d ", i+1);
	printf("\nRecord types: ");
	for (i=0; i<8; i++) printf("%s: %d  ", type_names[i], Run.TypeStat[i]);
	BlkCnt = 0;
	flag = 0;
	for (i=0; i<MAXWFD; i++) if (Run.WFD[i]) {
		cnt = Run.WFD[i]->GetErrCnt();
		BlkCnt += Run.WFD[i]->GetBlkCnt();
		for (j=0; j<=ERR_OTHER; j++) if (cnt[j]) break;
		if (j <= ERR_OTHER) {
			if (!flag) {
				printf("Format statistics (errors):\n");
//					12345671234567890A1234567890A1234567890A1234567890A1234567890A1234567890A
				printf("Module ChanPar    SumPar     Token      Delimiter  SelfTrig   Other      Blocks\n");
				flag = 1;
			}
			printf(" %3d:  ", i+1);
			for (j=0; j<=ERR_OTHER; j++) printf((cnt[j]) ? TXT_BOLDRED "%10d " TXT_NORMAL : "%10d ", cnt[j]);
			printf("%10d\n", Run.WFD[i]->GetBlkCnt());
		}
	}
	printf("Grand total %Ld blocks received.\n", BlkCnt);
}

void Init(void)
{
	int i;
	for (i=0; i<Config.NSlaves; i++) if (Run.Slave[i].PID) SendScript(Run.Slave[i].in.f, Config.InitScript);
	Run.Initialized = 1;
}

void Log(const char *msg, ...)
{
	char str[MAXSTR];
	time_t t;
	FILE *f;
	va_list ap;

	va_start(ap, msg);
	t = time(NULL);
	strftime(str, MAXSTR,"%F %T", localtime(&t));
	f = (Run.fLog) ? Run.fLog : stdout;
	fprintf(f, str);
	vfprintf(f, msg, ap);
	va_end(ap);
	fflush(f);
}

/*	Convert int to inet address						*/
char *My_inet_ntoa(int num)
{
	struct in_addr addr;

	addr.s_addr = num;
	return inet_ntoa(addr);
}

/*	Open client data connection to the server				*/
void OpenCon(int fd, con_struct *con)
{
	struct sockaddr_in addr;
	socklen_t len;
	int irc;
	
	memset(con, 0, sizeof(con_struct));
	len = sizeof(addr);
	irc = accept(fd, (struct sockaddr *)&addr, &len);
	if (irc < 0) {
		Log(TXT_ERROR "DSINK: Connection accept error: %m\n");
		return;
	}
	con->buf = (char *)malloc(MBYTE);
	if (!con->buf) {
		Log(TXT_ERROR "DSINK: Can not allocate buffer of %d bytes: %m\n", MBYTE);
		close(irc);
		return;
	}
	con->header = (struct rec_header_struct *) con->buf;
	con->fd = irc;
	con->ip = addr.sin_addr.s_addr;
	con->BlkCnt = -1;
	Log(TXT_INFO "DSINK: connection from %s accepted\n", inet_ntoa(addr.sin_addr));
}

/*	Open file to write data							*/
void OpenDataFile(char *name)
{
	Run.fDataName[MAXSTR-1] = '\0';	
	if (Run.fData) fclose(Run.fData);
	Run.fData = NULL;
	if (!name || !name[0]) {
		Run.fDataName[0] = '\0';
		Run.iTempData = 1;
		return;
	} else if (name[0] == '/') {
		strncpy(Run.fDataName, name, MAXSTR);
		Run.iTempData = 0;
	} else {
		snprintf(Run.fDataName, MAXSTR, "%s/%s", Config.DataDir, name);
		Run.iTempData = 0;
	}
	Run.fData = fopen(Run.fDataName, (Run.iTempData) ? "w" : "a");	// we append to data files, but rewrite the default
	if (!Run.fData) {
		Log(TXT_ERROR "DSINK: Can not open data file to write: %s (%m)\n", Run.fDataName);
		Run.fDataName[0] = '\0';
		return;
	}
	Run.fHead.cnt = 0;
	Run.fHead.ip = INADDR_LOOPBACK;
	memset(Run.RecStat, 0, sizeof(Run.RecStat));
}

/*	Open log file and xterm							*/
int OpenLog(void)
{
	char cmd[MAXSTR];

	Run.fLog = fopen(Config.LogFile, "at");
	if (!Run.fLog) {
		Log(TXT_FATAL "DSINK: Can not open log-file: %s\n", Config.LogFile);
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
	int i, irc;
	pid_t pid;

	for (i=0; i<Config.NSlaves; i++) {
		if (TEMP_FAILURE_RETRY(pipe(Run.Slave[i].in.fd))) {
			Log(TXT_FATAL "DSINK: Can not create in pipe for %s: %m\n", Config.SlaveList[i]);			
			return -1;
		}
		if (TEMP_FAILURE_RETRY(pipe(Run.Slave[i].out.fd))) {
			Log(TXT_FATAL "DSINK: Can not create out pipe for %s: %m\n", Config.SlaveList[i]);			
			return -2;
		}
		if (TEMP_FAILURE_RETRY(pipe(Run.Slave[i].err.fd))) {
			Log(TXT_FATAL "DSINK: Can not create error pipe for %s: %m\n", Config.SlaveList[i]);			
			return -3;
		}
		Run.Slave[i].in.f = fdopen(Run.Slave[i].in.fd[1], "wt");
		if (!Run.Slave[i].in.f) {
			Log(TXT_FATAL "DSINK: fdopen for stdin failed for %s: %m\n", Config.SlaveList[i]);
			return -30;
		}
		Run.Slave[i].out.f = fdopen(Run.Slave[i].out.fd[0], "rt");
		if (!Run.Slave[i].out.f) {
			Log(TXT_FATAL "DSINK: fdopen for stdout failed for %s: %m\n", Config.SlaveList[i]);
			return -31;
		}
		Run.Slave[i].err.f = fdopen(Run.Slave[i].err.fd[0], "rt");
		if (!Run.Slave[i].err.f) {
			Log(TXT_FATAL "DSINK: fdopen for stderr failed for %s: %m\n", Config.SlaveList[i]);
			return -32;
		}
		if (setvbuf(Run.Slave[i].out.f, NULL, _IONBF, 0)) {
			Log(TXT_FATAL "DSINK: stdout setting no buffering mode failed for %s: %m\n", Config.SlaveList[i]);
			return -33;			
		}
		if (setvbuf(Run.Slave[i].err.f, NULL, _IONBF, 0)) {
			Log(TXT_FATAL "DSINK: stderr setting no buffering mode failed for %s: %m\n", Config.SlaveList[i]);
			return -34;			
		}
		pid = fork();
		if ((int) pid < 0) {		// error
			Log(TXT_FATAL "DSINK: Can not fork for %s: %m\n", Config.SlaveList[i]);			
			return -10;
		} else if ((int) pid == 0) {	// child process
			dup2(Run.Slave[i].in.fd[0], STDIN_FILENO);
			TEMP_FAILURE_RETRY(close(Run.Slave[i].in.fd[0]));
			TEMP_FAILURE_RETRY(close(Run.Slave[i].in.fd[1]));
			dup2(Run.Slave[i].out.fd[1], STDOUT_FILENO);
			TEMP_FAILURE_RETRY(close(Run.Slave[i].out.fd[0]));
			TEMP_FAILURE_RETRY(close(Run.Slave[i].out.fd[1]));
			dup2(Run.Slave[i].err.fd[1], STDERR_FILENO);
			TEMP_FAILURE_RETRY(close(Run.Slave[i].err.fd[0]));
			TEMP_FAILURE_RETRY(close(Run.Slave[i].err.fd[1]));
			execl(SSH, SSH, "-x", Config.SlaveList[i], Config.SlaveCMD, NULL);
			Log(TXT_FATAL "DSINK: Can not do ssh %s: %s (%m)\n", Config.SlaveList[i], Config.SlaveCMD);	// we shouldn't get here after execl
			exit(-20);
		} else {			// main process
			Run.Slave[i].PID = pid;
			TEMP_FAILURE_RETRY(close(Run.Slave[i].in.fd[0]));
			TEMP_FAILURE_RETRY(close(Run.Slave[i].out.fd[1]));
			TEMP_FAILURE_RETRY(close(Run.Slave[i].err.fd[1]));
		}
	}
	return 0;
}

/*	Create socket for UDP write						*/
void OpenUDP(void)
{
	struct hostent *host;
	struct sockaddr_in hostname;

	Run.fdUDP = -1;
	host = gethostbyname(Config.UDPHost);
	if (!host) {
		Log(TXT_ERROR "DSINK: Host %s not found (UDP server).\n", Config.UDPHost);
		return;
	}

	hostname.sin_family = AF_INET;
	hostname.sin_port = htons(Config.UDPPort);
	hostname.sin_addr = *(struct in_addr *) host->h_addr;

	Run.fdUDP = socket(PF_INET, SOCK_DGRAM, 0);
	if (Run.fdUDP < 0) {
		Log(TXT_ERROR "DSINK: Can not create socket %s:%d - %m\n", Config.UDPHost, Config.UDPPort);
		return;
	}

	if (connect(Run.fdUDP, (struct sockaddr *)&hostname, sizeof(hostname))) {
		Log(TXT_ERROR "Setting default UDP destination to %s:%d failed %m\n", Config.UDPHost, Config.UDPPort);
		close(Run.fdUDP);
		Run.fdUDP = -1;
		return;
	}
}

/*	Process data received							*/
void ProcessData(char *buf)
{
	struct rec_header_struct *header;
	struct blkinfo_struct *info;
	int num;
	
	header = (struct rec_header_struct *)buf;
	if ((header->type & 0xFFFF0000) != REC_WFDDATA) return;	// we ignore other records here
	num = header->type & REC_SERIALMASK;
	if (num == 0 || num >= MAXWFD) {
		Log(TXT_WARN "Out of range module serial number %d met (1-%d)\n", num, MAXWFD);
		return;
	}
	if (!Run.WFD[num-1]) return;	// we ignore not configured modules
	try {
		Run.WFD[num-1]->Add(buf, header->len);		// store data
		for (;;) {					// distribute data over events
			info = Run.WFD[num-1]->Get();
			if (!info) break;
			if (info->type == TYPE_SELF) {
				WriteSelfTrig(num, info);
			} else {
				Add2Event(num, info);
				if (info->type == TYPE_DELIM) CheckReadyEvents();
			}
			Run.TypeStat[info->type & 7]++;
		}
	} catch (const int irc) {
		Log(TXT_FATAL "Exception %d signalled.\n", irc);
		Run.iStop = 1;
	};
}

/*	Read configuration file							*/
int ReadConf(char *fname)
{
	config_t cnf;
	int tmp, i;
	char *stmp;
	char *tok;
	config_setting_t *ptr;

	memset(&Config, 0, sizeof(Config));
	config_init(&cnf);
	if (config_read_file(&cnf, fname) != CONFIG_TRUE) {
        	Log(TXT_FATAL "DSINK: Configuration error in file %s at line %d: %s\n", fname, config_error_line(&cnf), config_error_text(&cnf));
            	return -10;
    	}
//	int Port;			// Data port 0xA336
	Config.Port = (config_lookup_int(&cnf, "Sink.Port", &tmp)) ? tmp : 0xA336;
//	int UDPPort;			// UDP port 0xA230
	Config.UDPPort = (config_lookup_int(&cnf, "Sink.UDPPort", &tmp)) ? tmp : 0xA230;
//	char MyName[MAXSTR];		// The server host name
	strncpy(Config.MyName, (config_lookup_string(&cnf, "Sink.MyName", (const char **) &stmp)) ? stmp : "dserver.danss.local", MAXSTR);
//	char UDPHost[MAXSTR];		// host to send UDP data
	strncpy(Config.UDPHost, (config_lookup_string(&cnf, "Sink.UDPHost", (const char **) &stmp)) ? stmp : "dserver.danss.local", MAXSTR);
//	char SlaveList[MAXCON][MAXSTR];	// Crate host names list
	if (!config_lookup_string(&cnf, "Sink.SlaveList", (const char **) &stmp)) 
		stmp = (char *)"vme01.danss.local vme02.danss.local vme03.danss.local vme04.danss.local";
	tok = strtok(stmp, " \t,");
	for (i=0; i<MAXCON; i++) {
		if (!tok || !tok[0]) break;
		strncpy(Config.SlaveList[i], tok, MAXSTR);
		tok = strtok(NULL, " \t,");
	}
	Config.NSlaves = i;
	if (!Config.NSlaves) {
		Log(TXT_FATAL "DSINK: Bad configuration %s - no VME crates defined.\n", fname);
		return -11;
	} 
//	char SlaveCMD[MAXSTR];		// Command to start slave
	strncpy(Config.SlaveCMD, (config_lookup_string(&cnf, "Sink.SlaveCMD", (const char **) &stmp)) ? stmp : (char *)"cd bin;./uwfdtool", MAXSTR);
//	char TriggerMaster[MAXSTR];	// Trigger master module and crate (index in slave list)
	if (!config_lookup_string(&cnf, "Sink.TriggerMaster", (const char **) &stmp)) stmp = (char *)"0:1";
	tok = strtok(stmp, " \t,:");
	Config.TriggerMasterCrate = strtol(tok, NULL, 0);
	if (Config.TriggerMasterCrate < 0 || Config.TriggerMasterCrate >= Config.NSlaves) {
		Log(TXT_FATAL "DSINK: Config.TriggerMasterCrate = %d out of range in file %s\n", Config.TriggerMasterCrate, fname);
		return -12;
	}
	tok = strtok(NULL, " \t,:");
	Config.TriggerMasterModule = (tok) ? strtol(tok, NULL, 0) : 1;
//	char LogFile[MAXSTR];		// dsink log file name
	strncpy(Config.LogFile, (config_lookup_string(&cnf, "Sink.LogFile", (const char **) &stmp)) ? stmp : "dsink.log", MAXSTR);
//	char LogTermCMD[MAXSTR];	// start log view in a separate window
	strncpy(Config.LogTermCMD, (config_lookup_string(&cnf, "Sink.LogTermCMD", 
		(const char **) &stmp)) ? stmp : "xterm -geometry 240x23 -title DSINK_Log -e tail -f dsink.log", MAXSTR);
//	char DataDir[MAXSTR];		// directory to write data
	strncpy(Config.DataDir, (config_lookup_string(&cnf, "Sink.DataDir", (const char **) &stmp)) ? stmp : "data", MAXSTR);
//	char InitScript[MAXSTR];	// initialize modules
	strncpy(Config.InitScript, (config_lookup_string(&cnf, "Sink.InitScript", 
		(const char **) &stmp)) ? stmp : "p * main.bin;w 500;i general.conf", MAXSTR);
//	char StartScript[MAXSTR];	// put vme into acquire mode. Agruments: server, port
	strncpy(Config.StartScript, (config_lookup_string(&cnf, "Sink.StartScript", (const char **) &stmp)) ? stmp : "y * * %s:%d", MAXSTR);
//	char StopScript[MAXSTR];	// stop acquire mode
	strncpy(Config.StopScript, (config_lookup_string(&cnf, "Sink.StopScript", (const char **) &stmp)) ? stmp : "q", MAXSTR);
//	char InhibitScript[MAXSTR];
	strncpy(Config.InhibitScript, (config_lookup_string(&cnf, "Sink.InhibitScript", (const char **) &stmp)) ? stmp : "m %d 1", MAXSTR);
//	char EnableScript[MAXSTR];
	strncpy(Config.EnableScript, (config_lookup_string(&cnf, "Sink.EnableScript", (const char **) &stmp)) ? stmp : "z %d;m %d 0", MAXSTR);
//	Dmodule *WFD[MAXWFD];		// class WFD modules for data processing
	ptr = config_lookup(&cnf, "ModuleList");
	if (!ptr) {
		Log(TXT_FATAL "DSINK: No modules defined in %s. ModuleList section must be present.\n", fname);
		return -13;
	}
	for (i=0; i<MAXWFD;i++) {
		tmp = config_setting_get_int_elem(ptr, i);
		if (tmp <= 0) break;
		if (tmp >= MAXWFD) {
			Log(TXT_FATAL "DSINK: Module serial %d out of the range (1-%d).\n", tmp, MAXWFD);
			return -14;			
		}
		try {
			Run.WFD[tmp-1] = new Dmodule(tmp);
		}
		catch (...) {
			return -15;
		}
	}
	if (!i) {
		Log(TXT_FATAL "DSINK: ModuleList section is empty in %s.\n", fname);
		return -16;
	}
//	int MaxEvent;			// Number of simultaneously allowed events in the builder 
	Config.MaxEvent = (config_lookup_int(&cnf, "Sink.MaxEvent", &tmp)) ? tmp : 1024;
	Run.Evt = (struct event_struct *) malloc(Config.MaxEvent * sizeof(struct event_struct));
	Run.EvtCopy = (struct event_struct *) malloc(Config.MaxEvent * sizeof(struct event_struct));
	if (!Run.Evt || !Run.EvtCopy) {
		Log(TXT_FATAL "DSINK: Not enough memory for Event cache %m.\n");
		return -17;
	}
	memset(Run.Evt, 0, Config.MaxEvent * sizeof(struct event_struct));
	return 0;
}

/*	Send a script divided by semicolons as separate lines to f		*/
void SendScript(FILE *f, const char *script)
{
	char copy[MAXSTR];
	char *tok;

	strncpy(copy, script, MAXSTR);
	tok = strtok(copy, ";");

	for (;;) {
		if (!tok || !tok[0]) break;
		fprintf(f, "%s\n", tok);
//		printf("\nSendScript: %s\n", tok);
		fflush(f);
		tok = strtok(NULL, ";");
	}
}

/*	Spawn a new process and return its PID					*/
pid_t StartProcess(char *cmd)
{
	pid_t pid;
	
	pid = fork();
	if (!pid) {
		execl(SHELL, SHELL, "-c", cmd, NULL);
		Log(TXT_FATAL "DSINK: Wrong return\n");
		return -20;
	} else if (pid < 0) {
		Log(TXT_FATAL "DSINK: Can not execute %s\n", cmd);
		return pid;
	}
	return pid;
}

/*	start DAQ								*/
void StartRun(void)
{
	int i;
	char str[MAXSTR];

	for (i=0; i<Config.NSlaves; i++) if (!Run.Slave[i].PID) break;
	if (i != Config.NSlaves) {
		Log(TXT_ERROR "Not all VME crates are attached. Can not start.\n");
		return;	
	}
	snprintf(str, MAXSTR, Config.InhibitScript, Config.TriggerMasterModule);
	SendScript(Run.Slave[Config.TriggerMasterCrate].in.f, str);
	snprintf(str, MAXSTR, Config.StartScript, Config.MyName, Config.Port);
	for (i=0; i<Config.NSlaves; i++) SendScript(Run.Slave[i].in.f, str);
	sleep(1);
	snprintf(str, MAXSTR, Config.EnableScript, Config.TriggerMasterModule, Config.TriggerMasterModule);
	SendScript(Run.Slave[Config.TriggerMasterCrate].in.f, str);
	for (i=0; i<MAXWFD; i++) if (Run.WFD[i]) {
		Run.WFD[i]->ClearParity();
		Run.WFD[i]->ClearCounters();
	}
	Run.lTokenBase = 0;
	Run.iRun = 1;
	memset(Run.TypeStat, 0, sizeof(Run.TypeStat));
}

/*	Stop DAQ								*/
void StopRun(void)
{
	int i;
	char str[MAXSTR];

	snprintf(str, MAXSTR, Config.InhibitScript, Config.TriggerMasterModule);
	if (Run.Slave[Config.TriggerMasterCrate].PID) SendScript(Run.Slave[Config.TriggerMasterCrate].in.f, str);
	sleep(1);
	for (i=0; i<Config.NSlaves; i++) if (Run.Slave[i].PID) SendScript(Run.Slave[i].in.f, "q");
	Run.iRun = 0;
}

/*	Write and Send data from run.wData. Header MUST correspond to Run.fHead	*/
void WriteAndSend(void)
{
	int irc;

	if (Run.fData) {
		irc = fwrite(Run.wData, Run.fHead.len, 1, Run.fData);
		if (irc != 1) {
			Log(TXT_ERROR "DSINK: Write data file %s failed: %m\n", Run.fDataName);
			fclose(Run.fData);
			Run.fData = NULL;
		}
	}
	if (Run.fdUDP >= 0) TEMP_FAILURE_RETRY(write(Run.fdUDP, Run.wData, Run.fHead.len));
	Run.fHead.cnt++;
}

/*	Write regular events to the file					*/
void WriteEvent(int lToken, struct event_struct *event)
{
	int len;
	void *ptr;
//		Check that we have enough space
	len = sizeof(struct rec_header_struct) + event->len;
	if (Run.wDataSize < len) {
		ptr = realloc(Run.wData, len);
		if (!ptr) {
			Log(TXT_ERROR "DSINK: Memory allocation failure of %d bytes in WriteEvent %m.\n", len);
			return;
		}
		Run.wData = ptr;
		Run.wDataSize = len;
	}
//		make the block
	Run.fHead.len = len;
	Run.fHead.type = REC_EVENT | (lToken & REC_EVTCNTMASK);
	Run.fHead.time = time(NULL);
	memcpy(Run.wData, &Run.fHead, sizeof(struct rec_header_struct));
	memcpy((char *)Run.wData + sizeof(struct rec_header_struct), event->data, event->len);

	Run.RecStat[0]++;
	WriteAndSend();
}

/*	Write SelfTrig data to the file								*/
void WriteSelfTrig(int num, struct blkinfo_struct *info)
{
	int len;
	void *ptr;
//		Check that we have enough space
	len = sizeof(struct rec_header_struct) + (info->data[0] & 0x1FF) * sizeof(short);
	if (Run.wDataSize < len) {
		ptr = realloc(Run.wData, len);
		if (!ptr) {
			Log(TXT_ERROR "DSINK: Memory allocation failure of %d bytes in WriteSelfTrig %m.\n", len);
			return;
		}
		Run.wData = ptr;
		Run.wDataSize = len;
	}
//		Make the block
	Run.fHead.len = len;
	Run.fHead.type = REC_SELFTRIG + (num & REC_SERIALMASK) + ((((int)info->data[0]) << 7) & REC_CHANMASK);
	Run.fHead.time = time(NULL);
	memcpy(Run.wData, &Run.fHead, sizeof(struct rec_header_struct));
	memcpy((char *)Run.wData + sizeof(struct rec_header_struct), info->data, len - sizeof(struct rec_header_struct));

	Run.RecStat[1]++;
	WriteAndSend();
}

/*	Process commands							*/
static void ProcessCmd(char *cmd)
{
	char *tok;
	const char DELIM[] = " \t:,";
	char copy[MAXSTR];
	int i, num;

	if (!cmd || !cmd[0]) return;
	add_history(cmd);
	strncpy(copy, cmd, MAXSTR);
	tok = strtok(cmd, DELIM);
	if (!tok || !tok[0]) return;
	
	if (!strcasecmp(tok, "cmd")) {	// Send command to slave(s)
		tok = strtok(NULL, DELIM);
		if (!tok || !tok[0]) {
			printf("Not enough arguments: %s\n", copy);
			Help();
			return;
		}
		num = (tok[0] == '*') ? -1 : strtol(tok, NULL, 0);
		if (num < -1 || num >= Config.NSlaves) {
			printf("Wrong crate number in the list: %d\n", num);
			return;
		}
		tok = strtok(NULL, DELIM);
		if (!tok || !tok[0]) {
			printf("Not enough arguments: %s\n", copy);
			Help();
			return;
		}
		if (num < 0) {
			for (i=0; i<Config.NSlaves; i++) if (Run.Slave[i].PID) SendScript(Run.Slave[i].in.f, &copy[tok - cmd]);
		} else {
			if (Run.Slave[num].PID) SendScript(Run.Slave[num].in.f, &copy[tok - cmd]);
		}
	} else if (!strcasecmp(tok, "file")) {	// Open file to write data
		tok = strtok(NULL, DELIM);
		OpenDataFile((tok && tok[0]) ? tok : NULL);
	} else if (!strcasecmp(tok, "help")) {	// Print help
		Help();
	} else if (!strcasecmp(tok, "info")) {	// Print statistics
		Info();
	} else if (!strcasecmp(tok, "init")) {	// Initialize DAQ
		if (Run.iRun) StopRun();
		Init();
	} else if (!strcasecmp(tok, "list")) {	// List slaves
		printf("Configured crates:\n");
		for (i=0; i<Config.NSlaves; i++) printf("%2d\t%s\t%s\n", i, Config.SlaveList[i], 
			(Run.Slave[i].PID) ? TXT_BOLDGREEN "OK" TXT_NORMAL : TXT_BOLDRED "Disconnected" TXT_NORMAL);
		printf("Attached connections:\n");
		for (i=0; i<Run.NCon; i++) printf("%2d\t%s\t%16Ld bytes %10d blocks %8d errors\n", 
			i, My_inet_ntoa(Run.Con[i].ip), Run.Con[i].cnt, Run.Con[i].BlkCnt, Run.Con[i].ErrCnt);
	} else if (!strcasecmp(tok, "quit")) {	// Quit
		Run.iStop = 1;
	} else if (!strcasecmp(tok, "start")) {	// Start DAQ
		if (!Run.Initialized) Init();
		if (!Run.iRun) StartRun();
	} else if (!strcasecmp(tok, "stop")) {	// Stop DAQ
		if (Run.iRun) StopRun();
	} else {				// Unknown command
		printf("Unknown command: %s\n\n", tok);
		Help();
	}
	for (i=0; i<Config.NSlaves; i++) fflush(Run.Slave[i].in.f);
}

/*	The main								*/
int main(int argc, char **argv)
{
	fd_set set;
	struct timeval tm;
	int i, irc;
	
	memset(&Run, 0, sizeof(Run));
	read_history(".dsink_history");

	if (ReadConf((char *)DEFCONFIG)) goto MyExit;
	if (OpenLog()) goto MyExit;
	OpenUDP();

	Log(TXT_INFO "DSINK started.\n");

	Run.fdPort = BindPort();
	if (Run.fdPort < 0) goto MyExit;	

	if (OpenSlaves()) goto MyExit;

	rl_callback_handler_install("DSINK > ", ProcessCmd);	


//		Event loop
	while (!Run.iStop) {

		tm.tv_sec = 2;		// 2 s
		tm.tv_usec = 0;
		FD_ZERO(&set);
		FD_SET(fileno(rl_instream), &set);
		FD_SET(Run.fdPort, &set);
		for (i=0; i<Run.NCon; i++) FD_SET(Run.Con[i].fd, &set);
		for (i=0; i<Config.NSlaves; i++) if (Run.Slave[i].PID) FD_SET(Run.Slave[i].out.fd[0], &set);
		for (i=0; i<Config.NSlaves; i++) if (Run.Slave[i].PID) FD_SET(Run.Slave[i].err.fd[0], &set);

		irc = select(FD_SETSIZE, &set, NULL, NULL, &tm);
		if (irc < 0) continue;

		for (i=0; i<Config.NSlaves; i++) if (Run.Slave[i].PID && waitpid(Run.Slave[i].PID, NULL, WNOHANG)) {
			Log(TXT_ERROR "DSINK: no/lost connection to %s\n", Config.SlaveList[i]);
			Run.Slave[i].PID = 0;
		}

		if (irc) {
			if (FD_ISSET(fileno(rl_instream), &set)) rl_callback_read_char();
			if (FD_ISSET(Run.fdPort, &set)) {
				if (Run.NCon < MAXCON) {
					OpenCon(Run.fdPort, &Run.Con[Run.NCon]);
					if (Run.Con[Run.NCon].fd > 0) Run.NCon++;
				} else {
					DropCon(Run.fdPort);
				}
			}
			for (i=0; i<Run.NCon; i++) if (FD_ISSET(Run.Con[i].fd, &set)) GetAndWrite(i);
			for (i=0; i<Config.NSlaves; i++) if (Run.Slave[i].PID && FD_ISSET(Run.Slave[i].out.fd[0], &set)) 
				GetFromSlave(Config.SlaveList[i], &Run.Slave[i].out);
			for (i=0; i<Config.NSlaves; i++) if (Run.Slave[i].PID && FD_ISSET(Run.Slave[i].err.fd[0], &set)) 
				GetFromSlave(Config.SlaveList[i], &Run.Slave[i].err);
			CleanCon();
		} else {
			FlushEvents(-1);
		}
	}
MyExit:
	Log(TXT_INFO "DSINK: Exit.\n");
	printf(" Good bye.\n");
	for (i=0; i<Run.NCon; i++) {
		free(Run.Con[i].buf);
		close(Run.Con[i].fd);
	}

	CloseSlaves();
	if (Run.fdPort)	close(Run.fdPort);
	FlushEvents(-1);
	if (Run.Evt) {
		for (i=0; i<Config.MaxEvent; i++) if (Run.Evt[i].data) free(Run.Evt[i].data);
		free(Run.Evt);
	}
	if (Run.EvtCopy) free(Run.EvtCopy);
	if (Run.fData) fclose(Run.fData);
	if (Run.wData) free(Run.wData);
	if (Run.fdUDP) close(Run.fdUDP);
	sleep(2);
	if (Run.fLog) {
		fclose(Run.fLog);
		kill(Run.fLogPID, SIGINT);
	}
	rl_callback_handler_remove ();
	write_history(".dsink_history");	
	return 0;
}

