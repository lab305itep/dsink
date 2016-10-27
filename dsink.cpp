/*
	Simple data sink code for dserver
	SvirLex, ITEP, 2016
*/
#define _FILE_OFFSET_BITS 64
#include <arpa/inet.h>
#include <ctype.h>
#include <errno.h>
#include <fcntl.h>
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
	int InPort;			// Data port 0xA336
	int OutPort;			// Out port  0xB230
	char MyName[MAXSTR];		// The server host name
	char SlaveList[MAXCON][MAXSTR];	// Crate host names list
	int NSlaves;			// number of crates
	char SlaveCMD[MAXSTR];		// Command to start slave
	int TriggerMasterCrate;		// Trigger master crate (index in slave list)
	int TriggerMasterModule;	// Trigger master module
	int VetoMasterCrate;		// Veto master crate (index in slave list)
	int VetoMasterModule;		// Veto master module
	char LogFile[MAXSTR];		// dsink log file name
	char LogTermCMD[MAXSTR];	// start log view in a separate window
	char XilinxFirmware[MAXSTR];	// Xilinx firmware
//	char InitScript[MAXSTR];	// initialize modules
//	char StartScript[MAXSTR];	// put vme into acquire mode. Agruments: server, port
//	char StopScript[MAXSTR];	// stop acquire mode
//	char InhibitScript[MAXSTR];	// Inhibit triggers
//	char EnableScript[MAXSTR];	// Enables triggers
	int MaxEvent;			// Size of Event cache
	char CheckDiskScript[MAXSTR];	// the script is called before new file in auto mode is written
	char AutoName[MAXSTR];		// auto file name format
	int AutoTime;			// half an hour
	int AutoSize;			// in MBytes (2^20 bytes)
	char ConfSavePattern[MAXSTR];	// pattern to copy configuration when dsink reads it
	char LogSavePattern[MAXSTR];	// Pattern to rename the old log file before compression
	int PeriodicTriggerPeriod;	// Period of the pulser trigger, ms. 0 - disabled, Maximum - 2^13-1.
} Config;

struct run_struct {
	FILE *fLog;			// Log file
	pid_t fLogPID;			// Log file broser PID
	FILE *fData;			// Data file
	char fDataName[MAXSTR];		// Data file name
	struct rec_header_struct fHead;	// record header to write data file
	void *wData;			// mememory for write data
	int wDataSize;			// size of wData 
	int fdPort;			// Port for input data connections
	struct slave_struct Slave[MAXCON];	// Slave VME connections
	con_struct Con[MAXCON];		// Data connections
	int NCon;			// Number of data connections
	int fdOut;			// port for output data connections
	struct client_struct Client[MAXCON];	// output data clients
	int iStop;			// Quit flag
	int iRun;			// DAQ running flag
	int iAuto;			// Auto change file mode
	int Initialized;		// VME modules initialized
	Dmodule *WFD[MAXWFD];		// class WFD modules for data processing
	struct event_struct *Evt;	// Pointer to event cache
	struct event_struct *EvtCopy;	// Pointer to event cache copy (for rotation)
	int lTokenBase;			// long token of the first even in the cache
	int TypeStat[8];		// record types statistics
	int RecStat[2];			// events/selftriggers in the output file
	int LastFileStarted;		// time() when the last file was started
	long long FileCounter;		// bytes to the last file
	int RestartFlag;		// flag restart
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
		Run.RestartFlag = 1;
		return;
	}
	// Check memory
	new_len = ((info->data[0] & 0x1FF) + 1) * sizeof(short);
	if (new_len + Run.Evt[k].len > Run.Evt[k].size) {
		ptr = (char *)realloc(Run.Evt[k].data, Run.Evt[k].size + MCHUNK);
		if (!ptr) {
			Log(TXT_FATAL "DSINK: Out of memory: %m\n");
			Run.iStop = 1;
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
int BindPort(int port)
{
	int fd, i, irc;
	struct sockaddr_in name;
	
	fd = socket (PF_INET, SOCK_STREAM, 0);
	if (fd < 0) {
		printf(TXT_FATAL "DSINK: Can not create socket.: %m\n");
		return fd;
	}

	i = 1;
	irc = setsockopt(fd, SOL_SOCKET, SO_KEEPALIVE, &i, sizeof(i));
	if (irc) {
		Log(TXT_ERROR "DSINK: setsockopt error: %m\n");
		close(fd);
		return -1;
	}

	name.sin_family = AF_INET;
	name.sin_port = htons(port);
	name.sin_addr.s_addr = htonl(INADDR_ANY);
	if (bind(fd, (struct sockaddr *)&name, sizeof(name)) < 0) {
		Log(TXT_FATAL "DSINK: Can not bind to port %d: %m\n", port);
		close(fd);
		return -1;
	}
	
	if (listen(fd, MAXCON) < 0) {
		Log(TXT_FATAL "DSINK: Can not listen to port %d: %m\n", port);
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

/*	Push data for client to FIFO. Do nothing if no space left.	*/
void ClientPush(struct client_struct *client, char *data, int len)
{
	int flen;

	flen = client->rptr - client->wptr;
	if (flen <= 0) flen += FIFOSIZE;
	if (flen <= len) return;	// no room left

	flen = (len <= FIFOSIZE - client->wptr) ? len : FIFOSIZE - client->wptr;
	memcpy(&client->fifo[client->wptr], data, flen);
	if (flen < len) memcpy(client->fifo, &data[flen], len - flen);
	client->wptr += len;
	if (client->wptr >= FIFOSIZE) client->wptr -= FIFOSIZE;
}

/*	Send data from fifo to TCP client			*/
void ClientSend(struct client_struct *client)
{
	int flen, irc;
	
	flen = (client->wptr > client->rptr) ? client->wptr - client->rptr : FIFOSIZE - client->rptr;
	irc = write(client->fd, &client->fifo[client->rptr], flen);
	if (irc < 0 && errno != EAGAIN && errno != EWOULDBLOCK) {
		Log(TXT_WARN "DSINK: Client %s send error: %m\n", My_inet_ntoa(client->ip));
		shutdown(client->fd, 2);
		free(client->fifo);
		client->fd = 0;
	} else {
		client->rptr += irc;
		if (client->rptr == FIFOSIZE) client->rptr = 0;
	}
}

/*	Close Data File						*/
void CloseDataFile(void)
{
	char cmd[MAXSTR];

	if (!Run.fData) return;
	fclose(Run.fData);
	snprintf(cmd, MAXSTR, "echo %s >> %s", Run.fDataName, DATA_LOG);
	if (system(cmd)) Log(TXT_ERROR "DSINK: Can not write to " DATA_LOG);
	Log(TXT_INFO "DSINK: File %s closed. %d events, %d SelfTriggers, %Ld bytes in %d s.\n",
		Run.fDataName, Run.RecStat[0], Run.RecStat[1], Run.FileCounter, time(NULL) - Run.LastFileStarted);
	Run.fData = NULL;
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

/*	Select get events once							*/
void DoSelect(int AcceptCommands)
{
	fd_set set;
	fd_set wset;
	struct timeval tm;
	int i, irc;
	
	tm.tv_sec = 2;		// 2 s
	tm.tv_usec = 0;
	FD_ZERO(&set);
	if (AcceptCommands) FD_SET(fileno(rl_instream), &set);
	FD_SET(Run.fdPort, &set);
	FD_SET(Run.fdOut, &set);
	for (i=0; i<Run.NCon; i++) FD_SET(Run.Con[i].fd, &set);
	for (i=0; i<Config.NSlaves; i++) if (Run.Slave[i].PID) FD_SET(Run.Slave[i].out.fd[0], &set);
	for (i=0; i<Config.NSlaves; i++) if (Run.Slave[i].PID) FD_SET(Run.Slave[i].err.fd[0], &set);

	FD_ZERO(&wset);
	for (i=0; i<MAXCON; i++) if (Run.Client[i].fd && Run.Client[i].rptr != Run.Client[i].wptr) FD_SET(Run.Client[i].fd, &wset);

	irc = select(FD_SETSIZE, &set, &wset, NULL, &tm);
	if (irc < 0) return;

	for (i=0; i<Config.NSlaves; i++) if (Run.Slave[i].PID && waitpid(Run.Slave[i].PID, NULL, WNOHANG)) {
		Log(TXT_ERROR "DSINK: no/lost connection to %s\n", Config.SlaveList[i]);
		Run.Slave[i].PID = 0;
	}

	if (irc) {
		if (AcceptCommands) if (FD_ISSET(fileno(rl_instream), &set)) rl_callback_read_char();
		if (FD_ISSET(Run.fdPort, &set)) {
			if (Run.NCon < MAXCON) {
				OpenCon(Run.fdPort, &Run.Con[Run.NCon]);
				if (Run.Con[Run.NCon].fd > 0) Run.NCon++;
			} else {
				DropCon(Run.fdPort);
			}
		}
		if (FD_ISSET(Run.fdOut, &set) && OpenClient() > 0) DropCon(Run.fdOut);
		for (i=0; i<Run.NCon; i++) if (FD_ISSET(Run.Con[i].fd, &set)) GetAndWrite(i);
		for (i=0; i<Config.NSlaves; i++) if (Run.Slave[i].PID && FD_ISSET(Run.Slave[i].out.fd[0], &set)) 
			GetFromSlave(Config.SlaveList[i], &Run.Slave[i].out, &Run.Slave[i]);
		for (i=0; i<Config.NSlaves; i++) if (Run.Slave[i].PID && FD_ISSET(Run.Slave[i].err.fd[0], &set)) 
			GetFromSlave(Config.SlaveList[i], &Run.Slave[i].err, &Run.Slave[i]);
		CleanCon();
		for (i=0; i<MAXCON; i++) if (Run.Client[i].fd && FD_ISSET(Run.Client[i].fd, &wset)) ClientSend(&Run.Client[i]);
	} else {
		FlushEvents(-1);
	}
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
void GetFromSlave(char *name, struct pipe_struct *p, struct slave_struct *slave)
{
	char c;
	int irc;
	
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
	if (p->buf[0] == '_' && p->buf[1] == '_') {
		irc = strtol(&p->buf[2], NULL, 0);
		slave->IsWaiting = 0;
		slave->LastResponse = irc;
		if (irc) {
			Log(TXT_ERROR "%s: The last command returned an error.\n", name);
			slave->CommandFifo[0] = '\0';
		} else {
			SendFromFifo(slave);
		}
	} else {
		Log(TXT_INFO "%s: %s", name, p->buf);
	}
	p->wptr = 0;
}

/*	Read log file and find the next auto file number. 
	Also call the script to check/switch disk.				*/
int GetNextAutoNumber(void)
{
	FILE *f;
	char str[MAXSTR];
	int irc;
	int num;
	char *ptr;

	//	Get the last line from the data log
	f = popen("[ -f " DATA_LOG " ] && tail -1 " DATA_LOG, "r");
	if (!f) {
		Log(TXT_ERROR "DSINK: Internal error - can not get info from the data log\n");
		return -1;
	}
	irc = fread(str, 1, MAXSTR-1, f);
	str[irc] = '\0';
	fclose(f);
	//	Get file number - the first number in the file name
	for (ptr = str; ptr[0]; ptr++) if (isdigit(ptr[0])) break;
	if (ptr[0]) {
		num = strtol(ptr, NULL, 10);
	} else {
		num = 0;
	}
	num++;
	//	Make a file
	for (;;num++) {
		strcpy(str, DATA_DIR);
		snprintf(&str[strlen(DATA_DIR)], MAXSTR - strlen(DATA_DIR), Config.AutoName, num);
	//	Check if exists
		f = fopen(str, "rb");
		if (!f) break; 
		fclose(f);
	}
	//	Call disk check/switch script
	if (Config.CheckDiskScript[0] && system(Config.CheckDiskScript)) {
		Log(TXT_ERROR "DSINK: disk check error. No Space left ?\n");
		return -1;
	}
	return num;
}

/*	Print Help								*/
void Help(void)
{
	printf("Usage: dsink [-a] [-c config.conf] [-h]. Options:\n");
	printf("-a - auto start data taking;\n");
	printf("-c config.conf - use configuration file config,conf;\n");
	printf("-h - print this message and exit.\n");
	printf("Commands:\n");
	printf("\tcmd <vme>|* <uwfdtool command> - send command to vme crate;\n");
	printf("\tfile [<file_name>] - set file to write data;\n");
	printf("\thelp - print this message;\n");
	printf("\tinfo - print statistics;\n");
	printf("\tinit - init vme modules;\n");
	printf("\tlist - list connected slaves;\n");
	printf("\tpause - set inhibit;\n");
	printf("\tquit - Quit;\n");
	printf("\tresume - clear inhibit;\n");
	printf("\tstart/stop - start/stop data taking;\n");
	printf("The simplest way to write data in automatic mode is:\n");
	printf("\tstart\n");
	printf("\tfile auto\n");
	printf("\t\t... DAQ started - files and disks will be changed automatically\n");
}

void Info(void)
{
	int i, j;
	int *cnt;
	long long BlkCnt;
	int flag;
	const char type_names[8][8] = {"SELF", "MAST", "TRIG", "RAW ", "HIST", "SYNC", "RSRV", "RSRV"};

	printf("System Initialization %s done.\n", (Run.Initialized) ? "" : TXT_BOLDRED "not" TXT_NORMAL);
	if (Run.fData) printf("File: %s: %Ld bytes / %d records: %d events + %d SelfTriggers / %d s\n", 
		Run.fDataName, ftello(Run.fData), Run.fHead.cnt, Run.RecStat[0], Run.RecStat[1], time(NULL) - Run.LastFileStarted);
	printf("Modules: ");
	for (i=0; i<MAXWFD; i++) if (Run.WFD[i]) printf("%d ", i+1);
	printf("\nRecord types: ");
	for (i=0; i<8; i++) printf("%s: %d  ", type_names[i], Run.TypeStat[i]);
	printf("\n");
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
	char cmd[MAXSTR];
	int i;

	Run.Initialized = 0;
	snprintf(cmd, MAXSTR, "p * %s;?", Config.XilinxFirmware);
	for (i=0; i<Config.NSlaves; i++) if (Run.Slave[i].PID) SendScript(&Run.Slave[i], cmd);
	for (i=0; i<Config.NSlaves; i++) while (Run.Slave[i].IsWaiting) DoSelect(0);
	for (i=0; i<Config.NSlaves; i++) if (Run.Slave[i].LastResponse) break;
	if (i != Config.NSlaves) {
		Log(TXT_ERROR "DSINK: Can not load firmware - check VME responses.\n");
		printf(TXT_BOLDRED "Init failed. Check the system and try again." TXT_NORMAL "\n");
		return;
	}
	sleep(1);
	for (i=0; i<Config.NSlaves; i++) if (Run.Slave[i].PID) SendScript(&Run.Slave[i], "i *;?");
	for (i=0; i<Config.NSlaves; i++) while (Run.Slave[i].IsWaiting) DoSelect(0);
	for (i=0; i<Config.NSlaves; i++) if (Run.Slave[i].LastResponse) break;
	if (i != Config.NSlaves) {
		Log(TXT_ERROR "DSINK: Can not initialize the modules - check VME responses.\n");
		printf(TXT_BOLDRED "Init failed. Check the system and try again." TXT_NORMAL "\n");
		return;
	}
//		Clear inhibit on VETO module
	snprintf(cmd, MAXSTR, "m %d 0;?", Config.VetoMasterModule);	
	SendScript(&Run.Slave[Config.VetoMasterCrate], cmd);

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

/*	Open client data connection to the server. Return 0 on success.		*/
int OpenClient(void)
{
	struct sockaddr_in addr;
	socklen_t len;
	int i, irc, descr;

	// Search for empty slot
	for (i=0; i<MAXCON; i++) if (!Run.Client[i].fd) break;
	if (i == MAXCON) return 100;
	
	len = sizeof(addr);
	irc = accept(Run.fdOut, (struct sockaddr *)&addr, &len);
	if (irc <= 0) {
		Log(TXT_ERROR "DSINK: Client connection accept error: %m\n");
		return -10;
	}

	Run.Client[i].fd = irc;
	Run.Client[i].ip = addr.sin_addr.s_addr;
	Run.Client[i].rptr = 0;
	Run.Client[i].wptr = 0;

	// Set client non-blocking
	irc = fcntl(Run.Client[i].fd, F_GETFL, 0);
	if (irc == -1) {
		close(Run.Client[i].fd);
		Run.Client[i].fd = 0;
		Log(TXT_ERROR "DSINK: Client fcntl(F_GETFL) error: %m\n");
		return -15;
	}

	descr = irc | O_NONBLOCK;
	irc = fcntl(Run.Client[i].fd, F_SETFL, descr);
	if (irc == -1) {
		close(Run.Client[i].fd);
		Run.Client[i].fd = 0;
		Log(TXT_ERROR "DSINK: Client fcntl(F_SETFL) error: %m\n");
		return -16;
	}

	Run.Client[i].fifo = (char *) malloc(FIFOSIZE);
	if (!Run.Client[i].fifo) {
		close(Run.Client[i].fd);
		Run.Client[i].fd = 0;
		Log(TXT_ERROR "DSINK: Client FIFO allocation error: %m\n");
		return -20;
	}

	Log(TXT_INFO "DSINK: client connection from %s accepted\n", inet_ntoa(addr.sin_addr));
	return 0;
}

/*	Open slave data connection to the server				*/
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
	Log(TXT_INFO "DSINK: data connection from %s accepted\n", inet_ntoa(addr.sin_addr));
}

/*	Open file to write data							*/
void OpenDataFile(const char *name)
{
	char cmd[2*MAXSTR];
	int irc;
	
	CloseDataFile();
	Run.fDataName[MAXSTR-1] = '\0';	
	Run.fData = NULL;
	Run.iAuto = 0;
	if (!name || !name[0]) {
		Run.fDataName[0] = '\0';
		return;
	} else if (!strcmp(name, "auto")) {
		irc = GetNextAutoNumber();
		if (irc < 0) return;	// error message was already printed
		Run.iAuto = 1;
		strcpy(Run.fDataName, DATA_DIR);
		snprintf(&Run.fDataName[strlen(DATA_DIR)], MAXSTR - strlen(DATA_DIR), Config.AutoName, irc);
	} else if (name[0] == '/') {
		strncpy(Run.fDataName, name, MAXSTR);
	} else {
		snprintf(Run.fDataName, MAXSTR, "%s%s", DATA_DIR, name);
	}
	Run.fData = fopen(Run.fDataName, "ab");	// we append to data files
	if (!Run.fData) {
		Log(TXT_ERROR "DSINK: Can not open data file to write: %s (%m)\n", Run.fDataName);
		Run.fDataName[0] = '\0';
		Run.iAuto = 0;		// reset auto flag
		return;
	}
	Run.fHead.cnt = 0;
	Run.fHead.ip = INADDR_LOOPBACK;
	memset(Run.RecStat, 0, sizeof(Run.RecStat));
	Run.LastFileStarted = time(NULL);
	Run.FileCounter = 0;
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

/*	Process data received							*/
void ProcessData(char *buf)
{
	struct rec_header_struct *header;
	struct blkinfo_struct *info;
	int num;
	
	if (Run.RestartFlag) return;
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
int ReadConf(const char *fname)
{
	config_t cnf;
	int tmp;
	int i;
	char *stmp;
	char *tok;
	config_setting_t *ptr;
	char cmd[MAXSTR];

	memset(&Config, 0, sizeof(Config));
	config_init(&cnf);
	if (config_read_file(&cnf, fname) != CONFIG_TRUE) {
        	Log(TXT_FATAL "DSINK: Configuration error in file %s at line %d: %s\n", fname, config_error_line(&cnf), config_error_text(&cnf));
		config_destroy(&cnf);
            	return -10;
    	}
//	int InPort;			// Data port 0xA336
	Config.InPort = (config_lookup_int(&cnf, "Sink.InPort", &tmp)) ? tmp : 0xA336;
//	int OutPort;			// Out port 0xB230
	Config.OutPort = (config_lookup_int(&cnf, "Sink.OutPort", &tmp)) ? tmp : 0xB230;
//	char MyName[MAXSTR];		// The server host name
	strncpy(Config.MyName, (config_lookup_string(&cnf, "Sink.MyName", (const char **) &stmp)) ? stmp : "dserver.danss.local", MAXSTR);
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
		config_destroy(&cnf);
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
		config_destroy(&cnf);
		return -12;
	}
	tok = strtok(NULL, " \t,:");
	Config.TriggerMasterModule = (tok) ? strtol(tok, NULL, 0) : 1;
//	char VetoMaster[MAXSTR];	// Trigger master module and crate (index in slave list)
	if (!config_lookup_string(&cnf, "Sink.VetoMaster", (const char **) &stmp)) stmp = (char *)"0:1";
	tok = strtok(stmp, " \t,:");
	Config.VetoMasterCrate = strtol(tok, NULL, 0);
	if (Config.VetoMasterCrate < 0 || Config.VetoMasterCrate >= Config.NSlaves) {
		Log(TXT_FATAL "DSINK: Config.VetoMasterCrate = %d out of range in file %s\n", Config.VetoMasterCrate, fname);
		config_destroy(&cnf);
		return -12;
	}
	tok = strtok(NULL, " \t,:");
	Config.VetoMasterModule = (tok) ? strtol(tok, NULL, 0) : 1;
//	char LogFile[MAXSTR];		// dsink log file name
	strncpy(Config.LogFile, (config_lookup_string(&cnf, "Sink.LogFile", (const char **) &stmp)) ? stmp : "dsink.log", MAXSTR);
//	char LogTermCMD[MAXSTR];	// start log view in a separate window
	strncpy(Config.LogTermCMD, (config_lookup_string(&cnf, "Sink.LogTermCMD", 
		(const char **) &stmp)) ? stmp : "xterm -geometry 240x23 -title DSINK_Log -e tail -f dsink.log", MAXSTR);
//	char XilinxFirmware[MAXSTR];	// Xilinx firmware
	strncpy(Config.XilinxFirmware, (config_lookup_string(&cnf, "Sink.XilinxFirmware", (const char **) &stmp)) ? stmp : "", MAXSTR);
//	char InitScript[MAXSTR];	// initialize modules
//	strncpy(Config.InitScript, (config_lookup_string(&cnf, "Sink.InitScript", 
//		(const char **) &stmp)) ? stmp : "p * main.bin;w 500;i general.conf", MAXSTR);
//	char StartScript[MAXSTR];	// put vme into acquire mode. Agruments: server, port
//	strncpy(Config.StartScript, (config_lookup_string(&cnf, "Sink.StartScript", (const char **) &stmp)) ? stmp : "y * * %s:%d", MAXSTR);
//	char StopScript[MAXSTR];	// stop acquire mode
//	strncpy(Config.StopScript, (config_lookup_string(&cnf, "Sink.StopScript", (const char **) &stmp)) ? stmp : "q", MAXSTR);
//	char InhibitScript[MAXSTR];
//	strncpy(Config.InhibitScript, (config_lookup_string(&cnf, "Sink.InhibitScript", (const char **) &stmp)) ? stmp : "m %d 1", MAXSTR);
//	char EnableScript[MAXSTR];
//	strncpy(Config.EnableScript, (config_lookup_string(&cnf, "Sink.EnableScript", (const char **) &stmp)) ? stmp : "z %d;m %d 0", MAXSTR);
//	Dmodule *WFD[MAXWFD];		// class WFD modules for data processing
	ptr = config_lookup(&cnf, "ModuleList");
	if (!ptr) {
		Log(TXT_FATAL "DSINK: No modules defined in %s. ModuleList section must be present.\n", fname);
		config_destroy(&cnf);
		return -13;
	}
	for (i=0; i<MAXWFD;i++) {
		tmp = config_setting_get_int_elem(ptr, i);
		if (tmp <= 0) break;
		if (tmp >= MAXWFD) {
			Log(TXT_FATAL "DSINK: Module serial %d out of the range (1-%d).\n", tmp, MAXWFD);
			config_destroy(&cnf);
			return -14;			
		}
		try {
			Run.WFD[tmp-1] = new Dmodule(tmp);
		}
		catch (...) {
			config_destroy(&cnf);
			return -15;
		}
	}
	if (!i) {
		Log(TXT_FATAL "DSINK: ModuleList section is empty in %s.\n", fname);
		config_destroy(&cnf);
		return -16;
	}
//	int MaxEvent;			// Number of simultaneously allowed events in the builder 
	Config.MaxEvent = (config_lookup_int(&cnf, "Sink.MaxEvent", &tmp)) ? tmp : 1024;
	Run.Evt = (struct event_struct *) malloc(Config.MaxEvent * sizeof(struct event_struct));
	Run.EvtCopy = (struct event_struct *) malloc(Config.MaxEvent * sizeof(struct event_struct));
	if (!Run.Evt || !Run.EvtCopy) {
		Log(TXT_FATAL "DSINK: Not enough memory for Event cache %m.\n");
		config_destroy(&cnf);
		return -17;
	}
	memset(Run.Evt, 0, Config.MaxEvent * sizeof(struct event_struct));
//	char CheckDiskScript[MAXSTR];	// the script is called before new file in auto mode is written
	strncpy(Config.CheckDiskScript, (config_lookup_string(&cnf, "Sink.CheckDiskScript", (const char **) &stmp)) ? stmp : "", MAXSTR);
//	char AutoName[MAXSTR];		// auto file name format
	strncpy(Config.AutoName, (config_lookup_string(&cnf, "Sink.AutoName", (const char **) &stmp)) ? stmp : "danss_data_%6.6d.data", MAXSTR);
//	int AutoTime;			// half an hour
	Config.AutoTime = (config_lookup_int(&cnf, "Sink.AutoTime", &tmp)) ? tmp : 600;
//	int AutoSize;			// in MBytes (2^20 bytes)
	Config.AutoSize = (config_lookup_int(&cnf, "Sink.AutoSize", &tmp)) ? tmp : 10;
//	char ConfSavePattern[MAXSTR];	// pattern to copy configuration when dsink reads it
	strncpy(Config.ConfSavePattern, (config_lookup_string(&cnf, "Sink.ConfSavePattern", (const char **) &stmp)) ? stmp : "", MAXSTR);
	if (strlen(Config.ConfSavePattern)) {
		snprintf(cmd, MAXSTR, "cp %s %s", fname, Config.ConfSavePattern);
		system(cmd);
	}
//	LogSavePattern="history/log_`date +%F_%H%M`.log";	// Pattern to rename the old log file before compression
	strncpy(Config.LogSavePattern, (config_lookup_string(&cnf, "Sink.LogSavePattern", (const char **) &stmp)) ? stmp : "", MAXSTR);
	if (strlen(Config.LogSavePattern)) {
		snprintf(cmd, MAXSTR, "f=%s;mv %s $f;bzip2 $f&", Config.LogSavePattern, Config.LogFile);
		system(cmd);
	}
//	int PeriodicTriggerPeriod;	// Period of the pulser trigger, ms
	Config.PeriodicTriggerPeriod = (config_lookup_int(&cnf, "Sink.PeriodicTriggerPeriod", &tmp)) ? tmp : 0;
	
	config_destroy(&cnf);
	return 0;
}

/*	Restart data taking on a signoificant error				*/
void RestartRun(void)
{
	Log(TXT_INFO "DSINK: Restarting.\n");
	StopRun();
	if (Run.iAuto) {
		Init();
		if (Run.Initialized) {
			OpenDataFile("auto");
			StartRun();
		} else {
			Run.iAuto = 0;
		}
	}
	Run.RestartFlag = 0;
}

/*	Send script from FIFO to the slave					*/
void SendFromFifo(struct slave_struct *slave)
{
	char *ptr;

	if (!slave->CommandFifo[0]) return;
	for (ptr = slave->CommandFifo; *ptr; ptr++) {
		if (ptr[0] == ';') {
			putc('\n', slave->in.f);
			if (slave->IsWaiting) break;
			continue;
		}
		putc(ptr[0], slave->in.f);
		if (ptr[0] == '?') slave->IsWaiting = 1;
	}
	if (ptr[0]) {
		memmove(slave->CommandFifo, ptr, strlen(ptr) + 1);
	} else {
		putc('\n', slave->in.f);
		slave->CommandFifo[0] = '\0';
	}
	fflush(slave->in.f);
}

/*	Send a script divided by semicolons as separate lines to FIFO		*/
void SendScript(struct slave_struct *slave, const char *script)
{
	if (strlen(script) + strlen(slave->CommandFifo) > MAXSTR) {
		Log(TXT_ERROR "DSINK: Command fifo overflow for ssh channel\n");
		return;
	}
	
	strcat(slave->CommandFifo, ";");
	strcat(slave->CommandFifo, script);	

	if (!slave->IsWaiting) SendFromFifo(slave);
}

/*	Set/Clear Inhibit on the main trigger module				*/
void SetInhibit(int what)
{
	char str[MAXSTR];
	if (!Run.Slave[Config.TriggerMasterCrate].PID) {
		Log(TXT_ERROR "DSINK: No communication to Master module VME crate.\n");
		return;
	}
	snprintf(str, MAXSTR, "m %d %d;?", Config.TriggerMasterModule, (what) ? 1 : 0);	
	SendScript(&Run.Slave[Config.TriggerMasterCrate], str);
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
		Log(TXT_ERROR "DSINK: Not all VME crates are attached. Can not start.\n");
		printf(TXT_BOLDRED "Start failed. Check the system and try again." TXT_NORMAL "\n");
		return;	
	}

	if (!Run.Initialized) Init();
	if (!Run.Initialized) return;

	SetInhibit(1);

	for(i=0; i<MAXWFD; i++) if (Run.WFD[i]) Run.WFD[i]->Reset();

	snprintf(str, MAXSTR, "y * * %s:%d;?", Config.MyName, Config.InPort);
	for (i=0; i<Config.NSlaves; i++) SendScript(&Run.Slave[i], str);
	for (i=0; i<Config.NSlaves; i++) while (Run.Slave[i].IsWaiting) DoSelect(0);
	for (i=0; i<Config.NSlaves; i++) if (Run.Slave[i].LastResponse) break;
	if (i != Config.NSlaves) {
		Log(TXT_ERROR "DSINK: Can not start the run - check VME responses.\n");
		printf(TXT_BOLDRED "Start failed. Check the system and try again." TXT_NORMAL "\n");
		return;
	}
	sleep(1);
	snprintf(str, MAXSTR, "z %d;k %d %d", Config.TriggerMasterModule, Config.TriggerMasterModule, Config.PeriodicTriggerPeriod);
	SendScript(&Run.Slave[Config.TriggerMasterCrate], str);
	SetInhibit(0);
	Log(TXT_INFO "DSINK: Executing START command.\n");
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

	Log(TXT_INFO "DSINK: Executing STOP command.\n");
	if (Run.iAuto) {
		CloseDataFile();
		Run.fDataName[MAXSTR-1] = '\0';	
	}

	SetInhibit(1);
	sleep(1);
	for (i=0; i<Config.NSlaves; i++) if (Run.Slave[i].PID) SendScript(&Run.Slave[i], "q");
	Run.iRun = 0;
}

/*	Switch file in the auto mode						*/
void SwitchAutoFile(void)
{
	int irc;
	
//	SetInhibit(1);

	CloseDataFile();

	irc = GetNextAutoNumber();
	if (irc < 0) {
		Run.iAuto = 0;
		Run.fDataName[0] = '\0';
		Run.fData = NULL;
		return;
	}
	strcpy(Run.fDataName, DATA_DIR);
	snprintf(&Run.fDataName[strlen(DATA_DIR)], MAXSTR - strlen(DATA_DIR), Config.AutoName, irc);
	Run.fData = fopen(Run.fDataName, "ab");	// we append to data files
	if (!Run.fData) {
		Log(TXT_ERROR "DSINK: Can not open data file to write: %s (%m)\n", Run.fDataName);
		Run.fDataName[0] = '\0';
		Run.iAuto = 0;		// reset auto flag
		return;
	}
	Run.fHead.cnt = 0;
	Run.fHead.ip = INADDR_LOOPBACK;
	memset(Run.RecStat, 0, sizeof(Run.RecStat));
	Run.LastFileStarted = time(NULL);
	Run.FileCounter = 0;
//	SetInhibit(0);
}

/*	Write and Send data from run.wData. Header MUST correspond to Run.fHead	*/
void WriteAndSend(void)
{
	int irc;
	int i;

	if (Run.fData) {
		irc = fwrite(Run.wData, Run.fHead.len, 1, Run.fData);
		if (irc != 1) {
			Log(TXT_ERROR "DSINK: Write data file %s failed: %m\n", Run.fDataName);
			fclose(Run.fData);
			Run.fData = NULL;
		}
		Run.FileCounter += Run.fHead.len;
		if (Run.iAuto && Run.FileCounter > (long long) Config.AutoSize * MBYTE) SwitchAutoFile();
	}
	for (i=0; i<MAXCON; i++) if (Run.Client[i].fd) ClientPush(&Run.Client[i], (char *)Run.wData, Run.fHead.len);
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
	len = sizeof(struct rec_header_struct) + ((info->data[0] & 0x1FF) + 1) * sizeof(short);
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
	Run.fHead.type = REC_SELFTRIG + (num & REC_SERIALMASK) + ((((int)info->data[0]) << 7) & 0x3F0000);
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
			for (i=0; i<Config.NSlaves; i++) if (Run.Slave[i].PID) SendScript(&Run.Slave[i], &copy[tok - cmd]);
		} else {
			if (Run.Slave[num].PID) SendScript(&Run.Slave[num], &copy[tok - cmd]);
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
		printf("Data clients:\n");
		for (i=0; i<MAXCON; i++) if (Run.Client[i].fd) printf("%d: %s  W(%6d) R(%6d)\n", 
			i, My_inet_ntoa(Run.Client[i].ip), Run.Client[i].wptr, Run.Client[i].rptr);
	} else if (!strcasecmp(tok, "pause")) {	// Pause
		SetInhibit(1);
	} else if (!strcasecmp(tok, "quit")) {	// Quit
		Run.iStop = 1;
	} else if (!strcasecmp(tok, "resume")) {// Resume
		SetInhibit(0);
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
	int i, c;
	const char *ini_file_name;
	int iAutoStart;

//		Read options
	iAutoStart = 0;
	ini_file_name = (const char *) DEFCONFIG;
	for (;;) {
		c = getopt(argc, argv, "ac:h");
		if (c == -1) break;
		switch (c) {
		case 'a':
			iAutoStart = 1;
			break;
		case 'c':
			ini_file_name = optarg;
			break;
		case 'h':
		default:
			Help();
			return 0;
		}
	}
	
	memset(&Run, 0, sizeof(Run));
	read_history(".dsink_history");

	if (ReadConf(ini_file_name)) goto MyExit;
	if (OpenLog()) goto MyExit;

	Log(TXT_INFO "DSINK started.\n");

	signal(SIGPIPE, SIG_IGN);

	Run.fdPort = BindPort(Config.InPort);
	if (Run.fdPort < 0) goto MyExit;	

	Run.fdOut = BindPort(Config.OutPort);
	if (Run.fdOut < 0) goto MyExit;	

	if (OpenSlaves()) goto MyExit;

	rl_callback_handler_install("DSINK > ", ProcessCmd);	
	if (iAutoStart) {
		StartRun();
		OpenDataFile("auto");
	}

//		Event loop
	while (!Run.iStop) {
		DoSelect(1);
		if (Run.RestartFlag) RestartRun();
		if (Run.iAuto && Run.fData && time(NULL) > Run.LastFileStarted + Config.AutoTime) SwitchAutoFile();
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
	CloseDataFile();
	if (Run.wData) free(Run.wData);
	if (Run.fdOut)	close(Run.fdOut);
	for (i=0; i<MAXCON; i++) if (Run.Client[i].fd) {
		shutdown(Run.Client[i].fd, 2);
		free(Run.Client[i].fifo);
	}
	sleep(2);
	if (Run.fLog) {
		fclose(Run.fLog);
		kill(Run.fLogPID, SIGINT);
	}
	signal(SIGPIPE, SIG_DFL);
	rl_callback_handler_remove();
	write_history(".dsink_history");	
	return 0;
}

