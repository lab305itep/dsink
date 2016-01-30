/*
	Simple data sink code for dserver.
	Module buffer handling.
	SvirLex, ITEP, 2016
*/
#include <stdlib.h>
#include <string.h>
#include "dsink.h"
#include "recformat.h"
#include "dmodule.h"

Dmodule::Dmodule(int num)
{
	Serial = num;
	buf = (unsigned short int *) malloc(BSIZE);
	if (!buf) {
		Log(TXT_FATAL "DSINK: Memory allocation failure for module %d\n", Serial);
		throw -10;
	}
	rptr = 0;
	wptr = 0;
	ClearCounters();
	ClearParity();
}

Dmodule::~Dmodule(void)
{
	if (buf) free(buf);
}

void Dmodule::Add(char *data, int len)
{
	if (wptr + len - sizeof(struct rec_header_struct) > BSIZE) {
		Log(TXT_ERROR "DSINK: Buffer overflow for module %d\n", Serial);
		throw -20;
	} else if (len == sizeof(struct rec_header_struct)) return;
	memcpy(&buf[wptr >> 1], data + sizeof(struct rec_header_struct), len - sizeof(struct rec_header_struct));
	wptr += len - sizeof(struct rec_header_struct);
}

struct blkinfo_struct *Dmodule::Get(void)
{
	int ptr, i;
	int len, chan, type, token, parity;
//		search for control word
Again:
	for (ptr = rptr/sizeof(short); ptr*sizeof(short) < wptr; ptr++) {
		if (buf[ptr] == 0x8000) continue;
		if (buf[ptr] & 0x8000) break;
		Log(TXT_WARN "DSINK: Wrong data 0x%4.4X in place of the control word @ %d. WFD %d\n", buf[ptr] & 0xFFFF, ptr*sizeof(short), Serial);
		ErrCnt[ERR_OTHER]++;
	}
	if (ptr*sizeof(short) == wptr) {	// end of data reached
		rptr = 0;
		wptr = 0;
		return NULL;
	}
//		Get length and check data availability
	len = (buf[ptr] & 0x1FF) + 1;
	if ((ptr + len)*sizeof(short) > wptr) {	// not enough data in buffer. wait for new data.
		wptr -= ptr*sizeof(short);
		memmove(buf, &buf[ptr], wptr);
		rptr = 0;
		return NULL;
	}
//		Check for unexpected control words
	for (i=1; i<len; i++) if (buf[ptr+i] & 0x8000) break;
	if (i != len) {
		Log(TXT_WARN "DSINK: Wrong data 0x%4.4X in place of the regular word @ %d. WFD %d\n", buf[ptr+i] & 0xFFFF, (ptr+i)*sizeof(short), Serial);
		ErrCnt[ERR_OTHER]++;
		rptr = (ptr + i)*sizeof(short);
		goto Again;
	}
	rptr = (ptr + len) * sizeof(short);	// we can move rptr now
//		check for token recive error
	if (buf[ptr+1] & 0x400) {	// this bit must be zero - either by definition or it indicates bad token transmission
		Log(TXT_WARN "DSINK: token error flagged 0x%4.4X @ %d. WFD %d\n", buf[ptr+1] & 0xFFFF, (ptr+1)*sizeof(short), Serial);
		ErrCnt[ERR_OTHER]++;
		goto Again;		
	}
//		Analyze different types
	type = (buf[ptr+1] >> 12) & 7;
	token = buf[ptr+1] & 0x3FF;
	parity = (buf[ptr+1] >> 11) & 1;
	chan = (buf[ptr] >> 9) & 0x3F;
	switch (type) {
	case TYPE_SELF:		// selftrigger
		if (ChanParity[chan] >= 0 && ChanParity[chan] == parity) {
			Log(TXT_WARN "DSINK: Channel parity error (possible data loss). Channel %d.%2.2d\n", Serial, chan);
			ErrCnt[ERR_WFDPAR]++;
		}
		if (SelfToken[chan] >= 0 && ((SelfToken[chan] + 1) & 0x3FF) != token) {
			Log(TXT_WARN "DSINK: Self trigger sequential number missing (possible data loss). Channel %d.%2.2d\n", Serial, chan);
			ErrCnt[ERR_SELFTOK]++;
		}
		ChanParity[chan] = parity;
		SelfToken[chan] = token;
		break;
	case TYPE_MASTER:	// master trigger
	case TYPE_RAW:
		if (ChanParity[chan] >= 0 && ChanParity[chan] == parity) {
			Log(TXT_WARN "DSINK: Channel parity error (possible data loss). Channel %d.%2.2d\n", Serial, chan);
			ErrCnt[ERR_WFDPAR]++;
		}
		ChanParity[chan] = parity;
		break;
	case TYPE_TRIG:
		if (parity != (token & 1)) {
			Log(TXT_WARN "DSINK: Trigger block token/parity miscorrespondence. Module %d\n", Serial);
			ErrCnt[ERR_OTHER]++;
		}
		if (TrigToken >= 0 && ((TrigToken + 1) & 0x3FF) != token) {
			Log(TXT_WARN "DSINK: Master trigger sequential number missing (possible data loss). Module %d.\n", Serial);
			ErrCnt[ERR_TRIGTOK]++;
		}
		TrigToken = token;
		break;
	case TYPE_SUM:
		chan >>= 4;
		if (SumParity[chan] >= 0 && SumParity[chan] == parity) {
			Log(TXT_WARN "DSINK: History parity error (possible data loss). Channel %d.%2.2d\n", Serial, chan);
			ErrCnt[ERR_SUMPAR]++;
		}
		SumParity[chan] = parity;
		break;
	case TYPE_DELIM:
		if (token & 0xFF) {
			Log(TXT_WARN "DSINK: Bad synchronization token 0x%3X. Module%d\n", token, Serial);
			ErrCnt[ERR_OTHER]++;
			goto Again;
		}
		if (parity != ((token >> 8) & 1)) {
			Log(TXT_WARN "DSINK: Synchronization token/parity miscorrespondence. Module%d\n", Serial);
			ErrCnt[ERR_OTHER]++;
		}
		if (DelimToken >= 0 && ((DelimToken + 256) & 0x3FF) != token) {
			Log(TXT_WARN "DSINK: Delimiter sequential number missing (possible data loss). Module %d.\n", Serial);
			ErrCnt[ERR_DELIM]++;
		}
		if (token < DelimToken) SyncCnt++;
		DelimToken = token;
		break;
	}
//		Check for forbidden tokens
	if ((type == TYPE_MASTER || type == TYPE_RAW || type == TYPE_TRIG || type == TYPE_SUM) && DelimToken >= 0) {
		i = 0;
		switch(DelimToken) {
		case 0:
			if (token < 384 || token >= 896) i = 1;	// OK
			break;
		case 256:
			if (token < 640 && token >= 128) i = 1;
			break;
		case 512:
			if (token < 896 && token >= 384) i = 1;
			break;
		case 768:
			if (token < 128 || token >= 640) i = 1;
			break;
		default:
			Log(TXT_ERROR "DSINK Internal logical error. DelimToken = %d\n", DelimToken);
			break;
		}
		if (!i) {
			Log(TXT_WARN "DSINK token range error: Sync=%d / token = %d.\n", DelimToken, token);
			ErrCnt[ERR_OTHER]++;
			goto Again;
		}
	}
//		Block looks OK now
	BlkCnt++;
	BlkInfo.data = &buf[ptr];
	BlkInfo.type = type;
	BlkInfo.lToken = (SyncCnt << 10) + token;
	return &BlkInfo;	
}

int Dmodule::GetLongDelim(void)
{
	if (DelimToken < 0) return 0;
	return (SyncCnt << 10) + DelimToken;
}

void Dmodule::ClearCounters(void)
{
	memset(ErrCnt, 0, sizeof(ErrCnt));
	BlkCnt = 0;
	SyncCnt = 0;
}

void Dmodule::ClearParity(void)
{
	memset(ChanParity, -1, sizeof(ChanParity));
	memset(SumParity, -1, sizeof(SumParity));
	TrigToken = -1;
	DelimToken = -1;
	memset(SelfToken, -1, sizeof(SelfToken));
}

