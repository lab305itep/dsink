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
	buf = (short int *) malloc(BSIZE);
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

void Dmodule::Add(int *data, int len)
{
	if (wptr + len - sizeof(struct rec_header_struct) > BSIZE) {
		Log(TXT_ERROR "DSINK: Buffer overflow for module %d\n", Serial);
		throw -20;
	} else if (len == sizeof(struct rec_header_struct)) return;
	memcpy(&buf[wptr >> 1], ((char *) data) + sizeof(struct rec_header_struct), len - sizeof(struct rec_header_struct));
	wptr += len - sizeof(struct rec_header_struct);
}

short int *Dmodule::Get(void)
{
	int ptr, i;
	int len, chan, type, token, parity;
//		search for control word
Again:
	for (ptr = rptr/sizeof(short); ptr*sizeof(short) < wptr; ptr++) {
		if (buf[ptr] == 0x8000) continue;
		if (buf[ptr] & 0x8000) break;
		Log(TXT_WARN "DSINK: Wrong data 0x%4.4X in place of the control word @ %d. WFD %d\n", buf[ptr], ptr*sizeof(short), Serial);
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
	for (i=0; i<len; i++) if (buf[ptr+i] & 0x8000) break;
	if (i != len) {
		Log(TXT_WARN "DSINK: Wrong data 0x%4.4X in place of the control word @ %d. WFD %d\n", buf[ptr], ptr*sizeof(short), Serial);
		ErrCnt[ERR_OTHER]++;
		rptr = (ptr + i)*sizeof(short);
		goto Again;
	}
	type = (buf[ptr+1] >> 12) & 7;
	switch (type) {
	case TYPE_SELF:		// selftrigger
		chan = (buf[ptr] >> 9) & 0x3F;
		parity = (buf[ptr+1] >> 12) & 1;
		token = buf[ptr+1] & 0x3FF;
		if (ChanParity[chan] >= 0 && ChanParity[chan] == parity) ErrCnt[ERR_WFDPAR]++;
		if (SelfToken[chan] >= 0 && ((SelfToken[chan] + 1) & 0x3FF) != token) ErrCnt[ERR_SELFTOK]++;
		ChanParity[chan] = parity;
		SelfToken[chan] = token;
		break;
	case TYPE_MASTER:	// master trigger
	case TYPE_RAW:
		chan = (buf[ptr] >> 9) & 0x3F;
		parity = (buf[ptr+1] >> 12) & 1;
		if (ChanParity[chan] >= 0 && ChanParity[chan] == parity) ErrCnt[ERR_WFDPAR]++;
		ChanParity[chan] = parity;
		break;
	case TYPE_TRIG:
		parity = (buf[ptr+1] >> 12) & 1;
		token = buf[ptr+1] & 0x3FF;
		if (parity != (token & 1)) ErrCnt[ERR_OTHER]++;
		if (TrigToken >= 0 && ((TrigToken + 1) & 0x3FF) != token) ErrCnt[ERR_TRIGTOK]++;
		TrigToken = token;
		break;
	case TYPE_SUM:
		chan = (buf[ptr] >> 13) & 3;
		parity = (buf[ptr+1] >> 12) & 1;
		if (SumParity[chan] >= 0 && SumParity[chan] == parity) ErrCnt[ERR_SUMPAR]++;
		SumParity[chan] = parity;
		break;
	case TYPE_DELIM:
		parity = (buf[ptr+1] >> 12) & 1;
		token = buf[ptr+1] & 0x3FF;
		if (parity != ((token >> 8) & 1)) ErrCnt[ERR_OTHER]++;
		if (DelimToken >= 0 && ((DelimToken + 256) & 0x3FF) != token) ErrCnt[ERR_DELIM]++;
		DelimToken = token;
		break;
	}
	rptr = (ptr + len) * sizeof(short);
	BlkCnt++;
	return &buf[ptr];	
}

void Dmodule::ClearCounters(void)
{
	memset(ErrCnt, 0, sizeof(ErrCnt));
	BlkCnt = 0;
}

void Dmodule::ClearParity(void)
{
	memset(ChanParity, -1, sizeof(ChanParity));
	memset(SumParity, -1, sizeof(SumParity));
	TrigToken = -1;
	DelimToken = -1;
	memset(SelfToken, -1, sizeof(SelfToken));
}

