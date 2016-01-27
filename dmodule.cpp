/*
	Simple data sink code for dserver.
	Module buffer handling.
	SvirLex, ITEP, 2016
*/
#include <string.h>
#include "recformat.h"
#include "dmodule.h"

Dmodule::Dmodule(int num)
{
	Serial = num;
	buf = (short int *) malloc(BSIZE);
	if (!buf) {
		printf("FATAL! Memory allocation failure for module %d\n", Serial);
		throw "FATAL! Memory allocation failure";
	}
	rptr = 0;
	wptr = 0;
	memset(ChanParity, -1, sizeof(ChanParity));
	memset(SumParity, -1, sizeof(SumParity));
	TrigToken = -1;
	DelimToken = -1;
	ClearCounters();
}

Dmodule::~Dmodule(void)
{
	if (buf) free(buf);
}

void Dmodule::Add(int *data, int len)
{
	if (wptr + len - sizeof(struct rec_header_struct) > BSIZE) {
		printf("FATAL! Buffer overflow for module %d\n", Serial);
		throw "FATAL! Buffer overflow";
	} else if (len == sizeof(struct rec_header_struct)) return;
	memcpy(&buf[wptr >> 1], ((char *) data) + sizeof(struct rec_header_struct), len - sizeof(struct rec_header_struct));
	wptr += len - sizeof(struct rec_header_struct);
}

int *Dmodule::Get(void);

void Dmodule::ClearCounters(void)
{
	memset(ErrCnt, 0, sizeof(ErrCnt));
	BlkCnt = 0;
}

