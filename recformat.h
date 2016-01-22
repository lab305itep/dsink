#ifndef RECFORMAT_H
#define RECFORMAT_H

struct rec_header_struct {
	int len;
	int cnt;
	int ip;
	int type;
	int time;
};

#define REC_BEGIN	1		// Begin of file / data from the crate
#define REC_PSEOC	10		// Marker for the end of pseudo cycle
#define REC_END		999		// End of file / data from the crate
#define REC_WFDDATA	0x10000		// Regular wave form data

#endif /* RECFORMAT_H */

