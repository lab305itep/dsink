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

//	Data block type
#define TYPE_SELF	0		// Self trigger waveform 
#define TYPE_MASTER	1		// Master trigger waveform 
#define TYPE_TRIG	2		// Trigger information
#define TYPE_RAW	3		// Raw waveform
#define TYPE_SUM	4		// digital sum of channels waveform 
#define TYPE_DELIM	5		// synchronisation delimiter

#endif /* RECFORMAT_H */

