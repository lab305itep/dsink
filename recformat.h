#ifndef RECFORMAT_H
#define RECFORMAT_H

struct rec_header_struct {
	int len;
	int cnt;
	int ip;
	int type;
	int time;
};

//	Record types formed by UFWDTOOL
#define REC_BEGIN	1		// Begin of file / data from the crate
#define REC_PSEOC	10		// Marker for the end of pseudo cycle
#define REC_END		999		// End of file / data from the crate
#define REC_WFDDATA	0x00010000	// Regular wave form data
//	Record types formed by DSINK
#define REC_SELFTRIG	0x01000000	// SelfTrigger
#define REC_SERIALMASK	0x0000FFFF	// module serial number
#define REC_CHANMASK	0x00FF0000	// channel mask
#define REC_EVENT	0x80000000	// Event
#define REC_EVTCNTMASK	0x7FFFFFFF	// Event counter - the 10 LSB - token
#define REC_POSITION	0x02000000	// Position read from lift data file
#define REC_SLOW	0x04000000	// Slow data
#define REC_SLOWMASK	0x00FFFFFF	// Mask of slow sources
#define LIFT_SRC	20001		// Platfom lift

//	Data block type
#define TYPE_SELF	0		// Self trigger waveform 
#define TYPE_MASTER	1		// Master trigger waveform 
#define TYPE_TRIG	2		// Trigger information
#define TYPE_RAW	3		// Raw waveform
#define TYPE_SUM	4		// digital sum of channels waveform 
#define TYPE_DELIM	5		// synchronisation delimiter

#endif /* RECFORMAT_H */

