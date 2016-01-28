#ifndef DMODULE_H
#define DMADULE_H

#define BSIZE	0x120000

class Dmodule {
private:
	int Serial;		// Module serial number
	short int *buf;		// Data buffer
	int rptr;		// read pointer, bytes
	int wptr;		// write pointer, bytes
	char ChanParity[64];	// WFD Channel block last parity: 0/1/-1(undefined)
	char SumParity[4];	// TRIGGER sum block last parity: 0/1/-1(undefined)
	int TrigToken;		// Trigger source last token
	int DelimToken;		// Delimiter last token
	short int SelfToken[64];	// self trig token
	int ErrCnt[6];		// Error counters parity/presence WFDCHAN, TRIGSUM, TRIGTOKEN, DELIMTOKEN, SelfToken and length&other format errors
	int BlkCnt;		// data block counter
public:
	Dmodule(int num);
	~Dmodule(void);
	void Add(int *data, int len);	// add data to the buffer
	short int *Get(void);			// return pointer to data block in the buffer. Analyze format errors.
	inline int *GetErrCnt(void) { return ErrCnt;};
	inline int GetBlkCnt(void) { return BlkCnt;};
	void ClearCounters(void);
	void ClearParity(void);
};

//	Indexes in ErrCnt
#define ERR_WFDPAR	0
#define ERR_SUMPAR	1
#define ERR_TRIGTOK	2
#define ERR_DELIM	3
#define	ERR_SELFTOK	4
#define ERR_OTHER	5

#endif

