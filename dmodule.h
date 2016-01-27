#ifndef DMODULE_H
#define DMADULE_H

#define BSIZE	0x120000

class Dmodule {
private:
	int Serial;
	short int *buf;
	int rptr;
	int wptr;
	char ChanParity[64];
	char SumParity[4];
	int TrigToken;
	int DelimToken;
	int ErrCnt[4];
	int BlkCnt;
public:
	Dmodule(int num);
	~Dmodule(void);
	void Add(int *data, int len);
	int *Get(void);
	inline int *GetErrCnt(void) { return ErrCnt;};
	void ClearCounters(void);
};

#endif

