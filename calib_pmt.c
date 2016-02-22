#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define MIP	2000.0
#define CMIN	1001.0
#define CMAX	4000.0

int main()
{
	char str[256];
	char *ptr;
	double calib[64];
	int i;
	double f;
	
	for (i=0; i<sizeof(calib) / sizeof(calib[0]); i++) calib[i] = 1.0;
	
	for (;;) {
		if (!fgets(str, sizeof(str), stdin)) break;
		ptr = strstr(str, "ADCch=");
		if (!ptr) continue;
		ptr += strlen("ADCch=");
		i = strtol(ptr, NULL, 10);
		if (i < 0 || i >= sizeof(calib) / sizeof(calib[0])) continue;
		ptr = strstr(str, "mip=");
		ptr += strlen("mip=");
		if (!ptr) continue;
		f = strtod(ptr, NULL);
		if (f > CMIN && f < CMAX) calib[i] = MIP / f;
	}
	
	for (i=0; i<sizeof(calib) / sizeof(calib[0]); i++) 
	        printf("%5.3f,%c", calib[i], ((i+1) % 16) ? ' ' : '\n');
	
	return 0;
}
