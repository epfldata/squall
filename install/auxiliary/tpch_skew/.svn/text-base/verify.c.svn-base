/*
 * Sccsid:     @(#)verify.c	9.1.1.1     12/14/95  15:45:19
 *
 * a simple routine to verify the accuracy of a DBGEN version
 *
 * Algorithm:
 *   traverse the data file named on the command line, OR-ing each
 *   set of 4 characters (8 for machines with 64 bit support). Output
 *   the reulting value to stdout
 */
#include "config.h"
#include <stdlib.h>
#if (defined(_POSIX_)||!defined(WIN32))
#include <unistd.h>
#endif /* WIN32 */
#include <io.h>
#include <fcntl.h>
#include <errno.h>
#include <stdio.h>

#define BUFFER	(1024 * 64)

int
main(int ac, char **av)
{
    int fd;
    char buffer[BUFFER];
    long *lptr, 
	 result = 0L, 
	 steps,
	 i,
         cnt;

    if (ac != 2)
	goto bad;
    if ((fd = open(av[1], O_RDONLY, 0)) == -1)
	goto bad;
    while((cnt = (long)read(fd, buffer, BUFFER)) > 0)
	{
	steps = cnt / sizeof(long);
	for (i=0L, lptr = (long *)buffer; i < steps; i++, lptr++)
	    result ^= *lptr;
	}
    if (cnt == 0)
	{
	fprintf(stderr, "%s verified\n", av[1]);
	printf("%s:\t%ld\n", av[1], result);
	}
    else
	fprintf(stderr, "read() error: %d\n", errno);
    exit(cnt);
	return 0; // to suppress compiler warnings
    
    
bad:
    fprintf(stderr, "\nUSAGE: %s <file>\n", av[0]);
    fprintf(stderr, "\tcomputes a simplified checksum of <file>\n\n");
    exit(1);
}
