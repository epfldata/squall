/*
 * Sccsid:     @(#)bm_utils.c	9.1.1.18     12/11/95  15:32:20
 *
 * Various routines that handle distributions, value selections and
 * seed value management for the DSS benchmark. Current functions:
 * env_config -- set config vars with optional environment override
 * yes_no -- ask simple yes/no question and return boolean result
 * a_rnd(min, max) -- random alphanumeric within length range
 * pick_str(size, set) -- select a string from the set of size
 * read_dist(file, name, distribution *) -- read named dist from file
 * tbl_open(path, mode) -- std fopen with lifenoise
 * julian(date) -- julian date correction
 * rowcnt(tbl) -- proper scaling of given table
 * e_str(set, min, max) -- build an embedded str
 * agg_str() -- build a string from the named set
 * dsscasecmp() -- version of strcasecmp()
 * dssncasecmp() -- version of strncasecmp()
 * getopt()
 * load_state() -- load the data set configuration details
 * store_state() -- store the data set configuration details
 */

#include "dss.h"
#include <stdio.h>
#include <time.h>
#include <errno.h>
#include <string.h>
#ifdef HP
#include <strings.h>
#endif            /* HP */
#include <ctype.h>
#include <math.h>
#ifndef _POSIX_SOURCE
#include <malloc.h>
#endif /* POSIX_SOURCE */
#include <fcntl.h>
#ifdef IBM
#include <sys/mode.h>
#endif /* IBM */
#include <sys/types.h>
#include <sys/stat.h>
/* Lines added by Chuck McDevitt for WIN32 support */
#ifdef WIN32
#ifndef _POSIX_
#include <io.h>
#ifndef S_ISREG
#define S_ISREG(m) ( ((m) & _S_IFMT) == _S_IFREG )
#define S_ISFIFO(m) ( ((m) & _S_IFMT) == _S_IFIFO )
#endif 
#endif
#ifndef stat
#define stat _stat
#endif
#ifndef fdopen
#define fdopen _fdopen
#endif
#ifndef open
#define open _open
#endif
#ifndef O_RDONLY
#define O_RDONLY _O_RDONLY
#endif
#ifndef O_WRONLY
#define O_WRONLY _O_WRONLY
#endif
#ifndef O_CREAT
#define O_CREAT _O_CREAT
#endif
#endif
/* End of lines added by Chuck McDevitt for WIN32 support */
#include "dsstypes.h"

#define INTERNAL_ERROR(p)  fprintf(stderr,"%s", p);abort()

static char alpha_num[65] =
"0123456789abcdefghijklmnopqrstuvwxyz ABCDEFGHIJKLMNOPQRSTUVWXYZ,";

#if defined(__STDC__) || defined(__cplusplus)
#define PROTO(s) s
#else
#define PROTO(s) ()
#endif

char     *getenv PROTO((const char *name));
void usage();
extern long Seed[];
extern double skew;
extern long SkewInt(long nLow, long nHigh, long nStream, double skew, long n);

/*
 * env_config: look for a environmental variable setting and return its
 * value; otherwise return the default supplied
 */
char     *
env_config(char *var, char *dflt)
{
   static char *evar;

   if ((evar = getenv(var)) != NULL)
      return (evar);
   else
      return (dflt);
}

/*
 * return the answer to a yes/no question as a boolean
 */
long
yes_no(char *prompt)
{
    char      reply[128];

#ifdef WIN32
/* Disable warning about conditional expression is constant */ 
#pragma warning(disable:4127)
#endif 

    while (1)
        {
#ifdef WIN32
#pragma warning(default:4127)
#endif 
        printf("%s [Y/N]: ", prompt);
        gets(reply);
        switch (*reply)
            {
            case 'y':
            case 'Y':
                return (1);
            case 'n':
            case 'N':
                return (0);
            default:
                printf("Please answer 'yes' or 'no'.\n");
            }
        }
}

/*
 * generate a random string with length randomly selected in [min, max]
 * and using the characters in alphanum (currently includes a space
 * and comma)
 */
int
a_rnd(int min, int max, int column, char *dest)
{
   long      i,
             len,
             char_int;

   RANDOM(len, min, max, column, skew, (max-min+1));
   for (i = 0; i < len; i++)
      {
      if (i % 5 == 0)
        RANDOM(char_int, 0, MAX_LONG, column, 0, (MAX_LONG));
      *(dest + i) = alpha_num[char_int & 067];
      char_int >>= 6;
      }
   *(dest + len) = '\0';
   return (len);
}

/*
 * embed a randomly selected member of distribution d in alpha-numeric
 * noise of a length rendomly selected between min and max at a random
 * position
 */
void
e_str(distribution *d, int min, int max, int stream, char *dest)
{
    char strtmp[MAXAGG_LEN + 1];
    int loc;
    int len;

    a_rnd(min, max, stream, dest);
    pick_str(d, stream, strtmp);
    len = strlen(strtmp);
    RANDOM(loc, 0, (strlen(dest) - 1 - len), stream, skew, (strlen(dest) - len));
    strncpy(dest + loc, strtmp, len);

    return;
}


/*
 * return the string associate with the LSB of a uniformly selected
 * long in [1, max] where max is determined by the distribution
 * being queried
 */
int
pick_str(distribution *s, int c, char *target)
{
    long      i = 0;
    long      j;

    RANDOM(j, 1, s->list[s->count - 1].weight, c, skew, s->list[s->count - 1].weight);
    while (s->list[i].weight < j)
        i++;
    strcpy(target, s->list[i].text);
    return(i);
}

/*
 * unjulian (long date) -- return(date - STARTDATE)
 */
long
unjulian(long date)
{
    int i;
    long res = 0;

    for (i = STARTDATE / 1000; i < date / 1000; i++)
        res += 365 + LEAP(i);
    res += date %  1000 - 1;

    return(res);
}

long
julian(long date)
{
    long       offset;
    long      result;
    long      yr;
    long      yend;

    offset = date - STARTDATE;
    result = STARTDATE;

#ifdef WIN32
/* Disable warning about conditional expression is constant */ 
#pragma warning(disable:4127)
#endif 

    while (1)
        {
#ifdef WIN32 
#pragma warning(default:4127)
#endif 
        yr = result / 1000;
        yend = yr * 1000 + 365 + LEAP(yr);
        if (result + offset > yend)   /* overflow into next year */
            {
            offset -= yend - result + 1;
            result += 1000;
            continue;
            }
        else
            break;
        }
    return (result + offset);
}

/*
* load a distribution from a flat file into the target structure;
* should be rewritten to allow multiple dists in a file
*/
void
read_dist(char *path, char *name, distribution *target)
{
FILE     *fp;
char      line[256],
         token[256],
        *c;
long      weight,
         count = 0,
         name_set = 0;

    sprintf(line, "%s%c%s", env_config(CONFIG_TAG, CONFIG_DFLT), PATH_SEP, path);
    fp = fopen(line, "r");
    OPEN_CHECK(fp, line);
    while (fgets(line, sizeof(line), fp) != NULL)
        {
        if ((c = strchr(line, '\n')) != NULL)
            *c = '\0';
        if ((c = strchr(line, '#')) != NULL)
            *c = '\0';
        if (*line == '\0')
            continue;

        if (!name_set)
            {
            if (dsscasecmp(strtok(line, "\n\t "), "BEGIN"))
                continue;
            if (dsscasecmp(strtok(NULL, "\n\t "), name))
                continue;
            name_set = 1;
            continue;
            }
        else
            {
            if (!dssncasecmp(line, "END", 3))
                {
                fclose(fp);
                return;
                }
            }

        if (sscanf(line, "%[^|]|%ld", token, &weight) != 2)
            continue;

        if (!dsscasecmp(token, "count"))
            {
            target->count = weight;
            target->list =
                (set_member *)
                    malloc((size_t)(weight * sizeof(set_member)));
            MALLOC_CHECK(target->list);
            target->max = 0;
            continue;
            }
        target->list[count].text =
            (char *) malloc((size_t)(strlen(token) + 1));
        MALLOC_CHECK(target->list[count].text);
        strcpy(target->list[count].text, token);
        target->max += weight;
        target->list[count].weight = target->max;

        count += 1;
        } /* while fgets() */

    if (count != target->count)
        {
        fprintf(stderr, "Read error on dist '%s'\n", name);
        fclose(fp);
        exit(1);
        }
    fclose(fp);
    return;
}

/*
 * standard file open with life noise
 */

FILE     *
tbl_open(int tbl, char *mode)
{
    char      prompt[256];
    char      fullpath[256];
    FILE     *f;
    struct stat fstats;
    int      retcode;


    if (*tdefs[tbl].name == PATH_SEP)
        strcpy(fullpath, tdefs[tbl].name);
    else
        sprintf(fullpath, "%s%c%s",
            env_config(PATH_TAG, PATH_DFLT), PATH_SEP, tdefs[tbl].name);

    retcode = stat(fullpath, &fstats);
    if (retcode && (errno != ENOENT))
        {
        fprintf(stderr, "stat(%s) failed.\n", fullpath);
        exit(-1);
        }
    if (S_ISREG(fstats.st_mode) && !force && *mode != 'r' )
        {
        sprintf(prompt, "Do you want to overwrite %s ?", fullpath);
        if (!yes_no(prompt))
            exit(0);
        }

    if (S_ISFIFO(fstats.st_mode))
        {
        retcode =
            open(fullpath, ((*mode == 'r')?O_RDONLY:O_WRONLY)|O_CREAT);
        f = fdopen(retcode, mode);
        }
    else
        f = fopen(fullpath, mode);
    OPEN_CHECK(f, fullpath);
    if (header && columnar && tdefs[tbl].header != NULL)
        tdefs[tbl].header(f);

    return (f);
}


/*
 * agg_str(set, count) build an aggregated string from count unique
 * selections taken from set
 */
int
agg_str(distribution *set, long count, long col, char *dest)
{
    int used[128],
        picked = 0,
        length = 0,
        i;
    long slot;
    char *pos;

    used[0] = -1;
    pos = dest;

    do
        {
        slot = pick_str(set, (int)col, pos);
        for (i=0; i < picked; i++)
        if (used[i] == slot)
            break;
        if (i < picked)
            continue;
        used[picked++] = slot;
        i = strlen(set->list[slot].text);
        pos += i;
        length += i;
        *pos = ' ';
        pos++;
        } while (--count > 0);
    *(pos - 1) = '\0';

    return (length);
}


long
dssncasecmp(char *s1, char *s2, int n)
{
    for (; n > 0; ++s1, ++s2, --n)
        if (tolower(*s1) != tolower(*s2))
            return ((tolower(*s1) < tolower(*s2)) ? -1 : 1);
        else if (*s1 == '\0')
            return (0);
        return (0);
}

long
dsscasecmp(char *s1, char *s2)
{
    for (; tolower(*s1) == tolower(*s2); ++s1, ++s2)
        if (*s1 == '\0')
            return (0);
    return ((tolower(*s1) < tolower(*s2)) ? -1 : 1);
}

#ifndef STDLIB_HAS_GETOPT
int optind = 0;
int opterr = 0;
char *optarg = NULL;

int
getopt(int ac, char **av, char *opt)
{
    static char *nextchar = NULL;
    char *cp;
    char hold;

    if (optarg == NULL)
        {
        optarg = (char *)malloc(BUFSIZ);
        MALLOC_CHECK(optarg);
        }

    if (!nextchar || *nextchar == '\0')
        {
        optind++;
        if (optind == ac)
            return(-1);
        nextchar = av[optind];
        if (*nextchar != '-')
            return(-1);
        nextchar +=1;
        }

    if (nextchar && *nextchar == '-')   /* -- termination */
        {
        optind++;
        return(-1);
        }
    else        /* found an option */
        {
        cp = strchr(opt, *nextchar);
        nextchar += 1;
        if (cp == NULL) /* not defined for this run */
            return('?');
        if (*(cp + 1) == ':')   /* option takes an argument */
            {
            if (*nextchar)
                {
                hold = *cp;
                cp = optarg;
                while (*nextchar)
                    *cp++ = *nextchar++;
                *cp = '\0';
                *cp = hold;
                }
            else        /* white space separated, use next arg */
                {
                if (++optind == ac)
                    return('?');
                strcpy(optarg, av[optind]);
                }
            nextchar = NULL;
            }
        return(*cp);
        }
}
#endif /* STDLIB_HAS_GETOPT */

char **
mk_ascdate(void)
{
    char **m;
    dss_time_t t;
    int i;

    m = (char**) malloc((size_t)(TOTDATE * sizeof (char *)));
    MALLOC_CHECK(m);
    for (i = 0; i < TOTDATE; i++)
        {
        m[i] = (char *)malloc(DATE_LEN * sizeof(char));
        MALLOC_CHECK(m[i]);
        mk_time((long)(i + 1), &t);
        strcpy(m[i], t.alpha);
        }

    return(m);
}

/*
 * load_state() -- load the data set configuration details so that
 * appropriate data sets can be generated.
 */
void
seed_name(char *res, long sf, int procs, int step)
{
    int pow = 0;

    while (sf > 9)
        {
        pow++;
        sf /= 10;
        }
    sprintf(res, "%1d%1d%03X%03X", sf, pow, procs, step);

    return;
}

int
load_state(long sf, long procs, long step)
{
    FILE *ufp;
    int i;
    char fname[256];
    char line[1024];

    if (sf == 0 || step == 0)
        return(0);

    seed_name(line, sf, procs, step);
    sprintf(fname, "%s%c%s",
        env_config(SEED_TAG, SEED_DFLT),
        PATH_SEP, line);
    ufp = fopen(fname, "r");
    OPEN_CHECK(ufp, fname);

    for (i=0; i <= MAX_STREAM; i++ )
         if (fscanf(ufp, "%ld\n", Seed + i) != 1)
            return(MAX_STREAM);

    fclose(ufp);

    return(0);
}

/*
 * store_state() -- store the data set configuration details so that
 * appropriate update sets can be generated
 */
int
store_state(long sf, long procs, long step)
{
    FILE *ufp;
    int i;
    char fname[256], seed[9];

    seed_name(seed, sf, procs, step);
    sprintf(fname, "%s%c%s",
        env_config(SEED_TAG, SEED_DFLT), PATH_SEP, seed);
    ufp = fopen(fname, "w");
    OPEN_CHECK(ufp, fname);

    for (i=0; i <= MAX_STREAM; i++)
        if (fprintf(ufp, "%ld\n ", Seed[i]) == EOF)
            return(1);

    fclose(ufp);

    return(0);
}

