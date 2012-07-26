/*
 * Sccsid:     @(#)dss.h	9.1.1.26     5/1/96  11:45:05
 *
 * general definitions and control information for the DSS code 
 * generator; if it controls the data set, it's here
 */
#ifndef DSS_H
#define  DSS_H
#define VERSION           1
#define RELEASE           3
#define MODIFICATION      1
#define PATCH             ""
#define TPC             "Transaction Processing Performance Council"
#define C_DATES         "1994 - 1998"

#include "config.h"
#include "shared.h"

#include <stdio.h>
#include <stdlib.h>

#define  NONE   -1
#define  PART   0
#define  PSUPP  1
#define  SUPP   2
#define  CUST   3
#define  ORDER  4
#define  LINE   5
#define  ORDER_LINE   6
#define  PART_PSUPP   7
#define  TIME   8
#define  NATION 9
#define  REGION 10
#define  UPDATE 11
#define  MAX_TABLE 12
#define  ONE_STREAM 1
#define  ADD_AT_END 2
#define  MAX(a,b) ((a > b )?a:b)
#define MALLOC_CHECK(var) \
    if ((var) == NULL) \
        { \
        fprintf(stderr, "Malloc failed at %s:%d\n",  \
            __FILE__, __LINE__); \
        exit(1);\
        }
#define OPEN_CHECK(var, path) \
    if ((var) == NULL) \
        { \
        fprintf(stderr, "Open failed for %s at %s:%d\n",  \
            path, __FILE__, __LINE__); \
        exit(1);\
        }
#ifndef MAX_CHILDREN
#define MAX_CHILDREN    1000
#endif

/*
 * macros that control sparse keys
 *
 * refer to Porting.Notes for a complete explanation
 */
#ifndef BITS_PER_LONG
#define BITS_PER_LONG   32
#define MAX_LONG        0x7FFFFFFF
#endif /* BITS_PER_LONG */
#define SPARSE_BITS      2
#define SPARSE_KEEP      3
#define  MK_SPARSE(key, seq) \
         (((((key>>3)<<2)|(seq & 0x0003))<<3)|(key & 0x0007))
/*
 * macros to control RNG and assure reproducible multi-stream
 * runs without the need for seed files. Keep track of invocations of RNG
 * and always round-up to a known per-row boundary.
 */
#ifdef V2
typedef struct SEED_T {
	long table;
	long value;
	long usage;
	long boundary;
	} seed_t;
#define ROW_START(t)	\
	{ \
	int i; \
	for (i=0; i < MAX_STREAM; i++) \
		if (Seed[i].table == t) Seed[i].usage = 0 ; \
	}
#ifdef SET_SEEDS
#define ROW_STOP(t)	\
	{ \
	int i; \
	for (i=0; i < MAX_STREAM; i++) \
		if ((Seed[i].table == t) && (Seed[i].usage > Seed[i].boundary)) \
			{ \
			fprintf(stderr, "SEED CHANGE: seed[%d].usage = %d\n", \
				i, Seed[i].usage); \
			Seed[i].boundary = Seed[i].usage; \
			} \
	}
#else /* SET_SEEDS */
#define ROW_STOP(t)	\
	{ \
	int i;
	for (i=0; i < MAX_STREAM; i++) \
		if (Seed[i].table = t) \
			while (Seed[i].usage++ < Seed[i].boundary) \
				UnifInt((long)0, (long)100, (long)i); \
	}
#endif /* SET_SEEDS */

/*
 *  Extensions to dbgen for generation of skewed data.
 *	Surajit Chaudhuri, Vivek Narasayya.
 *  (Jan '99)
 */


#define RANDOM(tgt, upper, lower, stream, skew, n)	\
	if(skew==0) \
	{\
		tgt = UnifInt((long)upper, (long)lower, (long)stream); \
		Seed[stream].usage++; \
	} \
	else \
	{ \
		tgt = SkewInt((long)upper, (long)lower, (long)stream, skew, n); \
		Seed[stream].usage++; \
	}

#else	/* V1.x */
#define ROW_START(t)	
#define ROW_STOP(t)			

#define RANDOM(tgt, upper, lower, stream, skew, n)	\
	if(skew == 0) \
		tgt = UnifInt((long)upper, (long)lower, (long)stream); \
	else \
		tgt = SkewInt((long)upper, (long)lower, (long)stream, skew, n);

#endif /* V2 */

                          

typedef struct
{
   long      weight;
   char     *text;
}         set_member;

typedef struct
{
   int      count;
   int      max;
   set_member *list;
}         distribution;

typedef struct
{
   char     *name;
   char     *comment;
   long      base;
   int       (*header) ();
   int       (*loader[2]) ();
   long      (*gen_seed)();
   int       child;
}         tdef;

#if defined(__STDC__)
#define PROTO(s) s
#else
#define PROTO(s) ()
#endif

/* bm_utils.c */
char     *env_config PROTO((char *var, char *dflt));
long yes_no PROTO((char *prompt));
int     a_rnd PROTO((int min, int max, int column, char *dest));
int     tx_rnd PROTO((long min, long max, long column, char *tgt));
long julian PROTO((long date));
long unjulian PROTO((long date));
FILE     *tbl_open PROTO((int tbl, char *mode));
long dssncasecmp PROTO((char *s1, char *s2, int n));
long dsscasecmp PROTO((char *s1, char *s2));
int pick_str PROTO((distribution * s, int c, char *target));
int agg_str PROTO((distribution *set, long count, 
    long col, char *dest));
void read_dist PROTO((char *path, char *name, distribution * target));
void embed_str PROTO((distribution *d, int min, int max, 
    int stream, char *dest));
#ifndef STDLIB_HAS_GETOPT
int getopt PROTO((int arg_cnt, char **arg_vect, char *oprions));
#endif /* STDLIB_HAS_GETOPT */
int load_state PROTO((long scale, long procs, long step));
int store_state PROTO((long scale, long procs, long step));

/* rnd.c */
long NextRand PROTO((long nSeed));
long UnifInt PROTO((long nLow, long nHigh, long nStream));
double UnifReal PROTO((double dLow, double dHigh, long nStream));
double Exponential PROTO((double dMean, long nStream));

#ifdef DECLARER
#define EXTERN
#else
#define EXTERN extern
#endif            /* DECLARER */

#define MIN(A,B)  ( (A) < (B) ? (A) : (B))

EXTERN distribution nations;
EXTERN distribution nations2;
EXTERN distribution regions;
EXTERN distribution o_priority_set;
EXTERN distribution l_instruct_set;
EXTERN distribution l_smode_set;
EXTERN distribution l_category_set;
EXTERN distribution l_rflag_set;
EXTERN distribution c_mseg_set;
EXTERN distribution colors;
EXTERN distribution p_types_set;
EXTERN distribution p_cntr_set;
EXTERN long scale;
EXTERN int refresh;
EXTERN int resume;
EXTERN long verbose;
EXTERN long force;
EXTERN long header;
EXTERN long columnar;
EXTERN long bld_seeds;
EXTERN long oldtime;
EXTERN long direct;
EXTERN long updates;
EXTERN long table;
EXTERN long children;
EXTERN long fnames;
EXTERN int  gen_sql;
EXTERN int  gen_rng;
EXTERN char *db_name;
EXTERN int  step;

#ifndef DECLARER
extern tdef tdefs[];

#endif            /* DECLARER */


/*****************************************************************
 ** table level defines use the following naming convention: t_ccc_xxx
 ** with: t, a table identifier
 **       ccc, a column identifier
 **       xxx, a limit type
 ****************************************************************
 */

/*
 * defines which control the parts table
 */
#define  P_SIZE       126
#define  P_NAME_SCL   5
#define  P_MFG_TAG    "Manufacturer#"
#define  P_MFG_FMT     "%s%01d"
#define  P_MFG_MIN     1
#define  P_MFG_MAX     5
#define  P_BRND_TAG   "Brand#"
#define  P_BRND_FMT   "%s%02d"
#define  P_BRND_MIN     1
#define  P_BRND_MAX     5
#define  P_SIZE_MIN    1
#define  P_SIZE_MAX    50
#define  P_MCST_MIN    100
#define  P_MCST_MAX    99900
#define  P_MCST_SCL    100.0
#define  P_RCST_MIN    90000
#define  P_RCST_MAX    200000
#define  P_RCST_SCL    100.0
/*
 * defines which control the suppliers table
 */
#define  S_SIZE     145
#define  S_NAME_TAG "Supplier#"
#define  S_NAME_FMT "%s%09ld"
#define  S_ABAL_MIN   -99999
#define  S_ABAL_MAX    999999
#define  S_CMNT_MAX    101      
#define  S_CMNT_BBB    10       /* number of BBB comments/SF */
#define  BBB_DEADBEATS 50       /* % that are complaints */
#define  BBB_BASE  "Better Business Bureau "
#define  BBB_COMPLAIN  "Complaints"
#define  BBB_COMMEND   "Recommends"
#define  BBB_CMNT_LEN  33
#define  BBB_BASE_LEN  23
#define  BBB_TYPE_LEN  10

/*
 * defines which control the partsupp table
 */
#define  PS_SIZE      145
#define  PS_SKEY_MIN  0
#define  PS_SKEY_MAX  ((tdefs[SUPP].base - 1) * scale)
#define  PS_SCST_MIN  100
#define  PS_SCST_MAX  100000
#define  PS_QTY_MIN   1
#define  PS_QTY_MAX   9999
/*
 * defines which control the customers table
 */
#define  C_SIZE       165
#define  C_NAME_TAG   "Customer#"
#define  C_NAME_FMT   "%s%09d"
#define  C_MSEG_MAX    5
#define  C_ABAL_MIN   -99999
#define  C_ABAL_MAX    999999
/*
 * defines which control the order table
 */
#define  O_SIZE          109
#define  O_CKEY_MIN      1
#define  O_CKEY_MAX      (long)(tdefs[CUST].base * scale)
#define  O_ODATE_MIN     STARTDATE
#define  O_ODATE_MAX     (STARTDATE + TOTDATE - \
                         (L_SDTE_MAX + L_RDTE_MAX) - 1)
#define  O_CLRK_TAG      "Clerk#"
#define  O_CLRK_FMT      "%s%09d"
#define  O_CLRK_SCL      1000
#define  O_LCNT_MIN      1
#define  O_LCNT_MAX      7
#define  O_OKEY_MAX		(long)(tdefs[ORDER].base * scale)

/*
 * defines which control the lineitem table
 */
#define  L_SIZE       144L
#define  L_QTY_MIN    1
#define  L_QTY_MAX    50
#define  L_TAX_MIN    0
#define  L_TAX_MAX    8
#define  L_DCNT_MIN   0
#define  L_DCNT_MAX   10
#define  L_PKEY_MIN   1
#define  L_PKEY_MAX   (tdefs[PART].base * scale)
#define  L_SDTE_MIN   1
#define  L_SDTE_MAX   121
#define  L_CDTE_MIN   30
#define  L_CDTE_MAX   90
#define  L_RDTE_MIN   1
#define  L_RDTE_MAX   30
#define	 L_LCNT_MAX	  (tdefs[LINE].base * scale)
#define  L_LINE_SIZE  (4 * L_LCNT_MAX)

/*
 * defines which control the time table
 */
#define  T_SIZE       30
#define  T_START_DAY  3     /* wednesday ? */
#define  LEAP(y)  ((!(y % 4) && (y % 100))?1:0)
/*
 * defines which control the nation/region tables
 */
#define R_CMNT_LEN      72
/*******************************************************************
 *******************************************************************
 ***
 *** general or inter table defines
 ***
 *******************************************************************
 *******************************************************************/
#define  SUPP_PER_PART 4
#define  ORDERS_PER_CUST 10 /* sync this with CUST_MORTALITY */
#define  CUST_MORTALITY 3  /* portion with have no orders */
#define  NATIONS_MAX  90 /* limited by country codes in phone numbers */
#define  PHONE_FMT    "%02d-%03d-%03d-%04d"
#define  STARTDATE    92001
#define  CURRENTDATE  95168
#define  ENDDATE      98365
#define  TOTDATE      2557
#define  UPD_PCT      10
#define  MAX_STREAM   48
#define  V_STR_LOW    0.4
#define  PENNIES    100 /* for scaled int money arithmetic */
#define  Q11_FRACTION (double)0.0001
/*
 * max and min SF in GB; Larger SF will require changes to the build routines
 */
#define  MIN_SCALE      1.0
#define  MAX_SCALE   1000.0
/*
 * beyond this point we need to allow for BCD calculations
 */
#define  MAX_32B_SCALE   1000.0
#ifdef SUPPORT_64BITS
#define HUGE_T(name)			DSS_HUGE name
#define LONG2HUGE(src, dst)		dst = (DSS_HUGE)src	
#define HUGE2LONG(src, dst)		dst = (long)src
#define HUGE_SET(src, dst)		dst = src	
#define HUGE_MUL(op1, op2)		op1 *= op2	
#define HUGE_DIV(op1, op2)		op1 /= op2	
#define HUGE_ADD(op1, op2, dst)	dst = op1 + op2	
#define HUGE_SUB(op1, op2, dst)	dst = op1 - op2	
#define HUGE_MOD(op1, op2)		op1 % op2	
#define HUGE_CMP(op1, op2)		(op1 == op2)?0:(op1 < op2)-1:1
#else
#define HUGE_T(name)			long name[2]
#define LONG2HUGE(src, dst)		bin_bcd2(src, &dst[0], &dst[1])
#define HUGE2LONG(src, dst)		dst=0 ; \
								bcd2_bin(&dst, src[1]); \
								bcd2_bin(&dst, src[0])
#define HUGE_SET(src, dst)		dst[0] = src[0];dst[1] = src[1]
#define HUGE_MUL(op1,op2)		bcd2_mul(& op1[0], & op1[1], op2)
#define HUGE_DIV(op1,op2)		bcd2_div(& op1[0], & op1[1], op2)
#define HUGE_ADD(op1,op2,d)		d[0] = op1[0]; d[1] = op1[1]; \
								bcd2_add(& d[0], & d[1], op2)
#define HUGE_SUB(op1,op2,d)		d[0] = op1[0]; d[1] = op1[1]; \
								bcd2_sub(& d[0], & d[1], op2)
#define HUGE_MOD(op1, op2)		bcd2_mod(&op1[0], & op1[1], op2)
#define HUGE_CMP(op1, op2)		(bcd2_cmp(&op1[0], & op1[1], op2) == 0)?0:\
								((bcd2_cmp(&op1[0], & op1[1], op2) < 0)?-1:1)
#endif /* SUPPORT_64BITS */

/******** environmental variables and defaults ***************/
#define  DIST_TAG  "DSS_DIST"  /* environment var to override ... */
#define  DIST_DFLT "dists.dss" /* default file to hold distributions */
#define  PATH_TAG  "DSS_PATH"  /* environment var to override ... */
#define  PATH_DFLT "."      /* default directory to hold tables */
#define  CONFIG_TAG  "DSS_CONFIG"  /* environment var to override ... */
#define  CONFIG_DFLT "."      /* default directory to config files */
#define  SEED_TAG    "DSS_SEED"  /* environment var to override ... */
#define  SEED_DFLT   "."      /* default directory to seed files */

/******* output macros ********/
#ifndef SEPARATOR
#define SEPARATOR '|' /* field spearator for generated flat files */
#endif
#ifndef SET_SEEDS
#define  PR_STR(f,str,len) \
        if (columnar) fprintf(f, "%-*s", len, str); \
        else fprintf(f, "%s%c", str, SEPARATOR)
#ifdef MVS
#define  PR_VSTR(f,str,len) \
        fprintf(f, "%c%c%-*s", (len >> 0x10) & 0xFF, \
                len & 0xFF, len, str)
#else
#define  PR_VSTR(f,str,len) PR_STR(f,str,len)
#endif /* MVS */
#define  PR_INT(f,long)    \
        if (columnar) fprintf(f, "%12ld", long); \
        else fprintf(f,"%ld%c", long, SEPARATOR)
#ifndef SUPPORT_64BITS
#define  PR_HUGE(f,data) \
        if (data[1] == 0) \
           if (columnar) fprintf(f, "%12ld", data[0]); \
           else fprintf(f,"%ld%c", data[0], SEPARATOR); \
        else    \
           if (columnar) fprintf(f, "%5ld%07ld", data[1], data[0]); \
           else fprintf(f,"%ld%07ld%c", data[1], data[0], SEPARATOR)
#else
#define PR_HUGE(f, data) \
	fprintf(f, HUGE_FORMAT, data)
#endif /* SUPPORT_64BITS */
#define  PR_KEY(f,long)    \
        fprintf(f,"%ld", long)
/*
 * problem with the Turbo-C compiler
 */
#define MONEY_COL_FMT "%12ld.%02ld"
#define  PR_MONEY(fp,flt)    \
        if (columnar) fprintf(fp, MONEY_COL_FMT, \
                flt/100, ((flt < 0)?-flt:flt)%100); \
        else fprintf(fp,"%ld.%02ld%c", \
                flt/100, ((flt < 0)?-flt:flt)%100, SEPARATOR) 
#define  PR_CHR(fp,chr)   \
        if (columnar) fprintf(fp, "%c ", chr); \
        else fprintf(fp,"%c%c", chr, SEPARATOR)
#define  PR_STRT(fp)   /* any line prep for a record goes here */
#define  PR_END(fp)    fprintf(fp, "\n")   /* finish the record here */
#ifdef MDY_DATE
#define  PR_DATE(tgt, yr, mn, dy)	\
   sprintf(tgt, "%02d-%02d-19%02d", mn, dy, yr)
#else
#define  PR_DATE(tgt, yr, mn, dy)	\
   sprintf(tgt, "19%02d-%02d-%02d", yr, mn, dy)
#endif /* DATE_FORMAT */
#else /* for SET_SEEDS we don't need output macros */
#define  PR_STR(f,str,len)
#define  PR_VSTR(f,str,len)
#define  PR_INT(f,long)   
#define  PR_HUGE(f,data) 
#define  PR_KEY(f,long) 
#define  PR_MONEY(fp,flt)
#define  PR_CHR(fp,chr)
#define  PR_STRT(fp)  
#define  PR_END(fp)  
#define  PR_DATE(tgt, yr, mn, dy)	
#endif /* SET_SEEDS */


/*********** distribuitons currently defined *************/
#define  UNIFORM   0

/*
 * seed indexes; used to separate the generation of individual columns
 */
#define  P_MFG_SD  0
#define  P_BRND_SD 1
#define  P_TYPE_SD 2
#define  P_SIZE_SD 3
#define  P_CNTR_SD 4
#define  P_RCST_SD 5
#define  PS_QTY_SD 7
#define  PS_SCST_SD   8
#define  O_SUPP_SD 10
#define  O_CLRK_SD 11
#define  O_ODATE_SD   13
#define  L_QTY_SD  14
#define  L_DCNT_SD 15
#define  L_TAX_SD  16
#define  L_SHIP_SD 17
#define  L_SMODE_SD   18
#define  L_PKEY_SD 20
#define  L_SKEY_SD 21
#define  L_SDTE_SD 22
#define  L_CDTE_SD 23
#define  L_RDTE_SD 24
#define  L_RFLG_SD 25
#define  C_NTRG_SD 27
#define  C_PHNE_SD 28
#define  C_ABAL_SD 29
#define  C_MSEG_SD 30
#define  S_NTRG_SD 33
#define  S_PHNE_SD 34
#define  S_ABAL_SD 35
#define  P_NAME_SD 37
#define  O_PRIO_SD 38
#define  HVAR_SD   39
#define  O_CKEY_SD 40
#define  N_CMNT_SD 41
#define  R_CMNT_SD 42
#define  O_LCNT_SD 43
#define  BBB_JNK_SD    44          
#define  BBB_TYPE_SD   45         
#define  BBB_CMNT_SD   46         
#define  BBB_OFFSET_SD 47         
#define  L_OKEY_SD  48
#endif            /* DSS_H */
