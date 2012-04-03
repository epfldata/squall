/* Sccsid:     @(#)driver.c     9.1.1.34     5/1/96  11:45:07 */
/* main driver for dss banchmark */

#define DECLARER				/* EXTERN references get defined here */
#define NO_FUNC (int (*) ()) NULL	/* to clean up tdefs */
#define NO_LFUNC (long (*) ()) NULL		/* to clean up tdefs */

#include "config.h"
#include <stdlib.h>
#if (defined(_POSIX_)||!defined(WIN32))		/* Change for Windows NT */
#include <unistd.h>
#include <sys/wait.h>
#endif /* WIN32 */
#include <stdio.h>				/* */
#include <limits.h>
#include <math.h>
#include <ctype.h>
#include <signal.h>
#include <string.h>
#include <errno.h>
#ifdef HP
#include <strings.h>
#endif
#if (defined(WIN32)&&!defined(_POSIX_))
#include <process.h>
#pragma warning(disable:4201)
#pragma warning(disable:4214)
#pragma warning(disable:4514)
#define WIN32_LEAN_AND_MEAN
#define NOATOM
#define NOGDICAPMASKS
#define NOMETAFILE
#define NOMINMAX
#define NOMSG
#define NOOPENFILE
#define NORASTEROPS
#define NOSCROLL
#define NOSOUND
#define NOSYSMETRICS
#define NOTEXTMETRIC
#define NOWH
#define NOCOMM
#define NOKANJI
#define NOMCX
#include <windows.h>
#pragma warning(default:4201)
#pragma warning(default:4214)
#endif

#include "dss.h"
#include "dsstypes.h"
#include "bcd2.h"

/*
 * Function prototypes
 */
void usage (void);
int prep_direct (char *);
int close_direct (void);
void kill_load (void);
int pload (int tbl);
void gen_tbl (int tnum, long start, long count, long upd_num);
int pr_drange (int tbl, long min, long cnt, long num);
int set_files (int t, int pload);
void seed_name (char *tgt, long s, long c, long p);
int partial (int, int);
void gen_seeds (int start, int s);


extern int optind, opterr;
extern char *optarg;
long rowcnt = 0, minrow = 0, upd_num = 0;
double flt_scale;
#if (defined(WIN32)&&!defined(_POSIX_))
char *spawn_args[25];
#endif

/*
 *  Extensions to dbgen for generation of skewed data.
 *	Surajit Chaudhuri, Vivek Narasayya.
 *  (Jan '99)
 */
/* default skew is 0 -- i.e. uniform distribution */
double	skew = 0;


/*
 * general table descriptions. See dss.h for details on structure
 * NOTE: tables with no scaling info are scaled according to
 * another table
 *
 *
 * the following is based on the tdef structure defined in dss.h as:
 * typedef struct
 * {
 * char     *name;          -- name of the table; 
 *                             flat file output in <name>.tbl
 * long      base;          -- base scale rowcount of table; 
 *                             0 if derived
 * int       (*header) ();  -- function to prep output
 * int       (*loader[2]) ();  -- functions to present output
 * long      (*gen_seed) ();  -- functions to seed the RNG
 * int       child;           -- non-zero if there is an associated 
 detail table
 * }         tdef;
 *
 */

/*
 * flat file print functions; used with -F(lat) option
 */
int pr_cust (customer_t * c, int mode);
int pr_line (order_t * o, int mode);
int pr_order (order_t * o, int mode);
int pr_part (part_t * p, int mode);
int pr_psupp (part_t * p, int mode);
int pr_supp (supplier_t * s, int mode);
int pr_order_line (order_t * o, int mode);
int pr_part_psupp (part_t * p, int mode);
int pr_time (dss_time_t * t, int mode);
int pr_nation (code_t * c, int mode);
int pr_region (code_t * c, int mode);

/*
 * inline load functions; used with -D(irect) option
 */
int ld_cust (customer_t * c, int mode);
int ld_line (order_t * o, int mode);
int ld_order (order_t * o, int mode);
int ld_part (part_t * p, int mode);
int ld_psupp (part_t * p, int mode);
int ld_supp (supplier_t * s, int mode);
int ld_order_line (order_t * o, int mode);
int ld_part_psupp (part_t * p, int mode);
int ld_time (dss_time_t * t, int mode);
int ld_nation (code_t * c, int mode);
int ld_region (code_t * c, int mode);

/*
 * seed generation functions; used with '-O s' option
 */
long sd_cust (long skip_count);
long sd_line (long skip_count);
long sd_order (long skip_count);
long sd_part (long skip_count);
long sd_psupp (long skip_count);
long sd_supp (long skip_count);
long sd_order_line (long skip_count);
long sd_part_psupp (long skip_count);
long sd_nation (long skip_count);
long sd_region (long skip_count);

/*
 * header output functions); used with -h(eader) option
 */
int hd_cust (FILE * f);
int hd_line (FILE * f);
int hd_order (FILE * f);
int hd_part (FILE * f);
int hd_psupp (FILE * f);
int hd_supp (FILE * f);
int hd_order_line (FILE * f);
int hd_part_psupp (FILE * f);
int hd_time (FILE * f);
int hd_nation (FILE * f);
int hd_region (FILE * f);

tdef tdefs[] =
{
  {"part.tbl", "part table", 200000, hd_part,
   {pr_part, ld_part}, sd_part, NONE},
  {"partsupp.tbl", "partsupplier table", 200000, hd_psupp,
   {pr_psupp, ld_psupp}, sd_psupp, NONE},
  {"supplier.tbl", "suppliers table", 10000, hd_supp,
   {pr_supp, ld_supp}, sd_supp, NONE},
  {"customer.tbl", "customers table", 150000, hd_cust,
   {pr_cust, ld_cust}, sd_cust, NONE},
  {"order.tbl", "order table", 150000, hd_order,
   {pr_order, ld_order}, sd_order, NONE},
  {"lineitem.tbl", "lineitem table", 150000, hd_line,
   {pr_line, ld_line}, sd_line, NONE},
  {"order.tbl", "order/lineitem tables", 150000, hd_order_line,
   {pr_order_line, ld_order_line}, sd_order_line, LINE},
  {"part.tbl", "part/partsupplier tables", 200000, hd_part_psupp,
   {pr_part_psupp, ld_part_psupp}, sd_part_psupp, PSUPP},
  {"time.tbl", "time table", 2557, hd_time,
   {pr_time, ld_time}, NO_LFUNC, NONE},
  {"nation.tbl", "nation table", NATIONS_MAX, hd_nation,
   {pr_nation, ld_nation}, sd_nation, NONE},
  {"region.tbl", "region table", NATIONS_MAX, hd_region,
   {pr_region, ld_region}, sd_region, NONE},
  {"TPCDSEED", NULL, 0, NO_FUNC,
   {NO_FUNC, NO_FUNC}, NO_LFUNC, NONE}
};

int *pids;
#define LIFENOISE(n)	if (verbose && (i % n) == 0) fprintf(stderr, ".")

void
mk_sparse (long res[], long base, long seq)
{
  long low_mask, seq_mask, overflow = 0;
  int count = 0;

  low_mask = (1 << SPARSE_KEEP) - 1;
  seq_mask = (1 << SPARSE_BITS) - 1;
  LONG2HUGE (base, res);
  HUGE_DIV (res, 1 << SPARSE_KEEP);
  HUGE_MUL (res, 1 << SPARSE_BITS);
  HUGE_ADD (res, seq, res);
  HUGE_MUL (res, 1 << SPARSE_KEEP);
  HUGE_ADD (res, base & low_mask, res);
  bcd2_bin (&low_mask, res[0]);
  bcd2_bin (&seq_mask, res[1]);
  res[0] = low_mask;
  res[1] = seq_mask;
  return;
}

/*
 * routines to handle the graceful cleanup of multi-process loads
 */

void
stop_proc (int signum)
{
  exit (0);
}

void
kill_load (void)
{
  int i;

#if !defined(U2200) && !defined(DOS)
  for (i = 0; i < children; i++)
	if (pids[i])
	  KILL (pids[i]);
#endif /* !U2200 && !DOS */
  return;
}

/*
 * re-set default output file names 
 */
int
set_files (int i, int pload)
{
  char line[80], *new_name;

  if (table & (1 << i))
  child_table:
	{
	  if (pload != -1)
		sprintf (line, "%s.%d", tdefs[i].name, pload + 1);
	  else
		{
		  printf ("Enter new destination for %s data: ",
				  tdefs[i].name);
		  if (fgets (line, sizeof (line), stdin) == NULL)
			return (-1);;
		  if ((new_name = strchr (line, '\n')) != NULL)
			*new_name = '\0';
		  if (strlen (line) == 0)
			return (0);
		}
	  new_name = (char *) malloc (strlen (line) + 1);
	  MALLOC_CHECK (new_name);
	  strcpy (new_name, line);
	  tdefs[i].name = new_name;
	  if (tdefs[i].child != NONE)
		{
		  i = tdefs[i].child;
		  tdefs[i].child = NONE;
		  goto child_table;
		}
	}

  return (0);
}



/*
 * read the distributions needed in the benchamrk
 */
void
load_dists (void)
{
  read_dist (env_config (DIST_TAG, DIST_DFLT), "p_cntr", &p_cntr_set);
  read_dist (env_config (DIST_TAG, DIST_DFLT), "colors", &colors);
  read_dist (env_config (DIST_TAG, DIST_DFLT), "p_types", &p_types_set);
  read_dist (env_config (DIST_TAG, DIST_DFLT), "nations", &nations);
  read_dist (env_config (DIST_TAG, DIST_DFLT), "regions", &regions);
  read_dist (env_config (DIST_TAG, DIST_DFLT), "o_oprio",
			 &o_priority_set);
  read_dist (env_config (DIST_TAG, DIST_DFLT), "instruct",
			 &l_instruct_set);
  read_dist (env_config (DIST_TAG, DIST_DFLT), "smode", &l_smode_set);
  read_dist (env_config (DIST_TAG, DIST_DFLT), "category",
			 &l_category_set);
  read_dist (env_config (DIST_TAG, DIST_DFLT), "rflag", &l_rflag_set);
  read_dist (env_config (DIST_TAG, DIST_DFLT), "msegmnt", &c_mseg_set);

}

/*
 * generate a particular table
 */
#ifndef SUPPORT_64BITS
void
gen_h_tbl (int tnum, long start[], long count[], long upd_num)
{
  order_t o;
  static long max_easy = LONG_MAX >> SPARSE_BITS;
  HUGE_T (i);
  HUGE_T (sk);

  HUGE_SET (start, i);
  while (HUGE_CMP (count, 0) > 0)
	{
	  if (verbose && (i[0] % 1000) == 0)
		fprintf (stderr, ".");
	  mk_sparse (sk, i,
				 (upd_num == 0) ? 0 : 1 + upd_num / (10000 / refresh));
	  /*mk_order (sk, &o);*/
	  tdefs[tnum].loader[direct] (&o, upd_num);
	  HUGE_SUB (count, 1, count);
	  HUGE_ADD (i, 1, i);
	}
  return;
}
#endif /* SUPPORT_64BITS */

#ifdef SUPPORT_64BITS
void
gen_tbl (int tnum, DSS_HUGE start, DSS_HUGE count, long upd_num)
#else
void
gen_tbl (int tnum, long start, long count, long upd_num)
#endif							/* SUPPORT_64BITS */
{
  order_t o;
  supplier_t supp;
  customer_t cust;
  part_t part;
  dss_time_t t;
  code_t code;
  static long max_easy = LONG_MAX >> SPARSE_BITS;
  static int completed = 0;
  long i;
  HUGE_T (sk);

  for (i = start; count; count--, i++)
	{
	  LIFENOISE (1000);
	  ROW_START (tnum);
	  switch (tnum)
		{
		case LINE:
		case ORDER:
		case ORDER_LINE:
		  mk_sparse (sk, i,
					 (upd_num == 0) ? 0 : 1 + upd_num / (10000 / refresh));
		  mk_order (sk, &o);
		  /* tdefs[tnum].loader[direct] (&o, upd_num); */
		  break;
		case SUPP:
		  mk_supp (i, &supp);
		  tdefs[tnum].loader[direct] (&supp, upd_num);
		  break;
		case CUST:
		  mk_cust (i, &cust);
		  tdefs[tnum].loader[direct] (&cust, upd_num);
		  break;
		case PSUPP:
		case PART:
		case PART_PSUPP:
		  mk_part (i, &part);
		  tdefs[tnum].loader[direct] (&part, upd_num);
		  break;
		case TIME:
		  mk_time (i, &t);
		  tdefs[tnum].loader[direct] (&t, 0);
		  break;
		case NATION:
		  mk_nation (i, &code);
		  tdefs[tnum].loader[direct] (&code, 0);
		  break;
		case REGION:
		  mk_region (i, &code);
		  tdefs[tnum].loader[direct] (&code, 0);
		  break;
		}
	  ROW_STOP (tnum);
	}
  completed |= 1 << tnum;
}



void
usage (void)
{
  fprintf (stderr, "%s\n%s\n\t%s\n%s %s\n\n",
		   "USAGE:",
		   "dbgen [-{vfFD}] [-O {fhmst}][-T {pcsoPSOL}]",
		   "[-s <scale>][-C <procs>][-S <step>]",
		   "dbgen [-v] [-O {dhmrt}] [-s <scale>]",
		   "[-U <updates>] [-r <percent>]");
  fprintf (stderr, "-C <n> -- use <n> processes to generate data\n");
  fprintf (stderr, "          [Under DOS, must be used with -S]\n");
  fprintf (stderr, "-D     -- do database load in line\n");
  fprintf (stderr, "-f     -- force. Overwrite existing files\n");
  fprintf (stderr, "-F     -- generate flat files output\n");
  fprintf (stderr, "-h     -- display this message\n");
  fprintf (stderr, "-n <s> -- inline load into database <s>\n");
  fprintf (stderr, "-O d   -- generate SQL syntax for deletes\n");
  fprintf (stderr, "-O f   -- over-ride default output file names\n");
  fprintf (stderr, "-O h   -- output files with headers\n");
  fprintf (stderr, "-O m   -- produce columnar output\n");
  fprintf (stderr, "-O r   -- generate key ranges for deletes.\n");
  fprintf (stderr, "-O s   -- generate seed sets ONLY\n");
  fprintf (stderr, "-O t   -- use TIME table and julian dates\n");
  fprintf (stderr, "-r <n> -- updates refresh (n/100)%% of the\n");
  fprintf (stderr, "          data set\n");
  fprintf (stderr, "-R <n> -- resume seed rfile generation with step <n>\n");
  fprintf (stderr, "-s <n> -- set Scale Factor (SF) to  <n> \n");
  fprintf (stderr, "-S <n> -- build the <n>th step of the data set\n");
  fprintf (stderr, "-T c   -- generate cutomers ONLY\n");
  fprintf (stderr, "-T l   -- generate nation/region ONLY\n");
  fprintf (stderr, "-T L   -- generate lineitem ONLY\n");
  fprintf (stderr, "-T n   -- generate nation ONLY\n");
  fprintf (stderr, "-T o   -- generate orders/lineitem ONLY\n");
  fprintf (stderr, "-T O   -- generate orders ONLY\n");
  fprintf (stderr, "-T p   -- generate parts/partsupp ONLY\n");
  fprintf (stderr, "-T P   -- generate parts ONLY\n");
  fprintf (stderr, "-T r   -- generate region ONLY\n");
  fprintf (stderr, "-T s   -- generate suppliers ONLY\n");
  fprintf (stderr, "-T S   -- generate partsupp ONLY\n");
  fprintf (stderr, "-U <s> -- generate <s> update sets\n");
  fprintf (stderr, "-v     -- enable VERBOSE mode\n");
  fprintf (stderr, "-z     -- generate skewed data distributions\n");
  fprintf (stderr,
		   "\nTo generate the SF=1 (1GB) database population , use:\n");
  fprintf (stderr, "\tdbgen -vfF -s 1\n");
  fprintf (stderr, "\n%s %s\n",
		   "To generate the qualification database population",
		   "(100 MB), use:\n");
  fprintf (stderr, "\tdbgen -vfF -s 0.1\n");
  fprintf (stderr, "\nTo generate updates for a SF=1 (1GB), use:\n");
  fprintf (stderr, "\tdbgen -v -O s -s 1\n");
  fprintf (stderr, "\tdbgen -v -U 1 -s 1\n");
}

/*
 * pload() -- handle the parallel loading of tables
 */
#ifndef DOS

int
partial (int tbl, int s)
{
  HUGE_T (h_rowcnt);
  HUGE_T (h_minrow);
  long rowcnt;
  long minrow;
  char fname[80];
  long extrarows;

  if (verbose)
	{
	  fprintf (stderr, "Starting to load stage %d of %d of %s...",
			   s + 1, children, tdefs[tbl].comment);
	}
  if (load_state (scale, children, s))
	{
	  seed_name (fname, scale, children, s);
	  fprintf (stderr, "Unable to load seeds (%s)\n", fname);
	  exit (-1);
	}
  if (direct == 0)
	set_files (tbl, s);
#ifndef SUPPORT_64BITS
  if ((tbl == LINE || tbl == ORDER) && scale > MAX_32B_SCALE)
	{
	  long step_size;

	  step_size = scale / children;
	  if (step_size > MAX_32B_SCALE)
		{
		  fprintf (stderr, "Each child must generate less than 1TB.\n");
		  fprintf (stderr,
				   "Please rerun DBGEN with a larger number of children\n");
		  exit (1);
		}
	  rowcnt = step_size * tdefs[tbl].base;
	  extrarows = tdefs[tbl].base % children;
	  extrarows *= scale;
	  extrarows %= children;
	  LONG2HUGE (rowcnt, h_rowcnt);
	  LONG2HUGE (rowcnt, h_minrow);
	  HUGE_MUL (h_minrow, s);
	  if (s == children)
		HUGE_ADD (h_rowcnt, extrarows, h_rowcnt);
	  gen_h_tbl (tbl, h_minrow, h_rowcnt, upd_num);
	}
  else
	{
#endif /* SUPPORT_64BITS */
	  rowcnt = tdefs[tbl].base * scale;
	  extrarows = rowcnt % children;
	  rowcnt /= children;
	  minrow = rowcnt * s + 1;
	  if (s == children)
		rowcnt += extrarows;
	  gen_tbl (tbl, minrow, rowcnt, upd_num);
#ifndef SUPPORT_64BITS
	}
#endif /* SUPPORT_64BITS */
  if (verbose)
	fprintf (stderr, "done.\n");

  return (0);
}


int
pload (int tbl)
{
  int c = 0, i, status;
  char cmdline[256];

  rowcnt = tdefs[tbl].base * scale;
  if (rowcnt % children)
	{
	  fprintf (stderr, "'-C' cannot split load equally\n");
	  exit (-1);
	}
  else
	rowcnt /= children;
  if (verbose)
	{
	  fprintf (stderr, "Starting %d children to load %s",
			   children, tdefs[tbl].comment);
	}
  for (c = 0; c < children; c++)
	{
	  pids[c] = SPAWN ();
	  if (pids[c] == -1)
		{
		  perror ("Child loader not created");
		  kill_load ();
		  exit (-1);
		}
	  else if (pids[c] == 0)	/* CHILD */
		{
		  SET_HANDLER (stop_proc);
		  verbose = 0;
		  partial (tbl, c);
		  exit (0);
		}
	  else if (verbose)			/* PARENT */
		fprintf (stderr, ".");
	}

  if (verbose)
	fprintf (stderr, "waiting...");

  c = children;
  while (c)
	{
	  i = WAIT (&status, pids[c - 1]);
	  if (i == -1 && children)
		{
		  if (errno == ECHILD)
			fprintf (stderr, "Could not wait on pid %d\n", pids[c - 1]);
		  else if (errno == EINTR)
			fprintf (stderr, "Process %d stopped abnormally\n", pids[c - 1]);
		  else if (errno == EINVAL)
			fprintf (stderr, "Program bug\n");
		}
	  if (status & 0xFF)
		{
		  if (status & 0xFF == 0117)
			printf ("Process %d: STOPPED\n", i);
		  else
			printf ("Process %d: rcvd signal %d\n",
					i, status & 0x7F);
		}
	  c--;
	}

  if (direct == 0)
	{
	  sprintf (cmdline, "mv %s/pl%d.%d%c %s/%s",
			   env_config (PATH_TAG, PATH_DFLT), pids[0], tbl,
			   'B' + tdefs[tbl].child,
			   env_config (PATH_TAG, PATH_DFLT), tdefs[tbl].name);
	  system (cmdline);
#if (defined(WIN32)&&!defined(_POSIX_))
	  sprintf (cmdline, "COPY %s\\pl*.%d %s\\%s",
			   env_config (PATH_TAG, PATH_DFLT), tbl,
			   env_config (PATH_TAG, PATH_DFLT), tdefs[tbl].name);

	  fprintf (stderr, "%s\n", cmdline);
	  system (cmdline);

	  sprintf (cmdline, "DEL %s\\pl*.%d",
			   env_config (PATH_TAG, PATH_DFLT), tbl);
	  fprintf (stderr, "%s\n", cmdline);
	  system (cmdline);

#else
	  for (c = 1; c < children; c++)
		{
		  sprintf (cmdline,
				   "cat %s/pl%d.%d%c >> %s/%s; rm %s/pl%d.%d%c",
				   env_config (PATH_TAG, PATH_DFLT), pids[c], tbl,
				   'B' + tdefs[tbl].child,
				   env_config (PATH_TAG, PATH_DFLT), tdefs[tbl].name,
				   env_config (PATH_TAG, PATH_DFLT), pids[c], tbl,
				   'B' + tdefs[tbl].child);
		  system (cmdline);
		}
#endif /* WIN32 */
	}

  fprintf (stderr, "done\n");
  return (0);
}
#endif /* !DOS */

void
process_options (int count, char **vector)
{
  int option;

#ifndef DOS
  while ((option = getopt (count, vector,
						   "C:DFfhz:n:O:P:r:R:s:S:T:U:v")) != -1)
#else
  while ((option = getopt (count, vector,
						   "DFfhz:n:O:P:r:R:s:S:T:U:v")) != -1)
#endif /* !DOS */
	switch (option)
	  {
	  case 'S':				/* generate a particular STEP */
		step = atoi (optarg) - 1;
		break;
	  case 'v':				/* life noises enabled */
		verbose = 1;
		break;
	  case 'z':				/* for all columns use specified skew parameter */
		skew = atof(optarg);
		if(skew==-1.0)
			skew = 5;
		break;
	  case 'f':				/* blind overwrites; Force */
		force = 1;
		break;
	  case 'T':				/* generate a specifc table */
		switch (*optarg)
		  {
		  case 'c':			/* generate customer ONLY */
			table = 1 << CUST;
			break;
		  case 'L':			/* generate lineitems ONLY */
			table = 1 << LINE;
			break;
		  case 'l':			/* generate code table ONLY */
			table = 1 << NATION;
			table |= 1 << REGION;
			break;
		  case 'n':			/* generate nation table ONLY */
			table = 1 << NATION;
			break;
		  case 'O':			/* generate orders ONLY */
			table = 1 << ORDER;
			break;
		  case 'o':			/* generate orders/lineitems ONLY */
			table = 1 << ORDER_LINE;
			break;
		  case 'P':			/* generate part ONLY */
			table = 1 << PART;
			break;
		  case 'p':			/* generate part/partsupp ONLY */
			table = 1 << PART_PSUPP;
			break;
		  case 'r':			/* generate region table ONLY */
			table = 1 << REGION;
			break;
		  case 'S':			/* generate partsupp ONLY */
			table = 1 << PSUPP;
			break;
		  case 's':			/* generate suppliers ONLY */
			table = 1 << SUPP;
			break;
		  case 't':			/* generate time ONLY */
			table = 1 << TIME;
			break;
		  default:
			fprintf (stderr, "Unknown table name %s\n",
					 optarg);
			usage ();
			exit (1);
		  }
		break;
	  case 's':				/* scale by Percentage of base rowcount */
	  case 'P':				/* for backward compatibility */
		flt_scale = atof (optarg);
		if (flt_scale < MIN_SCALE)
		  {
			int i;

			scale = 1;
			for (i = PART; i < TIME; i++)
			  {
				tdefs[i].base *= flt_scale;
				if (tdefs[i].base < 1)
				  tdefs[i].base = 1;
			  }
		  }
		else
		  scale = (long) flt_scale;
		if (scale > MAX_SCALE && bld_seeds != 1)
		  {
			fprintf (stderr, "%s %5.0f %s\n\t%s\n\n",
					 "NOTE: Data generation for scale factors >",
					 MAX_SCALE,
					 "GB is still in development,",
					 "and is not yet supported.\n");
			fprintf (stderr,
					 "Your resulting data set MAY NOT BE COMPLIANT!\n");
		  }
		break;
	  case 'O':				/* optional actions */
		switch (tolower (*optarg))
		  {
		  case 'd':			/* generate SQL for deletes */
			gen_sql = 1;
			break;
		  case 'f':			/* over-ride default file names */
			fnames = 1;
			break;
		  case 'h':			/* generate headers */
			header = 1;
			break;
		  case 'm':			/* generate columnar output */
			columnar = 1;
			break;
		  case 'r':			/* generate key ranges for delete */
			gen_rng = 1;
			break;
		  case 's':			/* generate seed sets */
			bld_seeds = 1;
			break;
		  case 't':			/* use TIME table and join fields */
			oldtime = 1;
			if (updates == 0)
			  table |= (1 << TIME);
			break;
		  default:
			fprintf (stderr, "Unknown option name %s\n",
					 optarg);
			usage ();
			exit (1);
		  }
		break;
	  case 'D':				/* direct load of generated data */
		direct = 1;
		break;
	  case 'F':				/* generate flat files for later loading */
		direct = 0;
		break;
	  case 'U':				/* generate flat files for update stream */
		updates = atoi (optarg);
		break;
	  case 'r':				/* set the refresh (update) percentage */
		refresh = atoi (optarg);
		break;
	  case 'R':				/* resume seed file generation */
		resume = atoi (optarg);
		break;
#ifndef DOS
	  case 'C':
		children = atoi (optarg);
		if (children > 999)		/* limitation of current seed file names */
		  {
			printf ("Child process counts of > 999 not supported.\n");
			exit (1);
		  }
		pids = malloc (children * sizeof (pid_t));
		break;
#endif /* !DOS */
	  case 'n':				/* set name of database for direct load */
		db_name = (char *) malloc (strlen (optarg) + 1);
		MALLOC_CHECK (db_name);
		strcpy (db_name, optarg);
		break;
	  default:
		printf ("ERROR: option '%c' unknown.\n",
				*(vector[optind] + 1));
	  case 'h':				/* something unexpected */
		fprintf (stderr,
				 "TPC-D Population Generator (Version %d.%d.%d%s)\n",
				 VERSION, RELEASE,
				 MODIFICATION, PATCH);
		fprintf (stderr, "Copyright %s %s\n", TPC, C_DATES);
		usage ();
		exit (1);
	  }
  return;
}

void
gen_seeds (int start, int s)
{
int i;
long c;
double step_size;

if (start != 1)
	{
	if (load_state (s, children, start - 1))
		{
		char fname[80];
		seed_name (fname, s, children, start - 1);
		fprintf (stderr, "Unable to load seeds (%s)\n", fname);
		exit (-1);
		}
	}
for (c = (long) start; c <= children; c++)
	{
	for (i = PART; i <= PART_PSUPP; i++)
		if (table & (1 << i))
		  {
			if (verbose)
				fprintf (stderr,
					"Generating seeds for %s [step: %d]  ",
					tdefs[i].comment, c);

			step_size = scale / children;
			while (step_size > MAX_32B_SCALE)
				{
				rowcnt = tdefs[i].base * MAX_32B_SCALE;
				tdefs[i].gen_seed (rowcnt);
				step_size -= MAX_32B_SCALE;
				}
			rowcnt = tdefs[i].base * step_size;
			if (c == children)
				{
				int k, j=0;
				for (k = 0; k < scale; k++)
					j += tdefs[i].base % children;
				rowcnt += j % children;
				}
			if (rowcnt > 1)
				tdefs[i].gen_seed (rowcnt);
		  }

	  if (verbose)
		fprintf (stderr, "\n");
	  if (store_state (scale, children, c))
		{
		  fprintf (stderr, "Unable to store seeds\n");
		  exit (-1);
		}
	}
  exit (0);
}

/*
 * MAIN
 *
 * assumes the existance of getopt() to clean up the command 
 * line handling
 */
int
main (int ac, char **av)
{
  int i;

  table = (1 << CUST) |
	(1 << SUPP) |
	(1 << NATION) |
	(1 << REGION) |
	(1 << PART_PSUPP) |
	(1 << ORDER_LINE);
  force = 0;
  verbose = 0;
  columnar = 0;
  bld_seeds = 0;
  header = 0;
  oldtime = 0;
  direct = 0;
  scale = 1;
  flt_scale = 1.0;
  updates = 0;
  refresh = UPD_PCT;
  resume = -1;
  step = -1;
  tdefs[ORDER].base *=
	ORDERS_PER_CUST;			/* have to do this after init */
  tdefs[LINE].base *=
	ORDERS_PER_CUST;			/* have to do this after init */
  tdefs[ORDER_LINE].base *=
	ORDERS_PER_CUST;			/* have to do this after init */
  fnames = 0;
  db_name = NULL;
  gen_sql = 0;
  gen_rng = 0;
  children = 1;

#ifdef NO_SUPPORT
  signal (SIGINT, exit);
#endif /* NO_SUPPORT */
  process_options (ac, av);
#if (defined(WIN32)&&!defined(_POSIX_))
  for (i = 0; i < ac; i++)
	{
	  spawn_args[i] = malloc ((strlen (av[i]) + 1) * sizeof (char));
	  MALLOC_CHECK (spawn_args[i]);
	  strcpy (spawn_args[i], av[i]);
	}
  spawn_args[ac] = NULL;
#endif

  fprintf (stderr,
		   "TPC-D Population Generator (Version %d.%d.%d%s)\n",
		   VERSION, RELEASE, MODIFICATION, PATCH);
  fprintf (stderr, "Copyright %s %s\n", TPC, C_DATES);

  load_dists ();
  /* have to do this after init */
  tdefs[NATION].base = nations.count;
  tdefs[REGION].base = regions.count;

/* 
 * updates are never parallelized 
 */
  if (updates)
	{
	  for (i = LINE; i <= ORDER_LINE; i++)
		{
		  if (table & (1 << i))
			{
			  if (load_state (scale, children, children))
				{
				  fprintf (stderr, "Unable to load seeds (%d)\n",
						   scale);
				  exit (-1);
				}
			  rowcnt = tdefs[i].base / 10000 * scale * refresh;
			  upd_num = 0;
			  while (upd_num < updates)
				{
				  minrow = rowcnt * upd_num + 1;
				  if (verbose)
					fprintf (stderr,
							 "Generating updates for %s [pid: %d]",
							 tdefs[i].comment, DSS_PROC);
				  gen_tbl (i, minrow, rowcnt, upd_num + 1);
				  if (verbose)
					fprintf (stderr, "done.\n");
				  pr_drange (i, minrow, rowcnt, upd_num + 1);
				  upd_num++;
				}
			}
		}
	  exit (0);
	}
/*
 * seed gen is never parallelized 
 */
  if (bld_seeds)
	gen_seeds ((resume == -1) ? 1 : resume, scale);

/**
 ** actual data generation section starts here
 **/
/*
 * open database connection or set all the file names, as appropriate
 */
  if (direct)
	prep_direct ((db_name) ? db_name : DBNAME);
  else if (fnames)
	for (i = PART; i <= REGION; i++)
	  {
		if (table & (1 << i))
		  if (set_files (i, -1))
			{
			  fprintf (stderr, "Load aborted!\n");
			  exit (1);
			}
	  }


  for (i = PART; i <= REGION; i++)
	if (table & (1 << i))
	  {
		if (children > 1 && i < TIME)
		  if (step >= 0)
			partial (i, step);
#ifdef DOS
		  else
			{
			  fprintf (stderr,
					   "Parallel load is not supported on your platform.\n");
			  exit (1);
			}
#else
		  else
			pload (i);
#endif /* DOS */
		else
		  {
			minrow = 1;
			if (i < TIME)
			  rowcnt = tdefs[i].base * scale;
			else
			  rowcnt = tdefs[i].base;
			if (verbose)
			  fprintf (stderr, "Generating data for %s [pid: %d]",
					   tdefs[i].comment, DSS_PROC);
			gen_tbl (i, minrow, rowcnt, upd_num);
			if (verbose)
			  fprintf (stderr, "done.\n");
		  }
	  }

  if (direct)
	close_direct ();

  return (0);
}
