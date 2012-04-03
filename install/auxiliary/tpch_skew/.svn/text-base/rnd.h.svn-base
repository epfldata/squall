/*
 * Sccsid:  @(#)rnd.h	9.1.1.10   9/7/95  16:24:09
 * 
 * rnd.h -- header file for use withthe portable random number generator
 * provided by Frank Stephens of Unisys
 */

/* function protypes */
long            NextRand    PROTO((long));
long            UnifInt     PROTO((long, long, long));
double          UnifReal    PROTO((double, double, long));
double          Exponential PROTO((double, long));

static long     nA = 16807;     /* the multiplier */
static long     nM = 2147483647;/* the modulus == 2^31 - 1 */
static long     nQ = 127773;    /* the quotient nM / nA */
static long     nR = 2836;      /* the remainder nM % nA */

static double   dM = 2147483647.0;

#ifdef V2
seed_t     Seed[MAX_STREAM + 1] =
{
    {PART,   1,          0, 0},      /* P_MFG_SD     0 */
    {PART,   46831694,   0, 0},      /* P_BRND_SD    1 */
    {PART,   1841581359, 0, 0},      /* P_TYPE_SD    2 */
    {PART,   1193163244, 0, 0},      /* P_SIZE_SD    3 */
    {PART,   727633698,  0, 0},      /* P_CNTR_SD    4 */
    {PART,   933588178,  0, 0},      /* P_RCST_SD    5 */
    {PART,   804159733,  0, 0},      /* P_CMNT_SD    6 */
    {PSUPP,  1671059989, 0, 0},      /* PS_QTY_SD    7 */
    {PSUPP,  1051288424, 0, 0},      /* PS_SCST_SD   8 */
    {PSUPP,  1961692154, 0, 0},      /* PS_CMNT_SD   9 */
    {ORDER,  1227283347, 0, 1},      /* O_SUPP_SD    10 */
    {ORDER,  1171034773, 0, 1},      /* O_CLRK_SD    11 */
    {ORDER,  276090261,  0, V_STR_LEN(O_CMNT_LEN)},      /* O_CMNT_SD    12 */
    {ORDER,  1066728069, 0, 1},      /* O_ODATE_SD   13 */
    {LINE,   209208115,  0, 1},      /* L_QTY_SD     14 */
    {LINE,   554590007,  0, 1},      /* L_DCNT_SD    15 */
    {LINE,   721958466,  0, 1},      /* L_TAX_SD     16 */
    {LINE,   1371272478, 0, 1},      /* L_SHIP_SD    17 */
    {LINE,   675466456,  0, 1},      /* L_SMODE_SD   18 */
    {LINE,   1095462486, 0, V_STR_LEN(L_CMNT_LEN)},      /* L_CMNT_SD    19 */
    {LINE,   1808217256, 0, 0},      /* L_PKEY_SD    20 */
    {LINE,   2095021727, 0, 0},      /* L_SKEY_SD    21 */
    {LINE,   1769349045, 0, 0},      /* L_SDTE_SD    22 */
    {LINE,   904914315,  0, 0},      /* L_CDTE_SD    23 */
    {LINE,   373135028,  0, 0},      /* L_RDTE_SD    24 */
    {CUST,   717419739,  0, 0},      /* L_RFLG_SD    25 */
    {CUST,   881155353,  0, V_STR_LEN(C_ADDR_LEN)},      /* C_ADDR_SD    26 */
    {CUST,   1489529863, 0, 1},      /* C_NTRG_SD    27 */
    {CUST,   1521138112, 0, 3},      /* C_PHNE_SD    28 */
    {CUST,   298370230,  0, 1},      /* C_ABAL_SD    29 */
    {CUST,   1140279430, 0, 1},      /* C_MSEG_SD    30 */
    {CUST,   1335826707, 0, V_STR_LEN(C_CMNT_LEN)},      /* C_CMNT_SD    31 */
    {SUPP,   706178559,  0, 0},      /* S_ADDR_SD    32 */
    {SUPP,   110356601,  0, 0},      /* S_NTRG_SD    33 */
    {SUPP,   884434366,  0, 0},      /* S_PHNE_SD    34 */
    {SUPP,   962338209,  0, 0},      /* S_ABAL_SD    35 */
    {SUPP,   1341315363, 0, 0},      /* S_CMNT_SD    36 */
    {PART,   709314158,  0, 0},      /* P_NAME_SD    37 */
    {ORDER,  591449447,  0, 1},      /* O_PRIO_SD    38 */
    {LINE,   431918286,  0, 0},      /* HVAR_SD      39 */
    {NONE,   851767375,  0, 1},      /* O_CKEY_SD    40 */
    {NATION, 606179079,  0, 0},      /* N_CMNT_SD    41 */
    {REGION, 1500869201, 0, 0},      /* R_CMNT_SD    42 */
    {ORDER,  1434868289, 0, 0},      /* O_LCNT_SD    43 */
    {SUPP,   263032577,  0, 0},      /* BBB offset   44 */
    {SUPP,   753643799,  0, 0},      /* BBB type     45 */
    {SUPP,   202794285,  0, 0},      /* BBB comment  46 */
    {SUPP,   715851524,  0, 0},       /* BBB junk    47 */
    {LINE,   2095021727, 0, 0}       /* L_OKEY_SD    48 */
};
#else
long     Seed[MAX_STREAM + 1] =
{
    1,                          /* P_MFG_SD     0 */
    46831694,                   /* P_BRND_SD    1 */
    1841581359,                 /* P_TYPE_SD 2 */
    1193163244,                 /* P_SIZE_SD    3 */
    727633698,                  /* P_CNTR_SD    4 */
    933588178,                  /* P_RCST_SD    5 */
    804159733,                  /* P_CMNT_SD    6 */
    1671059989,                 /* PS_QTY_SD    7 */
    1051288424,                 /* PS_SCST_SD   8 */
    1961692154,                 /* PS_CMNT_SD   9 */
    1227283347,                 /* O_SUPP_SD    10 */
    1171034773,                 /* O_CLRK_SD    11 */
    276090261,                  /* O_CMNT_SD    12 */
    1066728069,                 /* O_ODATE_SD   13 */
    209208115,                  /* L_QTY_SD     14 */
    554590007,                  /* L_DCNT_SD    15 */
    721958466,                  /* L_TAX_SD     16 */
    1371272478,                 /* L_SHIP_SD    17 */
    675466456,                  /* L_SMODE_SD   18 */
    1095462486,                 /* L_CMNT_SD    19 */
    1808217256,                 /* L_PKEY_SD    20 */
    2095021727,                 /* L_SKEY_SD    21 */
    1769349045,                 /* L_SDTE_SD    22 */
    904914315,                  /* L_CDTE_SD    23 */
    373135028,                  /* L_RDTE_SD    24 */
    717419739,                  /* L_RFLG_SD    25 */
    881155353,                  /* C_ADDR_SD    26 */
    1489529863,                 /* C_NTRG_SD    27 */
    1521138112,                 /* C_PHNE_SD    28 */
    298370230,                  /* C_ABAL_SD    29 */
    1140279430,                 /* C_MSEG_SD    30 */
    1335826707,                 /* C_CMNT_SD    31 */
    706178559,                  /* S_ADDR_SD    32 */
    110356601,                  /* S_NTRG_SD    33 */
    884434366,                  /* S_PHNE_SD    34 */
    962338209,                  /* S_ABAL_SD    35 */
    1341315363,                 /* S_CMNT_SD    36 */
    709314158,                  /* P_NAME_SD    37 */
    591449447,                  /* O_PRIO_SD    38 */
    431918286,                  /* HVAR_SD      39 */
    851767375,                  /* O_CKEY_SD    40 */
    606179079,                  /* N_CMNT_SD    41 */
    1500869201,                 /* R_CMNT_SD    42 */
    1434868289,                 /* O_LCNT_SD    43 */
    263032577,                  /* BBB offset   44 */
    753643799,                  /* BBB type     45 */
    202794285,                  /* BBB comment  46 */
    715851524,                  /* BBB junk     47 */
    2095021727                  /* L_OKEY_SD    48 */
};
/*
 *  Extensions to dbgen for generation of skewed data.
 *	Surajit Chaudhuri, Vivek Narasayya.
 *  (Jan '99)
 */

/*
 *  For Zipfian distribution, we need to know:
 *  (a) what the current value being generated for this column is
 *  (b) how many distinct values have been generated so far for this column
 *  (c) how many copies of the current value must be generated.
 *  (d) how many copies of the current value have been generated thus far.
 *	(e) skew value being used for a column (only if user chose mixed distr.)
 *  (f) multiplier for each stream.
 * (note: Global, hence initialized to 0)
 */
long     CurrentValue[MAX_STREAM + 1];
long     NumDistinctValuesGenerated[MAX_STREAM + 1];
long     CurrentValueTarget[MAX_STREAM + 1];
long     CurrentValueCounter[MAX_STREAM + 1];
double	 ColumnSkewValue[MAX_STREAM + 1];
double     Multiplier[MAX_STREAM + 1];

#endif /* V2 */
