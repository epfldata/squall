/* Sccsid:     @(#)speed_seed.c	9.1.1.10     12/13/95  10:56:18  */
#include <stdio.h>
#include <stdlib.h>
#include "dss.h"

#ifndef SUPPORT_64BITS
#define DSS_HUGE long
#endif /* SUPPORT_64BITS */
#define LN_CNT  4;
static char lnoise[4] = {'|', '/', '-', '\\' };

/*  _tal long RandSeed = "Random^SeedFromTimestamp" (void); */

extern long Seed[];

#define V_STR(avg, sd) fake_a_rnd((int)(avg * V_STR_LOW), \
                                      (int)(avg * V_STR_HGH), sd)
#define ADVANCE_STREAM(stream_id, num_calls) \
        Seed[stream_id] = NthElement(num_calls, Seed[stream_id])

#define MAX_COLOR 92
long name_bits[MAX_COLOR / BITS_PER_LONG];


/* WARNING!  This routine assumes the existence of 64-bit                 */
/* integers.  The notation used here- "HUGE" is *not* ANSI standard. */
/* Hopefully, you have this extension as well.  If not, use whatever      */
/* nonstandard trick you need to in order to get 64 bit integers.         */
/* The book says that this will work if MAXINT for the type you choose    */
/* is at least 2**46  - 1, so 64 bits is more than you *really* need      */

static DSS_HUGE Multiplier = 16807;      /* or whatever nonstandard */
static DSS_HUGE Modulus =  2147483647;   /* trick you use to get 64 bit int */

/* Returns value of Seed after N applications of the random number generator
   with multiplier Mult and given Modulus.  Doesn't actually touch Seed.
   Use:   Seed[] = NthElement(Seed[],count);

   Theory:  We are using a generator of the form
        X_n = [Mult * X_(n-1)]  mod Modulus.    It turns out that
        X_n = [(Mult ** n) X_0] mod Modulus.
   This can be computed using a divide-and-conquer technique, see
   the code below.

   In words, this means that if you want the value of the Seed after n
   applications of the generator,  you multiply the initial value of the
   Seed by the "super multiplier" which is the basic multiplier raised
   to the nth power, and then take mod Modulus.
*/

/* Nth Element of sequence starting with StartSeed */
/* Warning, needs 64-bit integers */
#ifdef SUPPORT_64BITS
long NthElement (long N, long StartSeed)
   {
   DSS_HUGE Z;
   DSS_HUGE Mult;
   static int ln=-1;
   int i;

   if (verbose && ++ln % 1000 == 0)
       {
       i = (ln/1000) % LN_CNT;
       fprintf(stderr, "%c\b", lnoise[i]);
       }
   Mult = Multiplier;
   Z = (DSS_HUGE) StartSeed;
   while (N > 0 )
      {
      if (N % 2 != 0)    /* testing for oddness, this seems portable */
         Z = (Mult * Z) % Modulus;
      N = N / 2;         /* integer division, truncates */
      Mult = (Mult * Mult) % Modulus;
      }
   return((long) Z);
   }
#else
/* add 32 bit version of NthElement HERE */
/*
 *    MODMULT.C
 *    R. M. Shelton -- Unisys
 *    July 26, 1995
 *
 *    RND_seed:  Computes the nth seed in the total sequence
 *    RND_shift:  Shifts a random number by a given number of seeds
 *    RND_ModMult:  Multiplies two numbers mod (2^31 - 1)
 *
 */



#include <math.h>
#include <stdio.h>       /* required only for F_FatalError */

typedef signed long RND;
typedef unsigned long URND;

#define FatalError(e)  F_FatalError( (e), __FILE__, __LINE__ )
void F_FatalError( int x, char *y, int z ) {fprintf(stderr, "Bang!\n");}


/* Prototypes */
RND RND_seed( RND );
RND RND_shift( RND, RND );
static RND RND_ModMult( RND, RND );



RND 
RND_seed ( RND Order            )
{
static const RND TopMask = 0x40000000;
RND Mask;
RND Result;


if (Order <= -Modulus || Order >= Modulus)
   FatalError(1023);

if (Order < 0) Order = Modulus - 1L + Order;

Mask = TopMask;
Result = 1L;

while (Mask > Order) Mask >>= 1;

while (Mask > 0)
   {
   if (Mask & Order)
      {
      Result = RND_ModMult( Result, Result);
      Result = RND_ModMult( Result, Multiplier );
      }
   else
      {
      Result = RND_ModMult( Result, Result );
      }
   Mask >>= 1;
   }

return (Result);

}  /*  RND_seed  */



/***********************************************************************

    RND_shift:  Shifts a random number by a given number of seeds

***********************************************************************/

DSS_HUGE 
NthElement ( long Shift, long Seed)

{
   RND Power;
   static int ln=-1;
   int i;

   if (verbose && ++ln % 100 == 0)
       {
       i = (ln/100) % LN_CNT;
       fprintf(stderr, "%c\b", lnoise[i]);
       }


if (Seed <= 0 || Seed >= Modulus)
   FatalError(1023);
if (Shift <= -Modulus || Shift >= Modulus)
   FatalError(1023);

Power = RND_seed( Shift );

Seed = RND_ModMult( Seed, Power );

return(Seed);
}  /*  RND_shift  */



/*********************************************************************

    RND_ModMult:  Multiplies two numbers mod (2^31 - 1)

*********************************************************************/

static RND 
RND_ModMult ( RND nA, RND nB)

{

static const double dTwoPowPlus31 = 2147483648.;
static const double dTwoPowMinus31 = 1./2147483648.;
static const double dTwoPowPlus15 = 32768.;
static const double dTwoPowMinus15 = 1./32768.;
static const RND    nLowMask = 0xFFFFL;
static const URND   ulBit31 = 1uL << 31;

double dAH, dAL, dX, dY, dZ, dW;
RND    nH, nL;
URND   ulP, ulQ, ulResult;

nL = nB & nLowMask;
nH = (nB - nL) >> 16;
dAH = (double)nA * (double)nH;
dAL = (double)nA * (double)nL;
dX = floor( dAH * dTwoPowMinus15 );
dY = dAH - dX*dTwoPowPlus15;
dZ = floor( dAL * dTwoPowMinus31 );
dW = dAL - dZ*dTwoPowPlus31;

ulQ = (URND)dW + ((URND)dY << 16);
ulP = (URND)dX + (URND)dZ;
if (ulQ & ulBit31) { ulQ -= ulBit31; ulP++; }

ulResult = ulP + ulQ;
if (ulResult & ulBit31) { ulResult -= ulBit31; ulResult++; }

return (RND)ulResult;
}
#endif /* SUPPORT_64BITS */

/* updates Seed[column] using the a_rnd algorithm */
void
fake_a_rnd(int min, int max, int column)
{
   long len, itcount;
   len = UnifInt((long)min, (long)max, (long)column);
   if (len % 5L == 0)
      itcount = len/5;
   else itcount = len/5 + 1L;
   Seed[column] = NthElement(itcount, Seed[column]);
   return;
}


long 
sd_part(long skip_count)
{
   int i;
   long li, color;

   for (i=P_MFG_SD; i<= P_CNTR_SD; i++)
       ADVANCE_STREAM(i, skip_count);
   for (li =1L; li <= skip_count;  li++)
       {
       V_STR(P_CMNT_LEN, P_CMNT_SD);
       for (i=0; i < MAX_COLOR / BITS_PER_LONG; i++) name_bits[i] = 0L;
       i = P_NAME_SCL;
       do {
           color = UnifInt(0L,MAX_COLOR,P_NAME_SD);
           if (name_bits[color/BITS_PER_LONG] & 
                  (1<<(color % BITS_PER_LONG)))
                continue;
           name_bits[color/BITS_PER_LONG] |= 1<<(color % BITS_PER_LONG);
           } while (--i);
        }
   return(0L);
}

long 
sd_line(long skip_count)
{
   int i;
   long li;
   for (i=L_QTY_SD; i<= L_SKEY_SD; i++)
       if( i != L_CMNT_SD ) ADVANCE_STREAM(i, skip_count);
   for (li = 1L; li <= skip_count; li++)
       {
       V_STR(L_CMNT_LEN, L_CMNT_SD);
       UnifInt((long)L_CDTE_MIN, (long)L_CDTE_MAX, (long)L_CDTE_SD);
       }
   return(0L);
}

long 
sd_order(long skip_count)        /* returns total lineitems */
{
   long rc, li, l, total_lines, o_date, rflg, tmp_date;
   total_lines = 0L;
   rflg = 0L;
   for (rc = 1L; rc <= skip_count; rc++)
      {
      li = UnifInt ((long)O_LCNT_MIN, 
                    (long)O_LCNT_MAX, 
                    (long)O_LCNT_SD);
      total_lines += li;
      while((UnifInt(O_CKEY_MIN, 
                     O_CKEY_MAX, 
                     (long)O_CKEY_SD) % CUST_MORTALITY) == 0);
      V_STR(O_CMNT_LEN, O_CMNT_SD);
      tmp_date = UnifInt ((long)O_ODATE_MIN, 
                          (long)O_ODATE_MAX,
                          (long)O_ODATE_SD);
      for (l = 1L; l <= li; l++)
          {
          o_date = tmp_date +
                UnifInt ((long)L_SDTE_MIN, (long)L_SDTE_MAX,  
                     (long)L_SDTE_SD);
          o_date += UnifInt ((long)L_RDTE_MIN, (long)L_RDTE_MAX,
             (long)L_RDTE_SD);
          if (julian(o_date) <= CURRENTDATE)
              rflg++ ;
          }
      }
   ADVANCE_STREAM(O_SUPP_SD, skip_count);
   ADVANCE_STREAM(O_CLRK_SD, skip_count);
   ADVANCE_STREAM(O_PRIO_SD, skip_count);
   ADVANCE_STREAM(L_RFLG_SD, rflg); 
   return (total_lines);
}

long
sd_psupp(long skip_count)
{
   int i;
   long li;
   for (i=PS_QTY_SD; i<= PS_SCST_SD; i++)
       ADVANCE_STREAM(i, skip_count);
   for (li =1L; li <= skip_count;  li++)
       V_STR(PS_CMNT_LEN, PS_CMNT_SD);
   return(0L);
}

long 
sd_cust(long skip_count)
{
   long li;
   for (li =1L; li <= skip_count;  li++)
      {
      V_STR(C_ADDR_LEN, C_ADDR_SD);
      V_STR(C_CMNT_LEN, C_CMNT_SD);
      }
   ADVANCE_STREAM(C_NTRG_SD, skip_count);
   ADVANCE_STREAM(C_PHNE_SD, 3L * skip_count);
   ADVANCE_STREAM(C_ABAL_SD, skip_count);
   ADVANCE_STREAM(C_MSEG_SD, skip_count);
   return(0L);
}

long
sd_supp(long skip_count)
{
   int noise, offset;
   long li;
   long ifpath;

   ADVANCE_STREAM(S_NTRG_SD, skip_count);
   ADVANCE_STREAM(S_PHNE_SD, 3L * skip_count);
   ADVANCE_STREAM(S_ABAL_SD, skip_count);
   ifpath = 0L;
   for (li =1L; li <= skip_count; li++)
      {
      V_STR(S_ADDR_LEN, S_ADDR_SD);
      if (UnifInt((long)1, 
                  (long)10000, 
                  (long)BBB_CMNT_SD) <= S_CMNT_BBB)
         {
         ifpath++;
         noise=UnifInt((long)0, (long)(S_CMNT_MAX - BBB_CMNT_LEN),
             (long)BBB_JNK_SD);    
         offset=UnifInt((long)0,
             (long)(S_CMNT_MAX - (BBB_CMNT_LEN + noise)),
             (long)BBB_OFFSET_SD);
         fake_a_rnd(BBB_CMNT_LEN + noise + offset, S_CMNT_MAX,
                    S_CMNT_SD);
         }
      else  V_STR(S_CMNT_LEN, S_CMNT_SD);
      }                    /* end of rowcount loop */
   ADVANCE_STREAM(BBB_TYPE_SD, ifpath);      /* avoid one trudge */
   
   return(0L);
}

long
nr_seeds(long skip_count)
{
   long li;
   for (li =1L; li <= skip_count;  li++)
      {
      V_STR(N_CMNT_LEN, N_CMNT_SD);
      V_STR(R_CMNT_LEN, R_CMNT_SD); 
      }
   return(0L);
}

long
sd_part_psupp(long skip_count)
{
    sd_part(skip_count);
    sd_psupp(skip_count * SUPP_PER_PART);
    return(0);
}

long
sd_order_line(long skip_count)
{
    long l_item;

    l_item = sd_order(skip_count);
    sd_line(l_item);
    return(0);
}

long 
sd_nation(long skip_count)
{
   long li;

   for (li =1L; li <= skip_count;  li++)
       V_STR(N_CMNT_LEN, N_CMNT_SD);
    return(0);
}

long 
sd_region(long skip_count)
{
   long li;

   for (li =1L; li <= skip_count;  li++)
       V_STR(R_CMNT_LEN, R_CMNT_SD);
    return(0);
}
