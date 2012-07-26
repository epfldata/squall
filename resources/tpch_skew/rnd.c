/*
 * Sccsid:      @(#)rnd.c	9.1.1.9     9/7/95  16:24:04
 * 
 * RANDOM.C -- Implements Park & Miller's "Minimum Standard" RNG
 * 
 * (Reference:  CACM, Oct 1988, pp 1192-1201)
 * 
 * NextRand:  Computes next random integer
 * UnifInt:   Yields an long uniformly distributed between given bounds 
 * UnifReal: ields a real uniformly distributed between given bounds   
 * Exponential: Yields a real exponentially distributed with given mean
 * 
 */

#include "config.h"
#include <stdio.h>
#include <math.h>
#include "dss.h"
#include "rnd.h" 

char *env_config PROTO((char *tag, char *dflt));
#define		PI		(3.141597)


/******************************************************************

   NextRand:  Computes next random integer

*******************************************************************/

/*
 * long NextRand( long nSeed )
 */
long
NextRand(long nSeed)

/*
 * nSeed is the previous random number; the returned value is the 
 * next random number. The routine generates all numbers in the 
 * range 1 .. nM-1.
 */

{

    /*
     * The routine returns (nSeed * nA) mod nM, where   nA (the 
     * multiplier) is 16807, and nM (the modulus) is 
     * 2147483647 = 2^31 - 1.
     * 
     * nM is prime and nA is a primitive element of the range 1..nM-1.  
     * This * means that the map nSeed = (nSeed*nA) mod nM, starting 
     * from any nSeed in 1..nM-1, runs through all elements of 1..nM-1 
     * before repeating.  It never hits 0 or nM.
     * 
     * To compute (nSeed * nA) mod nM without overflow, use the 
     * following trick.  Write nM as nQ * nA + nR, where nQ = nM / nA 
     * and nR = nM % nA.   (For nM = 2147483647 and nA = 16807, 
     * get nQ = 127773 and nR = 2836.) Write nSeed as nU * nQ + nV, 
     * where nU = nSeed / nQ and nV = nSeed % nQ.  Then we have:
     * 
     * nM  =  nA * nQ  +  nR        nQ = nM / nA        nR < nA < nQ
     * 
     * nSeed = nU * nQ  +  nV       nU = nSeed / nQ     nV < nU
     * 
     * Since nA < nQ, we have nA*nQ < nM < nA*nQ + nA < nA*nQ + nQ, 
     * i.e., nM/nQ = nA.  This gives bounds on nU and nV as well:   
     * nM > nSeed  =>  nM/nQ * >= nSeed/nQ  =>  nA >= nU ( > nV ).
     * 
     * Using ~ to mean "congruent mod nM" this gives:
     * 
     * nA * nSeed  ~  nA * (nU*nQ + nV)
     * 
     * ~  nA*nU*nQ + nA*nV
     * 
     * ~  nU * (-nR)  +  nA*nV      (as nA*nQ ~ -nR)
     * 
     * Both products in the last sum can be computed without overflow   
     * (i.e., both have absolute value < nM) since nU*nR < nA*nQ < nM, 
     * and  nA*nV < nA*nQ < nM.  Since the two products have opposite 
     * sign, their sum lies between -(nM-1) and +(nM-1).  If 
     * non-negative, it is the answer (i.e., it's congruent to 
     * nA*nSeed and lies between 0 and nM-1). Otherwise adding nM 
     * yields a number still congruent to nA*nSeed, but now between 
     * 0 and nM-1, so that's the answer.
     */

    long            nU, nV;

    nU = nSeed / nQ;
    nV = nSeed - nQ * nU;       /* i.e., nV = nSeed % nQ */
    nSeed = nA * nV - nU * nR;
    if (nSeed < 0)
        nSeed += nM;
    return (nSeed);
}

/******************************************************************

   UnifInt:  Yields an long uniformly distributed between given bounds

*******************************************************************/

/*
 * long UnifInt( long nLow, long nHigh, long nStream )
 */
long
UnifInt(long nLow, long nHigh, long nStream)

/*
 * Returns an integer uniformly distributed between nLow and nHigh, 
 * including * the endpoints.  nStream is the random number stream.   
 * Stream 0 is used if nStream is not in the range 0..MAX_STREAM.
 */

{
    double          dRange;
    long            nTemp;

    if (nStream < 0 || nStream > MAX_STREAM)
        nStream = 0;
    if (nLow == nHigh)
        return (nLow);
    if (nLow > nHigh)
    {
        nTemp = nLow;
        nLow = nHigh;
        nHigh = nTemp;
    }
    dRange = (double) (nHigh - nLow + 1);
    Seed[nStream] = NextRand(Seed[nStream]);
    nTemp = (long) (((double) Seed[nStream] / dM) * (dRange));
    return (nLow + nTemp);
}



/******************************************************************

   UnifReal:  Yields a real uniformly distributed between given bounds

*******************************************************************/

/*
 * double UnifReal( double dLow, double dHigh, long nStream )
 */
double
UnifReal(double dLow, double dHigh, long nStream)

/*
 * Returns a double uniformly distributed between dLow and dHigh,   
 * excluding the endpoints.  nStream is the random number stream.   
 * Stream 0 is used if nStream is not in the range 0..MAX_STREAM.
 */

{
    double          dTemp;

    if (nStream < 0 || nStream > MAX_STREAM)
        nStream = 0;
    if (dLow == dHigh)
        return (dLow);
    if (dLow > dHigh)
    {
        dTemp = dLow;
        dLow = dHigh;
        dHigh = dTemp;
    }
    Seed[nStream] = NextRand(Seed[nStream]);
    dTemp = ((double) Seed[nStream] / dM) * (dHigh - dLow);
    return (dLow + dTemp);
}



/******************************************************************%

   Exponential:  Yields a real exponentially distributed with given mean

*******************************************************************/

/*
 * double Exponential( double dMean, long nStream )
 */
double
Exponential(double dMean, long nStream)

/*
 * Returns a double uniformly distributed with mean dMean.  
 * 0.0 is returned iff dMean <= 0.0. nStream is the random number 
 * stream. Stream 0 is used if nStream is not in the range 
 * 0..MAX_STREAM.
 */

{
    double          dTemp;

    if (nStream < 0 || nStream > MAX_STREAM)
        nStream = 0;
    if (dMean <= 0.0)
        return (0.0);

    Seed[nStream] = NextRand(Seed[nStream]);
    dTemp = (double) Seed[nStream] / dM;        /* unif between 0..1 */
    return (-dMean * log(1.0 - dTemp));
}

/*
 *  Extensions to dbgen for generation of skewed data.
 *	Surajit Chaudhuri, Vivek Narasayya.
 *  (Jan '99)
 */

#define EPSILON  (0.0001)

/*
 * Round the number x to the nearest integer.
 */
double
round(double x)
{
	double fx = floor(x);
	double cx = ceil(x);

	if((x-fx) <= (cx-x))
		return fx;
	else
		return cx;
}

/*
 * Perform binary search to find D (number of distinct values for given n, zipf)
 *
 */
double 
SolveForMultipler(long n, double zipf)
{
	long lowD = 1;
	long highD = n;
	long mid;
	double sumMult;
	double nRowsPrime;
	long numRows;
	long i;

	while ( (highD - lowD) > 1)
	{
		mid = (highD + lowD) / 2;
		sumMult = 0.0;

		/* compute multiplier */
		for(i=1;i<=mid;i++)
		{
			sumMult += 1.0 / (pow((double) i, zipf));
		}

		/* compute number of rows generated for this mulitpler */
		numRows = 0;
		for(i=1;i<=mid;i++)
		{
			nRowsPrime = ((n / sumMult) / pow((double) i, zipf));
			numRows += (long) round(nRowsPrime);
		}
		if(((double)(n-numRows))/n  < EPSILON)
		{
			break;
		}

		if(numRows > n)
		{
			/* we overestimated numRows -- we need fewer distinct values */
			highD = mid;
		}
		else
		{
			/* underestimation of numRows -- need lower multiplier */
			lowD = mid;
		}
	}
	return sumMult;
}


double
GetMultiplier(long n, double zipf)
{
	double multiplier;
	double term;
	long i;

	if(zipf == 0.0)
		multiplier = n;
	else if(zipf==1.0)
		multiplier = log(n) + 0.577 ;
	else if(zipf==2.0)
		multiplier = (PI*PI)/6.0;
	else if(zipf==3.0)
		multiplier = 1.202;
	else if(zipf==4.0)
		multiplier = (PI*PI*PI*PI)/90.0;
	else
	{

		/* compute multiplier (must be within given bounds) */
		multiplier = 0;
		for(i=1;i<=n;i++)
		{
			term = 1.0 / pow((double)i, zipf);
			multiplier += term;

			/* if later terms add very little we can stop */
			if(term < EPSILON)
				break;
		}
	}
	return multiplier;
}

/******************************************************************%

   RANDOM:  Yields a random number from a Zipfian distribution with 
   a specified skewVal. Skew is an integer in the range 0..3

*******************************************************************/

/*
 *
 */
long 
SkewInt(long nLow, long nHigh, long nStream, double skewVal, long n)
/*
 * 
 */
{
	double	zipf;
	double  dRange;
	long    nTemp;
	double	multiplier;
	double	Czn;
	long	numDistinctValuesGenerated;


	/* check for validity of skewVal */
	if(skewVal < 0 || skewVal > 5)
		zipf = 0; /* assume uniform */
	else if(skewVal==5.0)
	{
		/* special case */
		/* check if any values have been generated for this column */
		if(NumDistinctValuesGenerated[nStream]==0)
		{
			/* pick a skew value to be used for this column*/
			zipf = (int) UnifInt(0,  4, 0);
			ColumnSkewValue[nStream] = zipf;
		}
		else
		{
			/* column skew value has been initialized before */
			zipf = ColumnSkewValue[nStream];
		}
	}
	else
	{
		/* skewVal is between 0 and 4 */
		zipf = skewVal;
	}

	/* If no values have been generated for this stream as yet, get multiplier */
	if(NumDistinctValuesGenerated[nStream]==0)
	{
		Multiplier[nStream] = GetMultiplier(n, zipf);
	}
	multiplier = Multiplier[nStream];

	/* 
	 * Check how many copies of the current value
	 * have already been generated for this stream.
	 * If we have generated enough, proceed to
	 * next value, and decide how many copies of
	 * this value should be generated.
	 */

	if(CurrentValueCounter[nStream] == CurrentValueTarget[nStream])
	{
		/* proceed to next value */
		if (nStream < 0 || nStream > MAX_STREAM)
			nStream = 0;
		if (nLow == nHigh)
			nTemp = nLow;
		else
		{
			if (nLow > nHigh)
			{
				nTemp = nLow;
				nLow = nHigh;
				nHigh = nTemp;
			}
			dRange = (double) (nHigh - nLow + 1);
			Seed[nStream] = NextRand(Seed[nStream]);
			nTemp = (long) (((double) Seed[nStream] / dM) * (dRange));
			nTemp += nLow;
			CurrentValue[nStream] = nTemp;
		}
	}
	else
	{
		/* return another copy of current value */
		nTemp = CurrentValue[nStream];
		CurrentValueCounter[nStream]++;
		return nTemp;
	}

	/*  
	 * check how many distinct values for this column
	 * have already been generated.
	 */
	numDistinctValuesGenerated = NumDistinctValuesGenerated[nStream] + 1;

	if(n<1)
		n = (nHigh - nLow + 1);

	/*
	* zipf is guaranteed to be in the range 0..4 
	* pick the multiplier 
	if(zipf == 0)
		multiplier = n;
	else if(zipf==1)
		multiplier = log(n) + 0.577 ;
	else if(zipf==2)
		multiplier = (PI*PI)/6.0;
	else if(zipf==3)
		multiplier = 1.202;
	else if(zipf==4)
		multiplier = (PI*PI*PI*PI)/90.0;
	*/

	Czn = n/multiplier;
	CurrentValueTarget[nStream]= 
		(long) (Czn/pow((double)numDistinctValuesGenerated, zipf));
	/* ensure that there is at least one value*/
	CurrentValueTarget[nStream] = (CurrentValueTarget[nStream] < 1) ? 1: CurrentValueTarget[nStream];
	CurrentValueCounter[nStream] = 1;
	NumDistinctValuesGenerated[nStream]++;

	return nTemp;
}


/******************************************************************%

   Returns the number of copies of the next value
   in the given distribution. Skew is an integer in the range 0..4

*******************************************************************/
/*
 *
 */
long 
SkewIntCount(long nStream, double skewVal, long n)
/*
 * 
 */
{
	double		zipf;
	double	multiplier;
	double	Czn;
	long	numDistinctValuesGenerated;
	long	numCopies;


	/* check for validity of skewVal */
	if(skewVal < 0 || skewVal > 4)
		zipf = 0; /* assume uniform */
	else if(skewVal==5.0)
	{
		/* column skew value has been initialized before */
		zipf = ColumnSkewValue[nStream];
	}
	else
	{
		/* skewVal is between 0 and 4 */
		zipf = skewVal;
	}

	/*  
	 * check how many distinct values for this column
	 * have already been generated.
	 */
	numDistinctValuesGenerated = NumDistinctValuesGenerated[nStream] + 1;

	multiplier = Multiplier[nStream];
	/*
	* zipf is guaranteed to be in the range 0..4 
	* pick the multiplier 
	if(zipf == 0)
		multiplier = n;
	else if(zipf==1)
		multiplier = log(n) + 0.577 ;
	else if(zipf==2)
		multiplier = (PI*PI)/6.0;
	else if(zipf==3)
		multiplier = 1.202;
	else if(zipf==4)
		multiplier = (PI*PI*PI*PI)/90.0;
	*/

	Czn = n/multiplier;
	numCopies= 
		(long) (Czn/pow((double)numDistinctValuesGenerated, zipf));
	/* ensure that there is at least one value*/
	CurrentValueTarget[nStream] = (CurrentValueTarget[nStream] < 1) ? 1: CurrentValueTarget[nStream];
	NumDistinctValuesGenerated[nStream]++;
	return numCopies;
}