/* Sccsid:     @(#)varsub.c	9.1.1.25     5/1/96  11:52:23 */
#include <stdio.h>
#ifndef _POSIX_SOURCE
#include <malloc.h>
#endif /* POSIX_SOURCE */
#if (defined(_POSIX_)||!defined(WIN32))
#include <unistd.h>
#endif /* WIN32 */
#include <string.h>
#include "config.h"
#include "dss.h"
#include "tpcd.h"
#define TYPE_CNT        8
extern long Seed[];
extern char **asc_date;
extern double flt_scale;

char *defaults[19][11] =
    {
    {"90",              NULL,                   NULL,
        NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL},  /* 1 */
    {"15",              "BRASS",                "EUROPE",
        NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL},  /* 2 */
    {"BUILDING",        "1995-03-15",           NULL,
        NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL},  /* 3 */
    {"1993-07-01",      NULL,                   NULL,
        NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL},  /* 4 */
    {"ASIA",            "1994-01-01",           NULL,
        NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL},  /* 5 */
    {"1994-01-01",      ".06",                  "24",
        NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL},  /* 6 */
    {"FRANCE",          "GERMANY",              NULL,
        NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL},  /* 7 */
    {"BRAZIL",          "AMERICA",      "ECONOMY ANODIZED STEEL",
        NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL},/* 8 */
    {"green",         NULL,                   NULL,
        NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL},  /* 9 */
    {"1993-10-01",      NULL,                   NULL,
        NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL},  /* 10 */
    {"GERMANY",         "0.001",                 NULL,
        NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL},  /* 11 */
    {"MAIL",            "SHIP",                 "1994-01-01",
        NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL},  /* 12 */
    {"Clerk#000000088", NULL,                   NULL,
        NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL},  /* 13 */
    {"1995-09-01",      NULL,                   NULL,
        NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL},  /* 14 */
    {"1996-01-01",      NULL,                   NULL,
        NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL},  /* 15 */
    {"Brand#45",        "MEDIUM POLISHED", "49",
                "14","23","45","19","3","36","9", NULL}, /* 16 */
    {"Brand#23",        "MED BOX",               NULL,
        NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL},  /* 17 */
    {NULL,              NULL,                   NULL,
        NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL},  /* UF1 */
    {NULL,              NULL,                   NULL,
        NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL},  /* UF2 */
    };
void
varsub(int qnum, int vnum, int flags)
{
    static char param[11][128];
    static FILE *lfp = NULL;
    char *ptr;
    int i = 0,
        size[10],
        tmp_date;

    if (vnum == 0)
    {
    if ((flags & DFLT) == 0)
    {
    switch(qnum)
        {
        case 1:
            sprintf(param[1], "%d", UnifInt((long)60,(long)120,(long)qnum));
            param[2][0] = '\0';
            break;
        case 2:
            sprintf(param[1], "%d", 
                UnifInt((long)P_SIZE_MIN, (long)P_SIZE_MAX, qnum));
            pick_str(&p_types_set, qnum, param[3]);
            ptr = param[3] + strlen(param[3]);
            while (*(ptr - 1) != ' ') ptr--;
            strcpy(param[2], ptr);
            pick_str(&regions, qnum, param[3]);
            param[4][0] = '\0';
            break;
        case 3:
            pick_str(&c_mseg_set, qnum, param[1]);
            /*
             * pick a random offset within the month of march and add the
             * appropriate magic numbers to position the output functions 
             * at the start of March '95
             */
            tmp_date = UnifInt((long)0, (long)30, (long)qnum);
	    strcpy(param[2], *(asc_date + tmp_date + 1155));
            if (oldtime) 
                {
                for (i=0;strcmp(*(asc_date + i), param[2]); i++);
                sprintf(param[2],"%ld", julian(i + STARTDATE));
                }
            param[3][0] = '\0';
            break;
        case 4:
            tmp_date = UnifInt(1,58,qnum);
            sprintf(param[1],"19%02d-%02d-01", 
                93 + tmp_date/12, tmp_date%12 + 1);
            if (oldtime) 
                {
                for (i=0;strcmp(*(asc_date + i), param[1]); i++);
                sprintf(param[1],"%ld", julian(i + STARTDATE));
                }
            param[2][0] = '\0';
            break;
        case 5:
            pick_str(&regions, qnum, param[1]);
            tmp_date = UnifInt((long)93,(long)97,(long)qnum);
            if (oldtime) sprintf(param[2],"%ld", tmp_date);
            else sprintf(param[2], "19%d-01-01", tmp_date);
            param[3][0] = '\0';
            break;
        case 6:
            tmp_date = UnifInt(93,97,qnum);
            if (oldtime) 
                sprintf(param[1],"%ld001", tmp_date);
            else 
                sprintf(param[1], "19%d-01-01", tmp_date);
            sprintf(param[2], "0.0%d", UnifInt(2, 9, qnum));
            sprintf(param[3], "%d", UnifInt((long)24, (long)25, (long)qnum));
            param[4][0] = '\0';
            break;
        case 7:
            tmp_date = pick_str(&nations2, qnum, param[1]);
            while (pick_str(&nations2, qnum, param[2]) == tmp_date);
            param[3][0] = '\0';
            break;
        case 8:
            tmp_date = pick_str(&nations2, qnum, param[1]);
            tmp_date = nations.list[tmp_date].weight;
            strcpy(param[2], regions.list[tmp_date].text);
            pick_str(&p_types_set, qnum, param[3]);
            param[4][0] = '\0';
            break;
        case 9:
            pick_str(&colors, qnum, param[1]);
            param[2][0] = '\0';
            break;
        case 10:
            tmp_date = UnifInt(1,24,qnum);
            sprintf(param[1],"19%02d-%02d-01", 
                93 + tmp_date/12, tmp_date%12 + 1);
            if (oldtime)
                {
                for (i=0;strcmp(*(asc_date + i), param[1]); i++);
                sprintf(param[1],"%ld", julian(i + STARTDATE));
                }
            param[2][0] = '\0';
            break;
        case 11:
            pick_str(&nations2, qnum, param[1]);
            sprintf(param[2], "%11.10f", Q11_FRACTION / flt_scale );
            param[3][0] = '\0';
            break;
        case 12:
            tmp_date = pick_str(&l_smode_set, qnum, param[1]);
            while (tmp_date == pick_str(&l_smode_set, qnum, param[2]));
            tmp_date = UnifInt(93,97,qnum);
            if (oldtime) sprintf(param[3],"%d", tmp_date*1000 + 1);
            else sprintf(param[3], "19%d-01-01", tmp_date);
            param[4][0] = '\0';
            break;
        case 13:
            sprintf(param[1], O_CLRK_FMT, O_CLRK_TAG, 
                UnifInt((long)1, 
                       (long) MAX((scale * O_CLRK_SCL), O_CLRK_SCL),
                       (long)qnum));

            param[2][0] = '\0';
            break;
        case 14:
            tmp_date = UnifInt(1,60,qnum);
            sprintf(param[1],"19%02d-%02d-01", 
                93 + tmp_date/12, tmp_date%12 + 1);
            if (oldtime)
                {
                for (i=0;strcmp(*(asc_date + i), param[1]); i++);
                sprintf(param[1],"%ld", julian(i + STARTDATE));
                }
            param[2][0] = '\0';
            break;
        case 15:
            tmp_date = UnifInt(1,58,qnum);
            sprintf(param[1],"19%02d-%02d-01", 
                93 + tmp_date/12, tmp_date%12 + 1);
            if (oldtime)
                {
                for (i=0;strcmp(*(asc_date + i), param[1]); i++);
                sprintf(param[1],"%ld", julian(i + STARTDATE));
                }
            param[2][0] = '\0';
            break;
        case 16:
            sprintf(param[1], "Brand#%d%d", 
                UnifInt(1, 5, qnum), 
                UnifInt(1, 5, qnum));
            pick_str(&p_types_set, qnum, param[2]);
            ptr = param[2] + strlen(param[2]);
            while (*(--ptr) != ' ');
            *ptr = '\0';
            i=0;
next:
            size[i] = UnifInt(1, 50, qnum);
            tmp_date = 0;
            while (tmp_date < i)
                if (size[i] == size[tmp_date])
                    goto next;
                else
		    tmp_date++;
            sprintf(param[i + 3], "%d", size[i]);
            if (++i <= TYPE_CNT)
                goto next;
            param[i + 2][0] = '\0';
            break;
        case 17:
            sprintf(param[1], "Brand#%ld%ld", 
                UnifInt(1, 5, qnum), 
                UnifInt(1, 5, qnum));
            pick_str(&p_cntr_set, qnum, param[2]);
            param[3][0] = '\0';
            break;
        case 18:
        case 19:
                break;
        default:
            fprintf(stderr, 
                "No variable definitions available for query %d\n", 
                    qnum);
        }
    }

    if (flags & LOG)
        {
        if (lfp == NULL)
            {
            lfp = fopen(lfile, "a");
            OPEN_CHECK(lfp, lfile);
            }
        fprintf(lfp, "%d", qnum);
        for (i=1; i <= 10; i++)
            if (flags & DFLT)
		{
		if (defaults[i - 1] == NULL)
		    break;
		else
		    fprintf(lfp, "\t%s", defaults[i - 1]);
		}
            else
		{
		if (param[i][0] == '\0')
		    break;
		else
		    fprintf(lfp, "\t%s", param[i]);
		}
        fprintf(lfp, "\n");
        }
    }
    else
        {
        if (flags & DFLT)   
            {
            /* to allow -d to work at all scale factors */
            if (qnum == 11 && vnum == 2)
                fprintf(ofp, "%11.10f", Q11_FRACTION/flt_scale);
            else
                if (defaults[qnum - 1][vnum - 1])
                    fprintf(ofp, "%s", defaults[qnum - 1][vnum - 1]);
                else
                   fprintf(stderr, 
                       "Bad default request (q: %d, p: %d)\n",
                        qnum, vnum);
            }
        else        
            {
            if (param[vnum])
                fprintf(ofp, "%s", param[vnum]);
            else
               fprintf(stderr, "Bad parameter request (q: %d, p: %d)\n",
                    qnum, vnum);
            }
        }
    return;
}
