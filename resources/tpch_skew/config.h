/* 
 * Sccsid:     @(#)config.h	9.1.1.21     1/23/96  09:39:23 
 * 
 * this file allows the compilation of DBGEN to be tailored to specific
 * architectures and operating systems. Some options are grouped 
 * together to allow easier compilation on a given vendor's hardware.
 * 
 * The following #defines will effect the code:
 *   KILL(pid)         -- how to terminate a process in a parallel load
 *   SPAWN             -- name of system call to clone an existing process
 *   SET_HANDLER(proc) -- name of routine to handle signals in parallel load
 *   WAIT(res, pid)    -- how to await the termination of a child
 *   SEPARATOR         -- character used to separate fields in flat files
 *   DBNAME            -- default name of database to be loaded
 *   STDLIB_HAS_GETOPT -- to prevent confilcts with gloabal getopt() 
 *   MDY_DATE          -- generate dates as MM-DD-YY
 *   WIN32             -- support for WindowsNT
 *   SUPPORT_64BITS    -- compiler defines a 64 bit datatype
 *   DSS_HUGE          -- 64 bit data type
 *   HUGE_FORMAT       -- printf string for 64 bit data type
 *
 *   OS defines
 *   ==========
 *   ATT        -- getopt() handling
 *   DOS        -- disable all multi-user functionality/dependency
 *   HP         -- posix source inclusion differences
 *   IBM        -- posix source inclusion differences
 *   ICL        -- getopt() handling
 *   MVS        -- special handling of varchar format
 *   SGI        -- getopt() handling
 *   SUN        -- getopt() handling
 *   U2200      -- death of parent kills children automatically
 *   VMS        -- signal/fork handing differences
 *
 *   Database defines
 *   ================
 *   DB2        -- use DB2 dialect in QGEN
 *   INFORMIX   -- use Informix dialect in QGEN
 *   SQLSERVER  -- use SQLSERVER dialect in QGEN
 *   SYBASE     -- use Sybase dialect in QGEN
 *   TDAT       -- use Teradata dialect in QGEN
 */

#ifdef DOS
#define DSS_PROC        1
#define PATH_SEP	'\\'
#else


#ifdef ATT
#define STDLIB_HAS_GETOPT
#ifdef SQLSERVER
#define WIN32
#endif
#endif /* ATT */

#ifdef HP
#define _INCLUDE_POSIX_SOURCE
#endif /* HP */

#ifdef IBM
#define _POSIX_SOURCE
/*
 * if the C compiler is 3.1 or later, then uncomment the
 * lines for 64 bit seed generation
 */
/* #define SUPPORT_64BITS /* */
/* #define DSS_HUGE long long /* */
#define STDLIB_HAS_GETOPT
#endif /* IBM */

#ifdef ICL
#define STDLIB_HAS_GETOPT
#endif /* ICL */

#ifdef SUN
#define STDLIB_HAS_GETOPT
#endif /* SUN */

#ifdef SGI
#define STDLIB_HAS_GETOPT
#define SUPPORT_64BITS
#define DSS_HUGE __uint64_t
#endif /* SGI */

#ifdef VMS
#define SPAWN   vfork
#define KILL(pid) kill(SIGQUIT, pid)
#define SET_HANDLER(proc) signal(SIGQUIT, proc)
#define WAIT(res, pid) wait(res)
#define SIGS_DEFINED
#endif /* VMS */

#if (defined(WIN32)&&!defined(_POSIX_))
#define pid_t int
#define SET_HANDLER(proc) signal(SIGINT, proc)
#define KILL(pid) \
     TerminateProcess(OpenProcess(PROCESS_TERMINATE,FALSE,pid),3)
#define SPAWN()   _spawnv(_P_NOWAIT, spawn_args[0], spawn_args)
#define WAIT(res, pid) _cwait(res, pid, _WAIT_CHILD)
#define getpid		_getpid
#define SIGS_DEFINED
#define PATH_SEP	'\\'
/* #define SUPPORT_64BITS */
#define DSS_HUGE __int64
#endif /* WIN32 */

#ifndef SIGS_DEFINED
#define KILL(pid) kill(SIGUSR1, pid)
#define SET_HANDLER(proc) signal(SIGUSR1, proc)
#define SPAWN   fork
#define WAIT(res, pid) wait(res)
#endif /* DEFAULT */

#define DSS_PROC        getpid()
#endif /* DOS */

#ifndef DBNAME
#define DBNAME "dss"
#endif /* DBNAME */

#ifndef PATH_SEP
#define PATH_SEP '/'
#endif /* PATH_SEP */
