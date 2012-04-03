#  Sccsid:     @(#)makefile.suite	9.1.1.13     12/14/95  16:06:25
# makefile for DSS benchamrk data generator
#
#
################
## CHANGE NAME OF ANSI COMPILER HERE
################
CC      = gcc
# Current values for DATABASE are: INFORMIX, DB2, TDAT (Teradata),
#                                  SQLSERVER, SYBASE
# Current values for PLATFORM are: ATT, DOS, HP, IBM, ICL, MVS,
#                                  SGI, U2200, VMS, WIN32
DATABASE= SQLSERVER
PLATFORM= LINUX
#
CFLAGS  = -O -D$(DATABASE) -D$(PLATFORM) -I/usr/include/sys
#CFLAGS  = -g -DDBNAME=\"dss\" -D$(MACHINE) -D$(DATABASE) -D$(WORKLOAD) -DRNG_TEST -D_FILE_OFFSET_BITS=64 -I/usr/include/sys
LDFLAGS = 
# The OBJ,EXE and LIB macros will need to be changed for compilation under
#  Windows NT
OBJ     = .o
EXE     =
LIBS    = -lm
#
# NO CHANGES SHOULD BE NECESSARY BELOW THIS LINE
###############

PROG1 = dbgen$(EXE)
PROG2 = qgen$(EXE)

HDR1 = dss.h rnd.h config.h dsstypes.h shared.h bcd2.h
SRC1 = build.c driver.c bm_utils.c rnd.c print.c load_stub.c bcd2.c speed_seed.c
OBJ1 = build$(OBJ) driver$(OBJ) bm_utils$(OBJ) rnd$(OBJ) print$(OBJ) load_stub$(OBJ) bcd2$(OBJ) speed_seed$(OBJ)
SETS = dists.dss 
DDL  = dss.ddl dss.ri
OTHER=makefile.suite $(SETS) $(DDL) 10001001
DOC=README History Porting.Notes FAQ
DBGENSRC=$(SRC1) $(HDR1) $(OTHER) $(DOC) $(SRC2) $(HDR2)
SRC2 = qgen.c varsub.c
HDR2 = tpcd.h
OBJ2 = build$(OBJ) bm_utils$(OBJ) qgen$(OBJ) rnd$(OBJ) varsub$(OBJ) 

all: $(PROG1) $(PROG2)

$(PROG1): $(OBJ1) $(SETS) 
	$(CC) $(LDFLAGS) -o $@ $(OBJ1) $(LIBS)

$(PROG2): $(OBJ2)
	$(CC) $(LDFLAGS) -o $@ $(OBJ2) $(LIBS)

clean:
	rm -f $(PROG1) $(PROG2) $(OBJ1) $(OBJ2)
lint:
	lint $(SRC)
	
tar: $(DBGENSRC) 
	tar cvhf $(PROG1).tar $(DBGENSRC) 
dbgenshar: $(DBGENSRC)
	shar -o dbgen.shar $(DBGENSRC)
dbgendisk: $(DBGENSRC)
	for f in $(DBGENSRC) ; \
        do  \
        unix2dos $$f /pcfs/$$f ; \
        done
portable:
	for f in $(DBGENSRC) ; \
        do  \
        expand $$f > /tmp/foo; \
        mv -f /tmp/foo $$f; \
        awk 'length > 72 { print FILENAME ":" NR " too long " }' $$f ; \
        done
rnd$(OBJ): rnd.h
$(OBJ1): config.h makefile
$(OBJ1): dss.h dsstypes.h 
$(OBJ2): dss.h tpcd.h config.h
