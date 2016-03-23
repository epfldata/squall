#!/bin/bash

#cp /home/vitorovi/Desktop/EWHSampleMatrixBolt_inpuCoarsener.java /home/vitorovi/working/installations/storm/squall/squall-core/src/main/java/ch/epfl/data/squall/ewh/storm_components/EWHSampleMatrixBolt.java
#./upconf_loop_cluster.sh ../resources/storm-linux_16GB.yaml ../experiments/histogram_vldb_2016/b_icd/Xmx16GB_inputCoarsener
#cp /home/vitorovi/Desktop/EWHSampleMatrixBolt_orig.java /home/vitorovi/working/installations/storm/squall/squall-core/src/main/java/ch/epfl/data/squall/ewh/storm_components/EWHSampleMatrixBolt.java

./upconf_loop_cluster.sh ../resources/storm-linux_4GB.yaml ../experiments/histogram_vldb_2016/e_ocd_scale_32J/Xmx4GB-remains/
