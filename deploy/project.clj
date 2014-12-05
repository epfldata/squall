(defproject squall "0.2.0"
  :java-source-path "../src"
  :javac-options {:debug "false" :fork "true" :target "1.7" :source "1.7"}
  :aot :all
  :jvm-opts ["-Xmx1024m" "-Djava.library.path=/usr/local/lib:/opt/local/lib:/usr/lib"]

  :dev-dependencies [
           [ujmp "0.2.5"]
		     [jsqlparser "0.7.0"]
		     [trove "3.0.2"]
		     [bheaven "0.0.3"]
		     [opencsv "2.3"]
		     [bdb-je "5.0.84"]

		     ; [org.clojure/clojure "1.4.0"]
		     [org.clojure/clojure "1.5.1"]
           [org.apache.storm/storm-core "0.9.2-incubating"]


					  ; for Apache Storm 0.9.2
       		     ; [org.clojure/clojure "1.5.1"]
                 ; [org.apache.storm/storm-core "0.9.2-incubating"]

					  ; for Storm 0.8.2
       		     ; [org.clojure/clojure "1.4.0"]
                 ; [storm "0.8.2"] 

					  ; for Storm 0.8.3 (unreleased)
       		     ; [org.clojure/clojure "1.4.0"]
                 ;        have to use storm-0.8.3/project.clj
                 ; [storm "0.8.3"]
                 ; [commons-io "1.4"]
                 ; [org.apache.commons/commons-exec "1.1"]
                 ; [storm/libthrift7 "0.7.0"]
                 ; [clj-time "0.4.1"]
                 ; [log4j/log4j "1.2.16"]
                 ; [com.netflix.curator/curator-framework "1.0.1"]
                 ; [backtype/jzmq "2.1.0"]
                 ; [com.googlecode.json-simple/json-simple "1.1"]
                 ; [compojure "1.1.3"]
                 ; [hiccup "0.3.6"]
                 ; [ring/ring-jetty-adapter "0.3.11"]
                 ; [org.clojure/tools.logging "0.2.3"]
                 ; [org.clojure/math.numeric-tower "0.0.1"]
                 ; [org.slf4j/slf4j-log4j12 "1.5.8"]
                 ; [storm/carbonite "1.5.0"]
                 ; [org.yaml/snakeyaml "1.9"]
                 ; [org.apache.httpcomponents/httpclient "4.1.1"]
                 ; [storm/tools.cli "0.2.2"]
                 ; [com.googlecode.disruptor/disruptor "2.10.1"]
                 ; [storm/jgrapht "0.8.3"]
                 ; [com.google.guava/guava "13.0"]
                    ])
