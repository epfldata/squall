(defproject squall "2.0"
  :java-source-path "../src"
  :javac-options {:debug "true" :fork "true"}
  :aot :all
  :jvm-opts ["-Djava.library.path=/usr/local/lib:/opt/local/lib:/usr/lib"
            ]

  :dependencies [
		     [jsqlparser "0.7.0"]
		     [trove "3.0.2"]
                    ]

  :dev-dependencies [
		     [org.clojure/clojure "1.2.0"]
                     [org.clojure/clojure-contrib "1.2.0"]
		     [storm "0.7.0"]
                    ])