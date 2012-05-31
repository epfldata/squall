(defproject squall "2.0"
  :java-source-path "src/squall_plan_runner/src;src/squall/src"
  :javac-options {:debug "true" :fork "true"}
  :aot :all
  :jvm-opts ["-Djava.library.path=/usr/local/lib:/opt/local/lib:/usr/lib"
            ]

  :dev-dependencies [
		     [storm "0.7.0"]
		     [jsqlparser "0.7.0"]
		     [trove "3.0.2"]
                    ])