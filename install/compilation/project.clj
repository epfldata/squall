(defproject squall "2.0"
  :java-source-path "../dip/Squall/src;../dip/SQLtoQueryPlanPlugin/src"
  :javac-options {:debug "true" :fork "true"}
  :aot :all
  :jvm-opts ["-Djava.library.path=/usr/local/lib:/opt/local/lib:/usr/lib"
            ]

  :dependencies [                 
		 [jsqlparser "0.7.0"]
                 ]

  :dev-dependencies [[storm "0.7.0"]
                    ])