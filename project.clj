(defproject trident-cassandra "0.0.1-wip1"
  :source-path "src/clj"
  :java-source-path "src/jvm"
  :javac-options {:debug "true" :fork "true"}
  :jvm-opts ["-Djava.library.path=/usr/local/lib:/opt/local/lib:/usr/lib"]
  :dependencies [
                 [org.hectorclient/hector-core "1.1-1"
                  :exclusions [com.google.guava/guava]]
                ]
  :dev-dependencies [
                     [org.clojure/clojure "1.4.0"]
                     [storm "0.8.1"]
                    ]
  :aot :all
)
