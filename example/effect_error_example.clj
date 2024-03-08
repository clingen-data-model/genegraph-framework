(ns genegraph.effect-error-example
  (:require [genegraph.framework.app :as app]
            [genegraph.framework.protocol :as p]
            [genegraph.framework.storage :as storage]
            [genegraph.framework.storage.rdf :as rdf]
            [genegraph.framework.storage.rdf.names :as names]
            [genegraph.framework.event :as event]
            [genegraph.framework.processor :as processor]
            [io.pedestal.log :as log]
            [io.pedestal.interceptor :as interceptor]
            [clojure.java.io :as io])
  (:import [java.time Instant]))

(defn store-something-fn [e]
  (let [target (:target e :test-rocks)]
    (event/store e target (::event/key e) (::event/data e))))

(def store-something
  {:name ::store-something
   :enter (fn [e] (store-something-fn e))})

(def test-def
  {:type :genegraph-app
   :topics
   {:test-topic
    {:name :test-topic
     :type :simple-queue-topic}}
   :storage
   {:test-rocks
    {:name :test-rocks
     :type :rocksdb
     :path "/Users/tristan/data/genegraph-neo/test-rocks-2"}
    :test-jena
    {:name :test-jena
     :type :rdf
     :path "/Users/tristan/data/genegraph-neo/test-jena-2"}}
   :processors
   {:test-processor
    {:name :test-processor
     :type :processor
     :subscribe :test-topic
     :interceptors [store-something]}}})

(names/add-prefixes
 {"rdfs" "http://www.w3.org/2000/01/rdf-schema#"
  "rdf" "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
  "dc" "http://purl.org/dc/terms/"
  "ex" "http://example/"})

(comment
  (def test-app (p/init test-def))

  (p/start test-app)
  (p/stop test-app)


  (def r
    (p/process (get-in test-app [:processors :test-processor])
               {::event/key :k
                ::event/data {:a :a}
                ::event/completion-promise (promise)}))

  (def rj
    (p/process (get-in test-app [:processors :test-processor])
               {::event/key "http://example/example"
                ::event/data (rdf/statements->model
                              [[:ex/thing4 :rdf/type :rdfs/Class]])
                :target :test-jena
                ::event/completion-promise (promise)}))
  (processor/effect-error r)
  (-> rj
      ::event/effects
      first
      :commit-promise
      (deref 100 :timeout)
      
      )
  (deref (::event/completion-promise r) 100 :timeout)

  (let [p (promise)]
    (p/publish (get-in test-app [:topics :test-topic])
               {::event/key :k
                ::event/data (fn [x] (println x)) ; indigestible, throws exception
                ::event/completion-promise p})
    (deref p 500 :timeout))

  (storage/read @(get-in test-app [:storage :test-rocks :instance]) :k)
  (let [db @(get-in test-app [:storage :test-jena :instance])]
    (rdf/tx db
      (str (storage/read db "http://example/example"))))
  
  (ex-data (ex-info "Hi"))
  )
