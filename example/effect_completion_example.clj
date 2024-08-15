(ns genegraph.effect-completion-example
  (:require [genegraph.framework.app :as app]
            [genegraph.framework.event :as event]
            [genegraph.framework.storage :as storage]
            [genegraph.framework.protocol :as p]
            [io.pedestal.interceptor :as interceptor]
            [io.pedestal.log :as log]))

(def test-publisher
  (interceptor/interceptor
   {:name ::test-publisher
    :enter (fn [e] #_(event/publish e
                                  {::event/topic :out
                                   ::event/key :k
                                   ::event/data :v})
             e)}))

(def test-reader
  (interceptor/interceptor
   {:name ::test-reader
    :enter (fn [e]
             (log/info :fn ::test-reader :msg (::event/data e))
             e)}))

(def test-app-def
  {:type :genegraph-app
   :storage
   {:db
    {:name :db
     :type :rocksdb
     :path "/users/tristan/data/genegraph-neo/effect-rocks/"}}
   :topics
   {:in
    {:name :in
     :type :simple-queue-topic}
    :out
    {:name :out
     :type :simple-queue-topic}}
   :processors
   {:test-processor
    {:type :processor
     :name :test-processor
     :subscribe :in
     :interceptors [test-publisher]}
    :test-reader
    {:type :processor
     :name :test-reader
     :subscribe :out
     :interceptors [test-reader]}}})

(comment
  (def test-app (p/init test-app-def))
  (p/start test-app)
  (p/stop test-app)

  (let [p (promise)]
    (p/publish (get-in test-app [:topics :in])
               {::event/key :k
                ::event/data {:a :a}
                ::event/completion-promise p})
    (deref p 100 :timeout))
  
  )
