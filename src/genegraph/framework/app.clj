(ns genegraph.framework.app
  "Core include refrerencing all necessary dependencies"
  (:require [genegraph.framework.http-server]
            [genegraph.framework.processor :as processor]
            [genegraph.framework.protocol :as p]
            [genegraph.framework.storage :as s]
            [genegraph.framework.storage.rocksdb]
            [genegraph.framework.storage.jdbc]
            [genegraph.framework.storage.rdf :as rdf]
            [genegraph.framework.storage.gcs]
            [genegraph.framework.storage.atom]
            [genegraph.framework.kafka :as kafka]
            [genegraph.framework.kafka.admin :as kafka-admin]
            [genegraph.framework.topic :as topic]
            [genegraph.framework.event :as event]
            [io.pedestal.http :as http]
            [io.pedestal.interceptor :as interceptor]
            [io.pedestal.log :as log]
            [clojure.spec.alpha :as spec]))

(spec/def ::app
  (spec/keys :req-un [::topics]))

(defrecord App [topics storage processors http-servers]

  p/Lifecycle
  (start [this]
    (run! #(when (satisfies? p/Lifecycle %) (p/start %))
          (concat (vals storage)
                  (vals topics)
                  (vals processors)
                  (vals http-servers)))
    this)
  (stop [this]
    (run! #(when (satisfies? p/Lifecycle %) (p/stop %))
          (concat (vals http-servers)
                  (vals processors)
                  (vals storage)
                  (vals topics)))
    this))


(defn create-system-topic []
  (p/init
   {:type :simple-queue-topic
    :name :system}))

(defn- init-components [def components]
  (reduce
   (fn [a k] (update a k update-vals p/init))
   def
   components))

(defn- add-kafka-cluster-refs [app component]
  (update app
          component
          (fn [entities]
            (update-vals
             entities
             (fn [e]
                 (update
                  e
                  :kafka-cluster
                  (fn [cluster]
                    (get-in app [:kafka-clusters cluster]))))))))

(defn- add-topic-and-storage-refs [app component]
  (update
   app
   component
   update-vals
   #(merge % (select-keys app [:topics :storage :kafka-clusters]))))

(defn- replace-kw-ref-for-http-server [server app]
  (assoc server
         :endpoints
         (mapv (fn [ep]
                 (update ep :processor (fn [p] (get-in app [:processors p]))))
               (:endpoints server))))

(defn- add-processor-refs-to-endpoints [app]
  (update
   app
   :http-servers
   update-vals
   #(replace-kw-ref-for-http-server % app)))

(defn add-system-topic-ref-to-components [app system-topic components]
  (reduce
   (fn [a c] (update a c
                     (fn [c1]
                       (update-vals c1 #(assoc % :system-topic system-topic)))))
   app
   components))

;;;; system processor

(def log-system-event-interceptor
  (interceptor/interceptor
   {:name ::log-system-event-interceptor
    :enter (fn [e]
             (p/log-event e)
             e)}))

(defn register-listener [event]
  (when (= :register-listener (:type event))
    (swap! (::listeners event)
           assoc
           (:name event)
           (select-keys event [:promise :predicate :name])))
  event)

(defn trigger-listeners [event]
  (log/info :fn ::trigger-listeners :listeners (::listeners event))  
  (run! (fn [listener]
          (deliver (:promise listener) (dissoc event ::listeners)))
        (filter #((:predicate %) event)
                (vals @(::listeners event))))
  event)

(defn handle-listeners [event]
  (log/info :fn ::handle-listeners :listeners (::listeners event))
  (-> event
      register-listener
      trigger-listeners))

(def handle-listeners-interceptor
  (interceptor/interceptor
   {:name ::handle-listeners
    :enter (fn [e] (handle-listeners e))}))

(def default-system-processor 
  {:subscribe :system
   :name :default-system-processor
   :type :processor
   :interceptors [log-system-event-interceptor
                  handle-listeners-interceptor]
   :init-fn #(assoc % ::event/metadata {::listeners (atom {})})})

;;; /system processor

(defn has-custom-system-processor? [app]
  (some #(= :system (:subscribe %)) (:processors app)))

(defn add-default-system-processor [app]
  (if (has-custom-system-processor? app)
    app
    (assoc-in app
              [:processors :default-system-processor]
              default-system-processor)))

(defmethod p/init :genegraph-app [app]
  (let [system-topic (create-system-topic)]
    (-> app
        add-default-system-processor
        (add-kafka-cluster-refs :topics)
        (add-kafka-cluster-refs :processors)
        (add-system-topic-ref-to-components
         system-topic
         [:topics :storage :processors :http-servers])
        (init-components [:topics :storage])
        (assoc-in [:topics :system] system-topic)
        (add-topic-and-storage-refs :processors)
        (init-components [:processors])
        add-processor-refs-to-endpoints
        (init-components [:http-servers])
        map->App)))


(defmethod p/log-event :default [e]
  (log/info :source (:source e)
            :type (:type e)))

(defmethod p/log-event :component-state-update [e]
  (log/info :source (:source e)
            :type (:type e)
            :state (:state e)))

;;;;;
;; Stuff for testing
;;;;;



(defn consumer-group-msg-interceptor [event]
  (Thread/startVirtualThread
   #(let [r @(::event/completion-promise event)]
      (log/info :msg "effect complete" :result r :offset (::event/offset event))))
  (event/store event
               :test-jena
               "http://example.com/test-graph"
               (rdf/statements->model
                [[:sepio/GeneValidityEvidenceLevelAssertion
                  :rdf/type
                  :sepio/Assertion]])))

#_(p/publish (get-in a2 [:topics :publish-to-test])
           {:payload
            {::event/key "k18"
             ::event/value "v19"
             ::event/topic :test-topic}
            #_#_#_#_::event/skip-local-effects true
              ::event/skip-publish-effects true})

#_(-> event
      (event/publish {::event/key (str "new-" (::event/key event))
                      ::event/data {:hgvs "NC_00000001:50000A>C"}
                      ::event/topic :test-endpoint}))


(defn test-interceptor-fn [event]
  (consumer-group-msg-interceptor event))

(defn test-publisher-fn [event]
  (event/publish event (:payload event)))

(defn test-endpoint-fn [event]
  (assoc event
         :response
         {:status 200
          :body "Hello, flower"}))

(def app-def-2
  {:type :genegraph-app
   :kafka-clusters {:local
                    {:common-config {"bootstrap.servers" "localhost:9092"}
                     :producer-config {"key.serializer"
                                       "org.apache.kafka.common.serialization.StringSerializer",
                                       "value.serializer"
                                       "org.apache.kafka.common.serialization.StringSerializer"}
                     :consumer-config {"key.deserializer"
                                       "org.apache.kafka.common.serialization.StringDeserializer"
                                       "value.deserializer"
                                       "org.apache.kafka.common.serialization.StringDeserializer"}}}
   :topics {:test-topic
            {:name :test-topic
             :type :kafka-consumer-group-topic
             :kafka-consumer-group "testcg9"
             :kafka-cluster :local
             :kafka-topic "test"}
            :test-reader
            {:name :test-reader
             :type :kafka-reader-topic
             :kafka-cluster :local
             :kafka-topic "test"}
            :test-endpoint
            {:name :test-endpoint
             :type :kafka-producer-topic
             :serialization :json
             :kafka-topic "test-out"
             :kafka-cluster :local}
            :test-input
            {:name :test-input
             :type :kafka-producer-topic
             :serialization :json
             :kafka-topic "test"
             :kafka-cluster :local}
            :publish-to-test
            {:name :publish-to-test
             :type :simple-queue-topic}}
   :storage {:test-rocksdb
             {:type :rocksdb
              :name :test-rocksdb
              :path "/users/tristan/desktop/test-rocks"}
             :test-jena
             {:type :rdf
              :name :test-jena
              :path "/users/tristan/desktop/test-jena"}}
   :processors {:test-processor
                {:subscribe :test-topic
                 :name :test-processor
                 :type :parallel-processor
                 :kafka-cluster :local
                 :interceptors `[test-interceptor-fn]}
                :test-publisher
                {:name :test-publisher
                 :subscribe :publish-to-test
                 :kafka-cluster :local
                 :type :processor
                 :interceptors `[test-publisher-fn]}
                :test-reader-processor
                {:name :test-reader-processor
                 :subscribe :test-reader
                 :kafka-cluster :local
                 :type :processor
                 :backing-store :test-jena
                 :interceptors `[test-publisher-fn]}
                :test-endpoint
                {:name :test-endpoint
                 :type :processor
                 :interceptors `[test-endpoint-fn]}}
   :http-servers {:test-server
                  {:type :http-server
                   :name :test-server
                   :endpoints [{:path "/hello"
                                :processor :test-endpoint}]
                   ::http/type :jetty
                   ::http/port 8888
                   ::http/join? false}}})




(defn print-event [event]
  (clojure.pprint/pprint event)
  event)

(def app-def-3
  {:type :genegraph-app
   :topics {:test-topic
            {:name :test-topic
             :type :simple-queue-topic}}
   :processors {:test-processor
                {:subscribe :test-topic
                 :name :test-processor
                 :type :processor
                 :interceptors `[print-event]
                 :init-fn (fn [this]
                            (assoc this ::event/metadata {::local-conf (:storage this)}))}}})


(def gcs-handle
  {:type :gcs
   :bucket "genegraph-framework-dev"})

(def fs-handle
  {:type :file
   :base "/users/tristan/data/genegraph-neo/"})

(comment


  (def a3 (p/init app-def-3))

  (p/start a3)
  (p/stop a3)

  (p/publish (get-in a3 [:topics :test-topic])
             {::event/key "akey"
              ::event/value "avalue"})

  )

(comment

  (def a2 (p/init app-def-2))
  (p/start a2)
  (p/stop a2)

  (-> a2 :topics :test-topic :initial-consumer-group-offset)
  (-> a2 :topics :test-topic :end-offset-at-start)
  (-> a2 :topics :test-topic :state deref)
  (-> a2 :topics :test-topic kafka/topic-up-to-date?)

  (-> a2 :processors :default-system-processor)
  
  (p/publish (get-in a2 [:topics :publish-to-test])
             {:payload
              {::event/key "k18"
               ::event/value "v19"
               ::event/topic :test-topic}
              #_#_#_#_::event/skip-local-effects true
              ::event/skip-publish-effects true})



  (p/publish (get-in a2 [:processors :test-processor :system-topic])
             {:key :k :type :system-event})

  (p/publish-system-update (get-in a2 [:topics :test-topic])
                           {:key :k :type :system-event})

  (def lp1 (promise))

  lp1

  (p/publish-system-update (get-in a2 [:processors :test-processor])
                           {:type :register-listener
                            :predicate #(= :system-event (:type %))
                            :promise lp1})

  (-> a2 :processors :test-processor :system-topic)
  (-> a2 :topics :system)
  
  (time
   (s/store-snapshot (get-in a2 [:storage :test-jena])
                     gcs-handle))
  (time
   (s/restore-snapshot (get-in a2 [:storage :test-jena])
                       gcs-handle))

  (s/store-offset @(get-in a2 [:storage :test-rocksdb :instance]) :test-topic 1)
  (s/retrieve-offset @(get-in a2 [:storage :test-rocksdb :instance]) :test-topic)
  (processor/starting-offset (get-in a2 [:processors :test-processor]))
  (get-in a2 [:processors :test-processor :storage :test-rocksdb :instance])

  (def lp2 (promise))

  (-> {::listeners (atom {:promise lp2
                          :predicate #(= :system-event (:type %))})
       :type :system-event
       :key :boo}
      ::listeners
      deref
      vals)

  #_(run! #(deliver (:promise %) event)
          (filter #((:predicate %) event)
                  (:vals @(::listeners event))))
  )


