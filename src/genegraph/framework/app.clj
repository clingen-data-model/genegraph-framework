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
