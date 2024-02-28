(ns genegraph.control-plane-example
  (:require [genegraph.framework.app :as app]
            [genegraph.framework.protocol :as p]
            [genegraph.framework.storage :as storage]
            [genegraph.framework.storage.rdf :as rdf]
            [genegraph.framework.event :as event]
            [clojure.java.io :as io]
            [io.pedestal.log :as log]
            [io.pedestal.interceptor :as interceptor]))

(def test-base-path
  "/users/tristan/data/genegraph-neo/control-plane-test/")

(def mark-event-interceptor
  (interceptor/interceptor
   {:name ::mark-event-interceptor
    :leave (fn [e]
             (log/info :interceptor ::mark-event-interceptor)
             e)}))

(def store-data-interceptor
  (interceptor/interceptor
   {:name ::store-jena-data-interceptor
    :leave (fn [{k ::event/key
                 v ::event/data
                 :as e}]
             (log/info :interceptor ::store-data-interceptor
                       :k k
                       :v v)
             (-> e 
                 (event/store :test-jena k v)
                 (event/store :test-rocks k v)))}))

(def control-plane-app-def
  {:type :genegraph-app
   :storage {:test-rocks
             {:name :test-rocks
              :type :rocksdb
              :path (str test-base-path "test-rocks")
              :snapshot-handle {:type :file
                                :base test-base-path
                                :path "rocks-snapshot.tar.lz4"}}
             :test-jena
             {:name :test-jena
              :type :rdf
              :path (str test-base-path "test-jena")
              :load-snapshot true
              :snapshot-handle {:type :file
                                :base test-base-path
                                :path "jena-snapshot.nq.lz4"}}}
   :topics {:control-plane
            {:name :control-plane
             :type :simple-queue-topic}}
   :processors {:control-plane-processor
                {:name :control-plane-processor
                 :type :processor
                 :subscribe :control-plane
                 :interceptors
                 [store-data-interceptor]}}})

(comment
  (def control-plane-app
    (p/init control-plane-app-def))

  (p/start control-plane-app)
  (p/stop control-plane-app)

  (storage/store-snapshot (get-in control-plane-app [:storage :test-jena]))

  
  (p/publish (get-in control-plane-app [:topics :control-plane])
             {::event/key "http://example.com/test"
              ::event/data (rdf/statements->model
                            [[(rdf/resource "http://example.com/test")
                              "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
                              (rdf/resource "http://www.w3.org/2000/01/rdf-schema#Class")]])})

  
  (let [tdb @(get-in control-plane-app [:storage :test-jena :instance])]
    (rdf/tx tdb
      (rdf/to-turtle (storage/read tdb "http://example.com/test"))))

  (storage/read @(get-in control-plane-app [:storage :test-rocks :instance])
                "http://example.com/test")
  
  (with-open [is (-> {:type :file
                      :base test-base-path
                      :path "no-snapshot"}
                     storage/as-handle
                     io/input-stream)]
    (slurp is))

  (-> {:type :file
       :base test-base-path
       :path "no-snapshot"}
      storage/as-handle
      storage/exists?)

  (-> {:type :gcs
       :bucket "genegraph-framework-dev"
       :path "no-snapshot"}
      storage/as-handle
      storage/exists?)

  (with-open [is (-> {:type :gcs
                      :bucket "genegraph-framework-dev"
                      :path "no-snapshot"}
                     storage/as-handle
                     io/input-stream)]
    (slurp is))
  )
