(ns genegraph.common.processors.remote-request
  "Processor for handling requests to remote resources,
  such as a RESTful HTTP endpoint. Currently functionality
  only exists for HTTP, but the requirements and semantics should
  be similar regardless of the protocol and this is intended to be
  extended to other protocols should the need arise.

  Keys attached to the event as metadata, in this namespace, will
  be used for interpreting the nature and paramaters of the request.
  They include:

  ::client (attached by default) java.net.http.HTTPClient for request, used by Hato.
  ::method HTTP method for request, :get, :post

  By default includes functionality for a circuit breaker:
  processing will slow or be completely interrupted if there are errors
  in the stream processing.

  Best practice is for a processor to be dedicated to a single endpoint."
  
  (:require [hato.client :as hc]
            [genegraph.framework.protocol :as p]
            [genegraph.framework.event :as event]
            [genegraph.framework.storage :as storage])
  (:import [java.time Instant]))

(defn exec-http [event]
  (try 
    (let [f (case (::http-method event)
              :get hc/get
              :post hc/post)
          uri (str (::endpoint event) (get-in event [::event/data :resource]))]
      (assoc event ::result (f uri (::request-opts event))))
    (catch Exception e (assoc event :error {:fn ::exec-http
                                            :exception e}))))

(defn http-ok? [event]
  (= 200 (get-in event [::result :status])))

(defn examine-result-for-error [event]
  (if (http-ok? event)
    event
    (assoc event :error {:fn ::examine-result-for-error
                         :code ::unuseable-http-result})))


(defn storage-key [event]
  (str (::event/key event)
       (.toEpochMilli (Instant/now))))

;; currently depends on hato result,
;; may need to adjust if using other clients
(defn store-result [event]
  (if (::store event)
    (event/store event
                 (::store event)
                 (storage-key event)
                 (get-in event [::result :body]))
    event))

(defn publish-notification [event]
  (event/publish
   event
   (get-in event [::event/data :notification-topic])
   {::event/key (storage-key event)
    ::event/data (assoc (:notification-event event)
                        :location
                        (storage-key event))
    ::event/format :edn}))

(comment

  (def test-app
    (genegraph.framework.app/create
     {:topics
      {:request {}
       :result {}}
      :storage
      {:gcs
       {:type :gcs-bucket
        :bucket "genegraph-framework-dev"}}
      :processors
      {:remote-request
       {:interceptors `[exec-http
                        examine-result-for-error
                        store-result
                        publish-notification]
        :subscribe :request
        ::event/metadata {::http-method :get
                          ::store :gcs
                          ::request-opts
                          {:as :stream
                           :http-client
                           (hc/build-http-client
                            {:redirect-policy :always})}}}}}))

  (p/start test-app)
  (p/stop test-app)

  
  
  (-> test-app
      :storage
      deref
      :gcs
      :instance
      deref
      (storage/write "test-file-2.txt" (clojure.java.io/input-stream
                                        "/users/tristan/desktop/test.txt")))

  
  (genegraph.framework.processor/process-event
   (-> test-app :processors deref :remote-request)
   {::event/key "k"
    ::event/value (pr-str
                   {:resource "/1999/02/22-rdf-syntax-ns.ttl"
                    :notification-topic :result})
    ::event/metadata {::event/format :edn
                      ::endpoint "http://www.w3.org"}})


  (genegraph.framework.processor/process-event
   (-> test-app :processors deref :remote-request)
   {::event/key "k"
    ::event/value (pr-str
                   {:resource "/obo/mondo.owl"})
    ::event/metadata {::event/format :edn
                      ::endpoint "http://purl.obolibrary.org"}})

  (genegraph.framework.processor/process-event
   (-> test-app :processors deref :remote-request)
   {::event/key "k"
    ::event/value (pr-str
                   {:resource "/obo/hp.owl"})
    ::event/metadata {::event/format :edn
                      ::endpoint "http://purl.obolibrary.org"}})


  r

  (count (get-in r [::result :body]))

  (count (get-in mondo [::result :body]))
  
  )


