(ns http-init-example
  (:require [genegraph.framework.app :as app]
            [genegraph.framework.protocol :as p]
            [genegraph.framework.event :as event]
            [io.pedestal.interceptor :as interceptor]
            #_[io.pedestal.http :as http]
            [io.pedestal.log :as log]))




(def print-event-interceptor
  (interceptor/interceptor
   {:name :print-system-event
    :enter (fn [e]
             (log/info :system-event :recieved)
             e)}))

(def hello-interceptor
  (interceptor/interceptor
   {:name :hello-interceptor
    :enter (fn [e] (assoc e :response {:status 200 :body "hello!"}))}))

(def hello-processor
  {:name :hello-processor
   :type :processor
   :interceptors [hello-interceptor]})

(def ready-server
  {:ready-server
   {:type :http-server
    :name :ready-server
    :init-fn (fn [svr]
               (log/info :ready-server :init)
               svr)
    :endpoints [{:path "/hello"
                 :processor :hello-processor
                 :method :get}]
    :routes
    [["/ready"
      :get (fn [_] {:status 200 :body "server is ready"})
      :route-name ::readiness]
     ["/live"
      :get (fn [_] {:status 200 :body "server is live"})
      :route-name ::liveness]]
    :port 8888}})

(def publish-system-event-interceptor
  (interceptor/interceptor
   {:name :publish-system-event
    :enter (fn [e]
             (event/publish
              e
              {::event/topic :system
               ::event/key :d
               ::event/data {:g :g}}))}))

(def ready-app-def
  {:type :genegraph-app
   :http-servers ready-server
   :topics
   {:events-topic {:type :simple-queue-topic
                   :name :events-topic}}
   :processors
   {:hello-processor hello-processor
    :event-processor
    {:type :processor
     :name :event-processor
     :subscribe :events-topic
     :interceptors [print-event-interceptor
                    publish-system-event-interceptor]}
    :system-processor
    (update app/default-system-processor
            :interceptors
            conj
            print-event-interceptor)
    :ready-api
    {:type :processor
     :name :ready-api
     }}})


(comment
  (def ready-app (p/init ready-app-def))
  (p/start ready-app)
  (p/publish (get-in ready-app [:topics :events-topic])
             {::event/key :a ::event/data {:b :b}})
  (p/stop ready-app)
  
  )
