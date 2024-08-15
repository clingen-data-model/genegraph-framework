(ns genegraph.framework.http-server
  (:require [genegraph.framework.protocol :as p]
            [io.pedestal.http :as http]
            [io.pedestal.http.route :as route]))


(defrecord Server [type
                   name
                   producer
                   kafka-cluster
                   state]
  p/Lifecycle
  (start [this]
    (swap! state assoc :status :running)
    (http/start this))
  (stop [this]
    (swap! state assoc :status :stopped)
    (http/stop this)))

(defn endpoint->route [{:keys [path processor method] :or {method :get}}]
  (let [http-method (or method :get)]
    [path http-method (p/as-interceptors processor) :route-name (:name processor)]))

(defmethod p/init :http-server [server-def]
  (let [init-fn (:init-fn server-def identity)]
    (-> server-def
        init-fn
        (update ::http/routes
                #(->> (:endpoints server-def)
                      (map endpoint->route)
                      (concat %)
                      set
                      route/expand-routes))
        (assoc :state (atom {:status :stopped})
               :producer (promise))
        http/create-server
        map->Server)))

(comment

  (defn test-response-fn [event]
    (println "test-response-fn")
    {:status 200
     :body "Hello, flower."})

  (def p
    (p/init
     {:type :processor
      :name :test-processor
      :interceptors `[test-response-fn]}))
  (p/start p)
  (p/process p {})
  (p/stop p)

  (def s
    (p/init
     {:type :http-server
      :name :test-server
      :endpoints [{:path "/hello"
                   :processor p}]
      ::http/type :jetty
      ::http/port 8888
      ::http/join? false}))

  (p/start s)
  (p/stop s)
  )








