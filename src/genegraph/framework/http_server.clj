(ns genegraph.framework.http-server
  (:require [genegraph.framework.protocol :as p]
            [io.pedestal.http :as http]
            [io.pedestal.http.route :as route]))


(defrecord Server [type
                   name
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
    [path http-method (p/interceptors processor) :route-name (:name processor)]))

(defmethod p/init :http-server [server-def]
  (-> server-def
      (update ::http/routes
              #(->> (:endpoints server-def)
                    (map endpoint->route)
                    (concat %)
                    set
                    route/expand-routes))
      (assoc :state (atom {:status :stopped}))
      http/create-server
      map->Server))

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
  (p/interceptors p)
  (p/stop p)

  (route/expand-routes
   #{(endpoint->route {:path "/huh"
                       :processor p})})
  
  (def s
    (p/init
     {:type :http-server
      :name :test-server
      :endpoints [["/hello" p]]
      ::http/type :jetty
      ::http/port 8888
      ::http/join? false}))
  s
  (p/start s)
  (p/stop s)

  (route/expand-routes
   #{["/hello"
      :get (fn [e] (p/process p e))
      :route-name ::hello]})

  
  (route/expand-routes
   {::http/route [{:path "/hello"
                   :method :get
                   :interceptors []
                   :route-name :genegraph.framework.http-server/hello}]})
  
  )








