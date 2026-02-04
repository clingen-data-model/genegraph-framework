(ns genegraph.framework.http-server
  (:require [genegraph.framework.protocol :as p]
            [io.pedestal.connector :as conn]
            [io.pedestal.http.http-kit :as hk]
            [io.pedestal.service.interceptors :as service-interceptors]))

(defrecord Server [type
                   name
                   state
                   connector]
  p/Lifecycle
  (start [this]
    (swap! state assoc :status :running)
    (conn/start! connector))
  (stop [this]
    (swap! state assoc :status :stopped)
    (conn/stop! connector)))

(defn endpoint->route [{:keys [path processor method] :or {method :get}}]
  (let [http-method (or method :get)]
    [path http-method (p/as-interceptors processor) :route-name (:name processor)]))

(def default-connector-map
  {:port 8888,
   :host "0.0.0.0",
   :router :sawtooth,
   :interceptors [],
   :initial-context {},
   :join? false})

(defn create-connector [server-def]
  (-> default-connector-map
      conn/with-default-interceptors
      (merge (select-keys server-def
                          [:port :host :interceptors]))
      (conn/with-routes (->> (:endpoints server-def)
                             (map endpoint->route)
                             (concat (:routes server-def))
                             set))
      (hk/create-connector nil)))

(defmethod p/init :http-server [server-def]
  (-> server-def
      (assoc :state (atom {:status :stopped}))
      (assoc :connector (create-connector server-def))
      map->Server))














