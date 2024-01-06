(ns genegraph.framework.env
  "Generate environment for application (including secrets) using GCP secretmanager.
  May also be used for updating secrets and cloud environment in an administrative capacity."
  (:import [com.google.cloud.secretmanager.v1
            ProjectName SecretManagerServiceClient SecretVersionName
            AccessSecretVersionResponse SecretPayload Replication
            Replication$Builder Replication$Automatic Secret Secret$Builder
            SecretPayload SecretPayload$Builder]
           [com.google.protobuf ByteString]))

(defn build-environment [project secrets]
  (with-open [secrets-client (SecretManagerServiceClient/create)]
    (reduce (fn [m s]
              (assoc m
                     (keyword s)
                     (-> (.accessSecretVersion secrets-client
                                               (SecretVersionName/of project s "latest"))
                         .getPayload
                         .getData
                         .toStringUtf8)))
            {} secrets)))

(comment
  (build-environment "522856288592" ["dev-genegraph-dev-dx-jaas"])
  (Secret/getDefaultInstance)



  (ProjectName/of "522856288592")
  
  (with-open [secrets-client (SecretManagerServiceClient/create)]
    (.createSecret secrets-client
                   (ProjectName/of "522856288592")
                   "test-genegraph-secret"
                   (-> (Secret/newBuilder)
                       (.setReplication
                        (-> (Replication/newBuilder)
                            (.setAutomatic (.build (Replication$Automatic/newBuilder)))
                            .build))
                       .build)))

  (with-open [secrets-client (SecretManagerServiceClient/create)]
    (.addSecretVersion secrets-client
                       (ProjectName/of "522856288592")
                       "test-genegraph-secret"
                       (-> (Secret/newBuilder)
                           (.setReplication
                            (-> (Replication/newBuilder)
                                (.setAutomatic (.build (Replication$Automatic/newBuilder)))
                                .build))
                           .build)))
  
  )

