(ns tech.v3.io.azure.blob
  (:require [tech.io.azure.blob :as v1]
            [tech.v3.io.protocols :as io-prot]))

(def ^:dynamic *default-v3-azure-provider*
  (v1/create-default-azure-provider))

(def ensure-container! v1/ensure-container!)

(def blob-provider v1/blob-provider)

(defmethod io-prot/url-parts->provider :azb
  [& args]
  *default-v3-azure-provider*)





