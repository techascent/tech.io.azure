(ns tech.io.azure.auth
  "Vault auth provider for azure."
  (:require [tech.io.auth :as io-auth]
            [tech.config.core :as config]))


(defn vault-azure-blob-creds
  [vault-path options]
  (let [{:keys [azure-blob-account-key
                azure-blob-account-name]}
        (io-auth/read-credentials vault-path)]
    {:tech.azure.blob/account-key azure-blob-account-key
     :tech.azure.blob/account-name azure-blob-account-name}))


(defn azure-blob-auth-provider
  [& [vault-path options]]
  (let [vault-path (or vault-path
                       (config/get-config :tech-azure-blob-vault-path))]
    (io-auth/auth-provider #(vault-azure-blob-creds vault-path options)
                           options)))


(comment
  (def provider (azure-blob-auth-provider))
  (def creds ((:request-credentials-fn provider)))
  )
