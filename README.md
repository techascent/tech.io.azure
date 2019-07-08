# tech.io.azure

Azure bindings for tech.io

## Usage

You can turn vault auth on or off in the main io config:

* [io configuration](https://github.com/techascent/tech.io/blob/master/resources/io-config.edn)

You can choose the vault key here:

* [configuration](resources/azure-io-config.edn)

If you don't want to use vault or if you want to export environment variables to
you environment (in a docker scenario, for instance), the system will look for:
* `AZURE_BLOB_ACCOUNT_NAME` - name of the account.
* `AZURE_BLOB_ACCOUNT_KEY` - key of the account.


The shape of the vault entry must be:
```console
chrisn@chrisn-lt-2:~/dev/tech.all/tech.io.azure$ vault read "techascent/fathym-azure"
Key                        Value
---                        -----
refresh_interval           768h
azure-blob-account-key     your-key-base-64-encoded==
azure-blob-account-name    your-account-name
```

All io functions work.  See [test](test/tech/io/azure/blob_test.clj).





## License

Copyright © 2019 TechAscent, LLC
