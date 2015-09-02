# most-data-pool
Most Web Framework Generic Pool Adapter
##Install
$ npm install most-data-pool
##Usage
Register MSSQL adapter on app.json as follows:

    "adapterTypes": [
        ...
        { "name":"...", "invariantName": "...", "type":"..." },
        { "name":"Pool Data Adapter", "invariantName": "pool", "type":"most-data-pool" }
        ...
    ],
    adapters: [
        ...
        { "name":"development", "invariantName":"...", "default":false,
            "options": {
              "server":"localhost",
              "user":"user",
              "password":"password",
              "database":"test"
            }
        },
        { "name":"development_with_pool", "invariantName":"pool", "default":true,
                    "options": {
                      "adapter":"development"
                    }
                }
        ...
    ]

If you are intended to use Pool data adapter as the default database adapter set the property "default" to true.

The generic pool adapter will try to instantiate the adapter defined in options.adapter property.
