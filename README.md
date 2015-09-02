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

The generic pool adapter will try to instantiate the adapter defined in options.adapter property.
