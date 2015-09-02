# most-data-pool
Most Web Framework Generic Pool Adapter
##Install
$ npm install most-data-pool
##Usage
Register Generic Pool Adapter on app.json as follows:

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

#Options
###size: 
The number of the data adapters that are going to be pooled for new connections.
###timeout: 
A number of milliseconds to wait for getting a new data adapter. If this timeout exceeds, an timeout error will occured.
###lifetime
A number of milliseconds which indicates whether a pooled data adapter will be automatically ejected from data adapters' collection.
