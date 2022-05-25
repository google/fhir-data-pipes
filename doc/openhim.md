# Integrating with OpenHIM (Middleware component)

For the case, where there are several OpenMRS systems, OpenHIM can be used as a
middleware component to integrate/track/log requests from several OpenMRS
instances.

## Fire up OpenHIM Instance

After spinning up the Hapi FHIR instance, see details above, you can spin up an
OpenHIM instance.

```
`$ docker-compose -f openhim-compose.yaml up # change ports appropriately (optional)`
```

You should be able to access OpenHIM via http://localhost:9000 Use the default
credentials i.e

```
`username : root@openhim.org and password : openhim-password`
```

Note: You will have problems logging in if your OpenHIM server is still setup to
use a self-signed certificate (the default). Visit the following link:
https://localhost:8093/authenticate/root@openhim.org in your browser.

You should see a message saying “Your connection is not private”. Click
“Advanced” and then click “Proceed”.

Once you have done this, you should see some JSON text displayed on the screen,
you can ignore this and close the page. This will ignore the fact that the
certificate is self-signed.

Now, you should be able to go back to the OpenHIM console login page and login.
This problem will occur every now and then until you load a properly signed
certificate into the OpenHIM core server.

For the first Login you will be requested to change your root password!

Then Go to the 'Export/Import' tab and import the default config file under
`Utils/openhim-config.json` for a basic configuration.

NB the default configurations have the following default configs:

For the Client :

```
`client : hapi` , `client-password : Admin123`
```

For the server to route to :

```
`Host: 172.17.0.1` (Docker Bridge Ip address), `Port: 8098` (Port for the Hapi fhir server) .
```

see section *Using Docker compose* to fire up the Hapi fhir server and openmrs.
\
Note that Openhim listens to client requests at `port:5001` by default.

You can now start the pipeline with these args below to point to the OpenHIM
instance

```
`--sinkUserName=hapi --sinkPassword=Admin123 --fhirSinkPath=http://localhost:5001/fhir`
```

e.g.,

for batch

```
mvn compile exec:java -pl batch \
    "-Dexec.args=--fhirServerUrl=http://localhost:8099/openmrs/ws/fhir2/R4  --resourceList=Patient \
    --batchSize=20 --targetParallelism=20  \
    --fhirSinkPath=http://localhost:5001/fhir \
    --sinkUserName=hapi --sinkPassword=Admin123 \
    --fhirServerUserName=admin --fhirServerPassword=Admin123 "
```

for streaming

```
mvn compile exec:java -pl streaming-binlog \
  -Dexec.args="--databaseHostName=localhost \
  --databasePort=3306 --databaseUser=root --databasePassword=debezium\
  --databaseServerName=mysql --databaseSchema=openmrs --databaseServerId=77 \
  --fhirServerUserName=admin --fhirServerPassword=Admin123 \
  --fhirServerUrl=http://localhost:8099/openmrs/ws/fhir2/R4 \
  --fhirSinkPath=http://localhost:5001/fhir \
  --sinkUserName=hapi --sinkPassword=Admin123 \
  --outputParquetPath=/tmp/"
```

You can track all the transactions in the OpenHIM instance Under the Tab
`Transaction Log`

See a demo at [https://youtu.be/U1Sz3GUKbIw]().

See
[here](https://openhim.readthedocs.io/en/latest/how-to/how-to-setup-and-configure-openhim.html)
for more details about setting up OpenHIM.
