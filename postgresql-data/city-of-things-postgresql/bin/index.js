const kafka = require('kafka-node');
const Consumer = kafka.Consumer;
const {Client} = require('pg');
const geo = require('latlon-geohash');

try {
    // POSTGRES
    const pgClient = new Client({
        user: '',
        host: '',			// Change IP according to docker container of postgis
        database: '',
        password: ''
    });
    pgClient.connect();

    // KAFKA
    //let kafkaClient = new kafka.KafkaClient();
    let kafkaClient = new kafka.KafkaClient({kafkaHost: 'XXX.XX.XX.XX:9092'});      // Change XX with IP
    let consumer = new Consumer(kafkaClient, [{topic: 'airquality'}], {fetchMaxBytes: 1024});
    consumer.on('message', async (message) => {
        let data = JSON.parse(message.value);
        let latlong = await geo.decode(data.geohash);
        let date = new Date(data.timestamp);

        // All measures
        if(data.metricId.indexOf('airquality') >= 0 && data.metricId.indexOf('voc') < 0){
            let query = 'INSERT INTO "AllSensorData"("SensorID", "AirQualityID", "Value", "Timestamp") VALUES($1,$2,$3,$4) ';
            let date = new Date(data.timestamp);
            let values = [data.sourceId, data.metricId, data.value, date];
            pgClient.query(query, values, (err, res) => {
                if(err){
                    console.log(err);
                }
            });
        }

        // Only PM10 measures
        if (data.metricId === 'airquality.pm10') {

            let location = "POINT(" + latlong.lon + " " + latlong.lat + ")";

            let query = 'INSERT INTO "SensorData"("sensorID","location","value","timestamp") VALUES($1,$2,$3,$4) ';
            let values = [data.sourceId, location, data.value, date];

            pgClient.query(query, values, (err, res) => {
                if (err) {
                    console.log(err.stack)
                }
            })
        }
    });

    consumer.on('error', (err) => {
        console.log(err);
    })

} catch (e) {
    console.log(e);
}
