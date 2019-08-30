const request = require('request');
const n3 = require('n3');
const Stream = require('stream');


try {


    // 1. Get latest fragment to get most recent observation
    //getMostRecentObservation();


    //let start = new Date("2019-05-14");               // DAY


    // 2. Get Time Range for historic data
    //let start = new Date("2019-05-28T19:00:00.000Z"); // HOUR
    // getTimeRangeData("hour", start, 1);

    // 2. Get Time Range for now data
    //let start = new Date("2019-05");                  // MONTH
    // getTimeRangeData("month", start, 1);

    // 3. Get statistical average for every sensor - historic data
   // let start = new Date("2019-05-20T08:00:00.000Z"); // HOUR
    //getStatisticalAverage("hour", start, 1);

    //4. Get statistical average for every sensor - now data
    let start = new Date("2019");                  // MONTH
    //getStatisticalAverage("month", start, 1);

    // 5. Get geographical values
    //getGeographicalData();


    /////////////////////////////
    ////////// TEST /////////////
    /////////////////////////////
    for(let i = 0 ; i < 100 ; i++){
        getStatisticalAverage("year", start, 1);
    }

    setTimeout( () => {}, 5000);


} catch (err) {
    console.error(err);
}


/////////////////////////////////////////////////////////
////////////////** RawData interface **//////////////////
/////////////////////////////////////////////////////////
// Works only when sensor data is being streamed to server
async function getLatestStreamUpdate(clients) {
    /*for (let client = 0; client < clients; client++) {
        let data = await executeRequest('http://localhost:8080/RawData/latest');
        console.log(data);
    }*/
}

async function getMostRecentObservation() {
    let parser = new n3.Parser();

    // Get data
    //let data = await executeRequest('http://172.18.0.3:80/RawData/fragments');
    let data = await executeRequest('http://server-cache/RawData/fragments');
    // Parse triples
    let result = await new Promise(resolve => {
        let result = [];

        parser.parse(data, (err, triple, prefixes) => {
            if (triple) {
                if (triple.predicate.value === 'http://www.w3.org/ns/sosa/hasFeatureOfInterest') {
                    if (!result[triple.subject.value]) {
                        result[triple.subject.value] = {};
                    }

                    result[triple.subject.value].property = triple.object.value;
                }

                if (triple.predicate.value === 'http://www.w3.org/ns/sosa/madeBySensor') {
                    if (!result[triple.subject.value]) {
                        result[triple.subject.value] = {};
                    }
                    result[triple.subject.value].sensor = triple.object.value;
                }

                if (triple.predicate.value === 'http://www.w3.org/ns/sosa/hasSimpleResult') {
                    if (!result[triple.subject.value]) {
                        result[triple.subject.value] = {};
                    }
                    result[triple.subject.value].value = triple.object.value;
                }

                if (triple.predicate.value === 'http://www.w3.org/ns/sosa/resultTime') {
                    if (!result[triple.subject.value]) {
                        result[triple.subject.value] = {};
                    }
                    result[triple.subject.value].time = triple.object.value;
                }
            } else {
                resolve(result);
            }
        });
    });

    let tmp = [];
    Object.keys(result).forEach(t => {
        tmp.push([result[t].time, result[t]]);
    });

    tmp.sort((a, b) => {
        return new Date(b[0]) - new Date(a[0]);
    });
    let object = tmp[0][1];
    // console.log("======================================");
    // console.log("Sensor: " + object.sensor);
    // console.log("Measured property: " + object.property);
    // console.log("Value: " + object.value);
    // console.log("Time: " + object.time);
    // console.log("======================================");

}


/////////////////////////////////////////////////////////
/////////////** TimeRangeData interface **///////////////
/////////////////////////////////////////////////////////
async function getTimeRangeData(type, start, number) {
    let parser = new n3.Parser();
    let stream = new Stream.Readable({objectMode: true});
    stream._read = () => {};

    stream.on('data', data => {
        // DOE IETS MET DATA
    });

    for (let i = 0; i < number; i++) {
        let query = createQueryString(start, type, i);
        let data = await executeRequest('http://server-cache/TimeRangeData/fragment' + query);
        parseData(data, stream);
    }


}


/////////////////////////////////////////////////////////
///////////** StatisticalAverage interface **////////////
/////////////////////////////////////////////////////////
// With the 'number' parameter you define how many values you want
// For example if type is month and number is 2, you want the values of 2 months starting at the date you enter in the 'start' parameter
async function getStatisticalAverage(type, start, number) {

    for (let i = 0; i < number; i++) {
        let query = createQueryString(start, type, i);
        let data = await executeRequest('http://server-cache/StatisticalAverage/fragment' + query);

        let result = await new Promise(resolve => {
            let parser = new n3.Parser();
            let result = [];

            parser.parse(data, (err, triple, prefixes) => {
                if (triple) {
                    if (triple.predicate.value === 'http://datapiloten.be/vocab/timeseries#mean') {
                        result.push({sensor: triple.subject.value, value: triple.object.value});
                    }
                } else {
                    resolve(result);
                }
            });
        });

        for (let j = 0; j < result.length; j++) {
            let tmp = result[j];
            let sensor = tmp.sensor.substring(0, tmp.sensor.lastIndexOf('/'));
            let property = tmp.sensor.substring(tmp.sensor.lastIndexOf('/') + 1, tmp.sensor.length);

            // console.log("SENSOR: " + sensor);
            // console.log('\tProperty: ' + property);
            // console.log('\tValue: ' + tmp.value)
        }
    }

}


/////////////////////////////////////////////////////////
/////////////** Geographical interface **////////////////
/////////////////////////////////////////////////////////
async function getGeographicalData() {
    let tile1 = '/14_8391_5468';
    let tile2 = '/14_8392_5468';
    let tile3 = '/14_8392_5470';
    let tile4 = '/14_8391_5470';

    let parser = new n3.Parser();
    let stream = new Stream.Readable({objectMode: true});
    stream._read = () => {};

    stream.on('data', data => {
        // DOE IETS MET DATA
        //console.log(data);
    });

    let data = await executeRequest('http://server-cache/Geographical/fragment' + tile2);
    parseData(data, stream);

}

/////////////////////////////////////////////////////
//////////////// HELPER FUNCTIONS ///////////////////
/////////////////////////////////////////////////////

// Help function to execute GET-request to server //
async function executeRequest(url) {
    return new Promise(resolve => {
        request(url, (err, res, body) => {
            if (err) console.log(err);
            resolve(body);
        });
    })
}

function createQueryString(date, type, i) {
    let query;

    if (type === 'year') {
        query = '/' + (date.getFullYear() + i) + '_' + (date.getFullYear() + i + 1);
    } else {
        let zeroString1 = date.getMonth() + 1 + i < 10 ? '0' : '';
        let zeroString2 = date.getMonth() + 2 + i < 10 ? '0' : '';

        if (type === 'month') {
            query = '/' + date.getFullYear() + '_' + (date.getFullYear() + 1)
                + '/' + zeroString1 + (date.getMonth() + 1 + i) + '_' + zeroString2 + (date.getMonth() + i + 2);
        } else if (type === 'day') {
            query = '/' + date.getFullYear() + '_' + (date.getFullYear() + 1)
                + '/' + zeroString1 + (date.getMonth() + 1) + '_' + zeroString2 + (date.getMonth() + 2) + '/'
                + (date.getDate() + i) + '_' + (date.getDate() + i + 1);
        } else {
            let zeroStringHour1 = date.getUTCHours() + i < 10 ? '0' : '';
            let zeroStringHour2 = date.getUTCHours() + 1 + i < 10 ? '0' : '';
            query = '/' + date.getFullYear() + '_' + (date.getFullYear() + 1)
                + '/' + zeroString1 + (date.getMonth() + 1) + '_' + zeroString2 + (date.getMonth() + 2) + '/'
                + date.getDate() + '_' + (date.getDate() + 1) + '/' + zeroStringHour1 + (date.getUTCHours() + i) + '_' + zeroStringHour2 + (date.getUTCHours() + i + 1);
        }

    }
    return query;
}

async function parseData(data, stream) {
    let parser = new n3.Parser();
    let result = [];

    parser.parse(data, async (err, triple, prefixes) => {
        if (triple) {
            if (triple.predicate.value === 'http://www.w3.org/ns/sosa/hasFeatureOfInterest') {
                if (!result[triple.subject.value]) {
                    result[triple.subject.value] = {};
                }

                result[triple.subject.value].property = triple.object.value;
            }

            if (triple.predicate.value === 'http://www.w3.org/ns/sosa/madeBySensor') {
                if (!result[triple.subject.value]) {
                    result[triple.subject.value] = {};
                }
                result[triple.subject.value].sensor = triple.object.value;
            }

            if (triple.predicate.value === 'http://www.w3.org/ns/sosa/hasSimpleResult') {
                if (!result[triple.subject.value]) {
                    result[triple.subject.value] = {};
                }
                result[triple.subject.value].value = triple.object.value;
            }

            if (triple.predicate.value === 'http://www.w3.org/ns/sosa/resultTime') {
                if (!result[triple.subject.value]) {
                    result[triple.subject.value] = {};
                }
                result[triple.subject.value].time = triple.object.value;
            }

            if (triple.predicate.value === 'http://www.w3.org/ns/hydra/core#previous') {
                let previous = triple.object.value;
                previous = previous.replace('localhost:8080', 'server-cache');
                let data = await executeRequest(previous);
                parseData(data, stream);

            }

            // Only for Geographical data
            if(triple.predicate.value === 'http://datapiloten.be/vocab/timeseries#mean'){
                result["Gemiddelde"] = triple.object.value;
            }

        } else {
            stream.push(result);
        }
    });
}