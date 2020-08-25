// producer.js

// Node.js packages used to communicate with ActiveMQ 
// utilising WebSocket and STOMP protocols
const StompJs = require('@stomp/stompjs');
Object.assign(global, { WebSocket: require('websocket').w3cwebsocket });

// Node.js package used to read data from .csv files
const csv = require('csvtojson');

// Node.js package used to work with dates and times
const moment = require('moment');

let appointmentsData = {};

// send the reminder "x" minutes before the appointment time
const leadTimeInMinutes = 15;

// create a STOMP client for ActiveMQ
const stompClient = new StompJs.Client({
    brokerURL: "ws://localhost:61614/ws"
});

// import appointments data from .csv file
csv()
    .fromFile("./appointments.csv")
    .then((jsonObj) => {
        appointmentsData = jsonObj;
        stompClient.activate();
        console.log("STOMP client activated...");
    });

// once connected add messages to queue then disconnect
stompClient.onConnect = (frame) => {
    console.log("STOMP client connected...");
    publishToQueue(appointmentsData);
};

function publishToQueue(data) {

    // stop condition, all application messages added to queue
    if (data.length === 0) {
        stompClient.deactivate();
        console.log("STOMP client deactivated.");
        return;
    }

    let row = data.shift();
    let mqData = {
        to: row['Phone'],
        body: (`Hello ${row['Name']}, you have an appointment with us in ${leadTimeInMinutes} minutes. See you soon.`)
    };

   // publish the current application message to the "foo.bar" queue
    // uses AMQ_SCHEDULED_DELAY, the time in milliseconds that a message will wait
    // must be a positive value
    if (toDelayFromDate(row['AppointmentDateTime']) > 0) {
        console.log(toDelayFromDate(row['AppointmentDateTime']));
        console.log("publish...");
        stompClient.publish({
            destination: '/queue/foo.bar',
            body: JSON.stringify(mqData),
            headers: {
                'content-type': 'application/json',
                AMQ_SCHEDULED_DELAY: toDelayFromDate(row['AppointmentDateTime'])
            }
        });
    }

    // recursive call until all messages are added to queue
    publishToQueue(data);

}

// utility function, returns milliseconds
// calculates the difference between the appointment time and the current time
function toDelayFromDate(dateTime) {
    let appointmentDateTime = new moment(dateTime);
    let now = new moment();
    const delay = (moment.duration(appointmentDateTime.diff(now)).as('milliseconds'));
    const leadTimeInMilliseconds = (leadTimeInMinutes * 60 * 1000);
    return (delay - leadTimeInMilliseconds);
}
