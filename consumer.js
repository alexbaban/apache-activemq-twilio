// consumer.js

// Node.js packages used to communicate with ActiveMQ 
// utilising WebSocket and STOMP protocols
const StompJs = require('@stomp/stompjs');
Object.assign(global, { WebSocket: require('websocket').w3cwebsocket });

// Node.js package used to read environment variables
require('dotenv').config();

// creates a Twilio client
const twilio = require('twilio')(
    process.env.TWILIO_ACCOUNT_SID,
    process.env.TWILIO_AUTH_TOKEN
);

// create a STOMP client for ActiveMQ
const stompClient = new StompJs.Client({
    brokerURL: "ws://localhost:61614/ws"
});

// connect with the broker
stompClient.activate();
console.log("STOMP client activated...");

// on connect subscribe to queue and consume messages
stompClient.onConnect = (frame) => {
    console.log("STOMP client connected...");

    // the queue you're interested in is identified by "foo.bar"
    const queue = "/queue/foo.bar";
    const headers = { ack: 'auto' };

    stompClient.subscribe(
        queue,
        onMessageCallback,
        headers
    );

}

// invoked for each received message
const onMessageCallback = (jsonMessage) => {
    // expecting JSON
    try {
        const jsonObj = JSON.parse(jsonMessage.body);
        sendSMSWithTwilio(jsonObj.to, jsonObj.body);
    } catch (err) {
        console.log("Payload is not a JSON...");
    }
}

// sends the SMS
function sendSMSWithTwilio(to, body) {
    twilio.messages
        .create({
            to: to,
            from: process.env.TWILIO_NUMBER,
            body: body
        })
        .then((message) => {
            console.log(`SMS: ${message.sid}`);
        })
        .catch((err) => {
            console.error(err);
        });
}
