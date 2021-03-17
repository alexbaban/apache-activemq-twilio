// consumer.js

// Node.js packages used to communicate with ActiveMQ
// utilising WebSocket and STOMP protocols
const {
  delay,
  isServiceBusError,
  ServiceBusClient,
} = require("@azure/service-bus");

// Load the .env file if it exists
require("dotenv").config();
// Define connection string and related Service Bus entity names here
const connectionString =
  process.env.SERVICEBUS_CONNECTION_STRING || "<connection string>";
const queueName = process.env.QUEUE_NAME || "<queue name>";

// creates a Twilio client
const accountSid = process.env.TWILIO_ACCOUNT_SID;
const authToken = process.env.TWILIO_AUTH_TOKEN;
const twilio = require("twilio")(accountSid, authToken);

// sends the SMS
async function sendSMSWithTwilio(to, body) {
  try {
    const message = await twilio.messages.create({
      to,
      messagingServiceSid: process.env.TWILIO_MESSAGING_SERVICE_SID,
      body,
    });

    console.log(`SMS: ${message.sid}`);
  } catch (err) {
    console.error(err);
  }
}

async function main() {
  const sbClient = new ServiceBusClient(connectionString);

  // - If receiving from a subscription you can use the createReceiver(topicName, subscriptionName) overload
  // instead.
  // - See session.ts for how to receive using sessions.
  const receiver = sbClient.createReceiver(queueName);

  const myMessageHandler = async ({ applicationProperties: { to }, body }) => {
    console.log(`Received message: ${body}`);

    await sendSMSWithTwilio(to, body);
  };

  const myErrorHandler = async (args) => {
    console.log(`Error from source ${args.errorSource} occurred: `, args.error);

    // the `subscribe() call will not stop trying to receive messages without explicit intervention from you.
    if (isServiceBusError(args.error)) {
      switch (args.error.code) {
        case "MessagingEntityDisabled":
        case "MessagingEntityNotFound":
        case "UnauthorizedAccess":
          // It's possible you have a temporary infrastructure change (for instance, the entity being
          // temporarily disabled). The handler will continue to retry if `close()` is not called on the subscription - it is completely up to you
          // what is considered fatal for your program.
          console.log(
            `An unrecoverable error occurred. Stopping processing. ${args.error.code}`,
            args.error
          );
          await subscription.close();
          break;
        case "MessageLockLost":
          console.log(`Message lock lost for message`, args.error);
          break;
        case "ServiceBusy":
          // choosing an arbitrary amount of time to wait.
          await delay(1000);
          break;
      }
    }
  };

  const subscription = receiver.subscribe({
    // After executing this callback you provide, the receiver will remove the message from the queue if you
    // have not already settled the message in your callback.
    // You can disable this by passing `false` to the `autoCompleteMessages` option in the `subscribe()` method.
    // If your callback _does_ throw an error before the message is settled, then it will be abandoned.
    processMessage: myMessageHandler,
    // This callback will be called for any error that occurs when either in the receiver when receiving the message
    // or when executing your `processMessage` callback or when the receiver automatically completes or abandons the message.
    processError: myErrorHandler,
  });

  console.log(`Receiving messages...`);
}

main().catch((err) => {
  console.log("Error occurred: ", err);
  process.exit(1);
});
