// producer.js

// Node.js packages used to communicate with Azure Service Bus
const { ServiceBusClient } = require("@azure/service-bus");

// Load the .env file if it exists
require("dotenv").config();

// Define connection string and related Service Bus entity names here
const connectionString =
  process.env.SERVICEBUS_CONNECTION_STRING || "<connection string>";
const queueName = process.env.QUEUE_NAME || "<queue name>";

// Node.js package used to read data from .csv files
const csv = require("csvtojson");

// Node.js package used to work with dates and times
const moment = require("moment");

// send the reminder "x" minutes before the appointment time
const leadTimeInMinutes = 15;

async function main() {
  const sbClient = new ServiceBusClient(connectionString);

  // createSender() can also be used to create a sender for a topic.
  const sender = sbClient.createSender(queueName);

  try {
    // Tries to send all messages in a single batch.
    // Will fail if the messages cannot fit in a batch.
    // import appointments data from .csv file
    const appointmentsData = await csv().fromFile("./appointments.csv");

    for (const row of appointmentsData) {
      const mqData = {
        applicationProperties: {
          to: row["Phone"]
        },
        body: `Hello ${row['Name']}, you have an appointment with us in ${leadTimeInMinutes}
minutes. See you soon.`
      };

      if (toDelayFromDate(row["AppointmentDateTime"]) - Date.now() > 0) {
        console.log(new Date(toDelayFromDate(row["AppointmentDateTime"])));
        console.log("publish...");

        const scheduledEnqueueTimeUtc = new Date(toDelayFromDate(row["AppointmentDateTime"]));

        await sender.scheduleMessages(mqData, scheduledEnqueueTimeUtc);
      }
    }

    console.log(`Sent a batch of messages to the queue: ${queueName}`);

    // Close the sender
    await sender.close();
  } finally {
    await sbClient.close();
  }
}

// utility function, returns milliseconds
// calculates the difference between the appointment time and the current time
function toDelayFromDate(dateTime) {
  return moment(dateTime).subtract(leadTimeInMinutes, 'minutes').valueOf();
}

main().catch((err) => {
  console.log("Error occurred: ", err);
  process.exit(1);
});

