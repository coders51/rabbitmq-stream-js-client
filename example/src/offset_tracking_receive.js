const rabbit = require("rabbitmq-stream-js-client");

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function eventually(fn, timeout = 1500) {
  const start = Date.now();
  while (true) {
    try {
      await fn();
      return;
    } catch (error) {
      if (elapsedFrom(start) > timeout) {
        if (error instanceof AssertionError) throw error;
        expect.fail(error);
      }
      await sleep(5);
    }
  }
}

async function main() {
  const streamName = "stream-offset-tracking-javascript";

  console.log("Connecting...");
  const client = await rabbit.connect({
    hostname: "localhost",
    port: 5552,
    username: "guest",
    password: "guest",
    vhost: "/",
  });

  // start consuming at the beginning of the stream
  const consumerRef = "offset-tracking-tutorial"; // the consumer must a have name
  let firstOffset = undefined;
  let offsetSpecification = rabbit.Offset.first();
  try {
    const offset = await client.queryOffset({ reference: consumerRef, stream: streamName });
    firstOffset = offset + 1n;
    offsetSpecification = rabbit.Offset.offset(firstOffset); // take the offset stored on the server if it exists
  } catch (e) {}

  let lastOffset = offsetSpecification.value;
  let messageCount = 0;
  const consumer = await client.declareConsumer(
    { stream: streamName, offset: offsetSpecification, consumerRef },
    async (message) => {
      const contextOffset = await consumer.queryOffset();
      console.log("MessageContent:", message.content.toString(), contextOffset);
      messageCount++;
      if (!firstOffset && messageCount === 1) {
        firstOffset = message.offset;
        console.log("First message received", firstOffset);
      }
      if (messageCount % 10 === 0) {
        console.log("Storing offset");
        await consumer.storeOffset(message.offset); // store offset every 10 messages
      }
      if (message.content.toString() === "marker" && messageCount > 1) {
        lastOffset = message.offset;
        console.log("Marker found", lastOffset);
        await consumer.storeOffset(message.offset); // store the offset on consumer closing
        await consumer.storeOffset(message.offset); // store the offset on consumer closing
        await consumer.storeOffset(message.offset); // store the offset on consumer closing
        await consumer.storeOffset(message.offset); // store the offset on consumer closing
        await consumer.storeOffset(message.offset); // store the offset on consumer closing
        console.log(`Done consuming, first offset was ${firstOffset}, last offset was ${lastOffset}`);
        await consumer.close(true);
      }
    }
  );

  console.log(`Start consuming...`);
  await sleep(2000);
}

main()
  .then(async () => {
    await new Promise(function () {});
  })
  .catch((res) => {
    console.log("Error while receiving message!", res);
    process.exit(-1);
  });
