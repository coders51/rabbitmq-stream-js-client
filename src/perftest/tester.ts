import { randomUUID } from "crypto"
import { connect } from "../connection"
import { PerfTestProducer } from "./perf_test_producer"

async function main() {
  const connection = await connect({
    hostname: "localhost",
    port: 5552,
    username: "rabbit",
    password: "rabbit",
    vhost: "/",
  })

  const streamName = `my-stream-${randomUUID()}`
  await connection.createStream({ stream: streamName, arguments: {} })
  const publisherRef = `my-publisher-${randomUUID()}`
  const perfTestProducer = new PerfTestProducer(connection, { stream: streamName, publisherRef: publisherRef })

  await perfTestProducer.cycle()

  await connection.close()
}

main()
  .then(() => console.log("done!"))
  .catch((res) => console.log("ERROR ", res))
