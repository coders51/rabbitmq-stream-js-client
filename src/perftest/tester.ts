import { randomUUID } from "crypto"
import { connect } from "../connection"
import { PerfTestProducer } from "./perf_test_producer"
import { argv } from "process"

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
  console.log(`Stream ${streamName} for publisher ${publisherRef}`)
  const messageLength = argv.length > 2 ? +argv[2] : 10
  const maxBufferLength = argv.length > 3 ? +argv[3] : 100
  const chunkSize = argv.length > 4 ? +argv[4] : 100
  const sendDuration = argv.length > 5 ? +argv[5] : 10

  const perfTestProducer = new PerfTestProducer(
    connection,
    { stream: streamName, publisherRef: publisherRef, maxBufferLength: maxBufferLength, chunkSize, sendDuration },
    messageLength
  )
  console.log(`${new Date().toISOString()} - cycle start`)
  await perfTestProducer.cycle()
}

main()
  .then((v) => {
    console.log(`main then ${v}`)
  })
  .catch((res) => console.log("ERROR ", res))
