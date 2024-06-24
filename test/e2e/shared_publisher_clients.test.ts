import { expect } from "chai"
import { randomUUID } from "crypto"
import { Client, Offset } from "../../src"
import { Publisher } from "../../src/publisher"
import { createClient, createPublisher, createStreamName } from "../support/fake_data"
import { Rabbit } from "../support/rabbit"
import { eventually, username, password } from "../support/util"

describe("publish messages through multiple publishers", () => {
  const rabbit = new Rabbit(username, password)
  let client: Client
  let streamName: string
  let publisher: Publisher

  beforeEach(async () => {
    client = await createClient(username, password)
    streamName = createStreamName()
    await rabbit.createStream(streamName)
    publisher = await createPublisher(streamName, client)
  })

  afterEach(async () => {
    try {
      await client.close()
      await rabbit.deleteStream(streamName)
      await rabbit.closeAllConnections()
      await rabbit.deleteAllQueues({ match: /my-stream-/ })
    } catch (e) {}
  })

  it("multiple publishers working on the same stream (no message is lost)", async () => {
    const publishers = [publisher, await client.declarePublisher({ stream: streamName })]
    const localPort = publisher.getConnectionInfo().localPort
    const howMany = 50

    for (let index = 0; index < howMany; index++) {
      for (const p of publishers) {
        await p.send(Buffer.from(`test${randomUUID()}`))
      }
    }

    expect(localPort).not.undefined
    expect(publishers).satisfies((plist: Publisher[]) =>
      plist.every((p) => {
        const connInfo = p.getConnectionInfo()
        return connInfo.localPort === localPort && connInfo.writable === true
      })
    )
    await eventually(
      async () => expect((await rabbit.getQueueInfo(streamName)).messages).eql(howMany * publishers.length),
      10000
    )
  }).timeout(15000)

  it("multiple publishers working on the same stream (the order is enforced when using flush)", async () => {
    const publishers = [publisher, await client.declarePublisher({ stream: streamName })]
    const howMany = 10
    const received: string[] = []
    for (let index = 0; index < howMany; index++) {
      for (const k of publishers.keys()) {
        const p = publishers[k]
        await p.send(Buffer.from(`${k};${index}`))
        await p.flush()
      }
    }

    await client.declareConsumer({ stream: streamName, offset: Offset.first() }, (msg) => {
      received.push(msg.content.toString("utf-8"))
    })

    await eventually(async () => expect(received.length).eql(howMany * publishers.length), 10000)
    expect(received).satisfies((msgs: string[]) => {
      let check = true
      for (const receivedMsgKey of msgs.keys()) {
        const publisherKey = receivedMsgKey % publishers.length
        const msgKey = Math.floor(receivedMsgKey / publishers.length)
        const msg = msgs[receivedMsgKey]
        check = check && msg === `${publisherKey};${msgKey}`
      }
      return check
    })
  }).timeout(15000)
})
