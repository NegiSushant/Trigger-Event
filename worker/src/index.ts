import { Kafka } from "kafkajs";

const TOPIC_NAME = "zap-events";
// const client = new PrismaClient();
const kafka = new Kafka({
  clientId: "outbox-processor",
  brokers: ["localhost:9092"],
});

async function main() {
  const consumer = kafka.consumer({ groupId: "main-worker" });
  await consumer.connect();

  await consumer.subscribe({ topic: TOPIC_NAME, fromBeginning: true });

  await consumer.run({
    autoCommit: false,
    eachMessage: async ({ topic, partition, message }) => {
      console.log({
        partition,
        offset: message.offset,
        value: message.value?.toString(),
      });
      //stop the excution for 1 second
      await new Promise((r) => setTimeout(r, 5000));

      console.log("processing done!");
      await consumer.commitOffsets([
        {
          topic: TOPIC_NAME,
          partition: 0,
          offset: (parseInt(message.offset) + 1).toString(),
        },
      ]);
    },
  });
}

main();
