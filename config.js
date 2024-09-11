import { Kafka, Partitioners } from "kafkajs";
import { kafkaGroupId } from "./constants.js";

class KafkaConfig {
  constructor() {
    this.kafka = new Kafka({
      clientId: "nodejs-kafka",
      brokers: ["localhost:9093"],
      // Kafka (3 Brokers)
      // brokers: ["localhost:9092", "localhost:9093", "localhost:9094"],
      connectionTimeout: 3000, // Timeout for establishing a connection
      retry: {
        initialRetryTime: 300, // Initial backoff delay
        retries: 10, // Number of retry attempts
      },
    });
    this.producer = this.kafka.producer({
      createPartitioner: Partitioners.LegacyPartitioner,
    });
    this.consumer = this.kafka.consumer({ groupId: kafkaGroupId });
  }

  async produce(topic, messages) {
    try {
      await this.producer.connect();
      await this.producer.send({
        topic: topic,
        messages: messages,
      });
      console.log(
        `[Produce] Log messages:  ${JSON.stringify(messages)} sent to Kafka`
      );
    } catch (error) {
      console.error(error);
    } finally {
      await this.producer.disconnect();
    }
  }

  async consume(topic, callback) {
    try {
      await this.consumer.connect();
      await this.consumer.subscribe({ topic: topic, fromBeginning: true });
      await this.consumer.run({
        eachMessage: async ({ topic, partition, message }) => {
          const value = message.value.toString();
          const key = message.key.toString();
          console.log(
            `[Consume] Processing log ${key}: message: ${value}, partition: ${partition}, topic: ${topic}`
          );

          callback(value);
        },
      });
    } catch (error) {
      console.error(error);
    }
  }
}

export default KafkaConfig;
