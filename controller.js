import KafkaConfig from "./config.js";
import { kafkaTopic } from "./constants.js";

const keys = ["key1", "key2", "key3"];

const sendMessageToKafka = async (req, res) => {
  try {
    const { message } = req.body;
    const kafkaConfig = new KafkaConfig();

    const randomNumber = Math.floor(Math.random() * keys.length);
    const randomKey = keys[randomNumber];
    const messages = [{ key: randomKey, value: message + ":" + randomKey }];
    kafkaConfig.produce(kafkaTopic, messages);

    res.status(200).json({
      status: "Ok!",
      message: "Message successfully send!",
    });
  } catch (error) {
    console.log(error);
  }
};

const controllers = { sendMessageToKafka };

export default controllers;
