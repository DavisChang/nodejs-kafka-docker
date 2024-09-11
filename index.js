import express from "express";
import bodyParser from "body-parser";
import controllers from "./controller.js";
import KafkaConfig from "./config.js";
import { kafkaTopic } from "./constants.js";
const app = express();
const jsonParser = bodyParser.json();

app.post("/api/send", jsonParser, controllers.sendMessageToKafka);

const kafkaConfig = new KafkaConfig();
kafkaConfig.consume(kafkaTopic, (value) => {
  console.log("📨 Receive message: ", value);
});

app.listen(8080, () => {
  console.log(`Server is running on port 8080.`);
});
