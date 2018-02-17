const debug = require('debug');

function consoleLoggerProvider (name) {
    // do something with the name
    return {
        debug: console.log.bind(console),
        info: console.log.bind(console),
        warn: console.log.bind(console),
        error: console.log.bind(console)
    };
}

// Active the log of kafka-node to understand the client behavior.
const kafkaLogging = require('kafka-node/logging');
kafkaLogging.setLoggerProvider(consoleLoggerProvider);

const kafka = require('kafka-node'),
    Producer = kafka.Producer,
    KeyedMessage = kafka.KeyedMessage,
    Client = kafka.KafkaClient; // New KafkaClient connects directly to Kafka brokers
                                // instead of connecting to zookeeper for broker discovery.

const topic = 'topic1';
const partition = 0;
const attributes = 0;

const client = new Client({kafkaHost: '127.0.0.1:9092'});
const producer = new Producer(client);


if (process.pid) {
    console.log('This process is pid ' + process.pid);
}

producer.on('ready', () => {
    console.log("Producer is ready");
    const simpleMessage = 'hi';
    const keyedMessage = new KeyedMessage('a', 'hi');

    producer.send([
        { topic, messages: [ simpleMessage, keyedMessage ] },
    ],
    (err, result) => {
        console.log(err || result);
        process.exit();
    });
});

producer.on('error', (err) => {
    console.log('error', err)
});
