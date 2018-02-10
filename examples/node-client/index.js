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

const topic = process.argv[1] || 'my-topic';
const partition = process.argv[2] || 0;
const attributes = process.argv[3] || 0;

const client = new Client({kafkaHost: '127.0.0.1:8080'});
const producer = new Producer(client);


if (process.pid) {
    console.log('This process is pid ' + process.pid);
}

producer.on('ready', () => {
    console.log("Producer is ready");

    producer.send([
        { topic, messages: 'hi' },
    ],
    (err, result) => {
        console.log(err || result);
        process.exit();
    });
});

producer.on('error', (err) => {
    console.log('error', err)
});
