const amqp = require('amqplib');

// RabbitMQ connection string
const connectionString = 'amqp://myuser:mypassword@localhost:5672/';


async function setupRabbitMQ() {
    try {
        const connection = await amqp.connect(connectionString);
        const channel = await connection.createChannel();
        return channel;
    } catch (error) {
        console.error('**** Error connecting to RabbitMQ:', error);
    }
}

async function declareFanoutExchange(channel, exchangeName) {
    try {
        await channel.assertExchange(exchangeName, 'fanout', { durable: true });
        console.log('Fan out exchange declared....', exchangeName);
    } catch (error) {
        console.error('***** Error declaring fanout exchange:', error);
    }
}

async function declareAndBindQueue(channel, queueName, exchangeName) {
    try {
        await channel.assertQueue(queueName, {
            durable: true, arguments: {
                'x-dead-letter-exchange': deadLetterExchangeName,
            }
        },);
        await channel.bindQueue(queueName, exchangeName, '');
        console.log('Queue declared and bound to exchange:', queueName);
    } catch (error) {
        console.error('Error declaring and binding queue:', error);
    }
}

async function produceMessage(channel, exchangeName, message) {
    try {
        await channel.publish(exchangeName, '', Buffer.from(JSON.stringify(message)));
        console.log('Message produced to exchange:', message);
    } catch (error) {
        console.error('Error producing message:', error);
    }
}

// write the consumer function that waill consume the message from the exchange of exchangeName

async function consumeMessage(channel, exchangeName) {
    try {
        // Declare the original queue with a dead-letter exchange
        await channel.assertQueue(queue1, {
            deadLetterExchange: 'dead_letter_exchange',
        });
        await channel.consume(queue1, (message) => {
            console.log(`Message consumed from queue ${queue1}:`, message.content.toString());
            // channel.ack(message) // use this code for successfully consume the message
            channel.nack(message, false, false); // use this code for not successfully consume the message and send that message to Dead letter channel
            // Add your logic to process the consumed message here
        }, { noAck: false });

        // await channel.consume(queue2, (message) => {
        //     console.log(`Message consumed from queue ${queue2}:`, message.content.toString());
        //     channel.ack(message)
        //     // Add your logic to process the consumed message here
        // }, { noAck: false });
        // console.log('Consumer started for exchange:', exchangeName);
    } catch (error) {
        console.error('Error consuming message:', error);
    }
}

async function setUpDeadLetterQueue(channel) {
    // Declare the dead-letter exchange
    await channel.assertExchange(deadLetterExchangeName, 'direct', { durable: true });

    // Declare the dead-letter queue
    await channel.assertQueue('dead_letter_queue', { durable: true });

    // Bind the dead-letter queue to the dead-letter exchange
    await channel.bindQueue('dead_letter_queue', deadLetterExchangeName, '');

}

// Usage
const queue1 = 'Queue1';
const queue2 = 'Queue2';
const exchangeName = 'MyFanoutExchange';
const message = { content: 'Hello, world 2!' };
const deadLetterExchangeName = "dead_letter_exchange";

setupRabbitMQ().then(async channel => {
    await setUpDeadLetterQueue(channel);
    await declareFanoutExchange(channel, exchangeName);
    await declareAndBindQueue(channel, queue1, exchangeName);
    await declareAndBindQueue(channel, queue2, exchangeName);
    await consumeMessage(channel, exchangeName)
    await produceMessage(channel, exchangeName, message);
});



