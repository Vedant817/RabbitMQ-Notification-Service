import amqp from 'amqplib';

async function startConsumer() {
    try {
        const connection = await amqp.connect('amqp://localhost:5672');
        const channel = await connection.createChannel();
        await channel.assertExchange('notification_exchanges', 'direct', { durable: true });
        await channel.assertQueue('notification_queue', { durable: true });
        await channel.bindQueue('notification_queue', 'notification_exchanges', 'email');

        channel.consume('notification_queue', (message) => {
            if (message) {
                const content = JSON.parse(message.content.toString()); //! Converting the Buffer to JSON string and then parsing it.
                console.log(`Processing notification: To ${content.to}, Content: ${content.content}`);

                setTimeout(() => {
                    console.log('Notification Processed Successfully');
                    channel.ack(message);
                }, 1000);
            }
        }, { noAck: false })
    } catch (error) {
        console.error('Error in consumer:', error);
    }
}

startConsumer();