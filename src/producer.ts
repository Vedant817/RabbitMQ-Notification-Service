import express from "express";
import amqp from "amqplib";

const app = express();
app.use(express.json());

let channel: amqp.Channel | null = null;

async function connectRabbitMQ() {
    try {
        const connection = await amqp.connect('amqp://localhost:5672');
        channel = await connection.createChannel();
        await channel.assertExchange('notification_exchanges', 'direct', { durable: true });
        await channel.assertQueue('notification_queue', { durable: true });
        await channel.bindQueue('notification_queue', 'notification_exchanges', 'email');
        console.log('Connected to RabbitMQ at amqp://localhost:5672');
    } catch (error) {
        console.error('RabbitMQ Connection Error: ', error);
    }
}

connectRabbitMQ();

app.post('/notify', async (req, res) => {
    const { to, content } = req.body;
    if(!channel) return res.status(500).send('RabbitMQ Connection Error');

    const message = Buffer.from(JSON.stringify({ //! Converting the notification data to JSON string and then a Buffer.
        type: 'email',
        to,
        content
    }));
    channel.publish('notification_exchanges', 'email', message, { persistent: true });

    res.send('Notification sent successfully')
});

app.listen(3000, () => {
    console.log('Producer API running at PORT 3000');
})