import json

import pika

from config import settings


def _connection():
    credentials = pika.PlainCredentials(settings.rabbitmq_user, settings.rabbitmq_password)
    return pika.BlockingConnection(
        pika.ConnectionParameters(host=settings.rabbitmq_host, credentials=credentials)
    )


def publish_reminder(card_id: str, title: str, due_date: str | None) -> None:
    connection = _connection()
    channel = connection.channel()
    channel.queue_declare(queue=settings.reminder_queue, durable=True)

    payload = json.dumps({"card_id": card_id, "title": title, "due_date": due_date})
    channel.basic_publish(
        exchange="",
        routing_key=settings.reminder_queue,
        body=payload,
        properties=pika.BasicProperties(delivery_mode=2),
    )
    connection.close()
