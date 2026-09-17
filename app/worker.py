import json
import time

import pika

from config import settings
from database import Base, SessionLocal, engine
from models import Reminder


def _connect():
    credentials = pika.PlainCredentials(settings.rabbitmq_user, settings.rabbitmq_password)
    for intento in range(10):
        try:
            return pika.BlockingConnection(
                pika.ConnectionParameters(host=settings.rabbitmq_host, credentials=credentials)
            )
        except pika.exceptions.AMQPConnectionError:
            time.sleep(3)
    raise RuntimeError("No se pudo conectar a RabbitMQ")


def procesar_mensaje(ch, method, properties, body):
    data = json.loads(body)
    mensaje = f"Recordatorio: la tarjeta '{data['title']}' vence el {data.get('due_date') or 'sin fecha'}"
    print(mensaje, flush=True)

    db = SessionLocal()
    try:
        db.add(Reminder(card_id=data["card_id"], message=mensaje))
        db.commit()
    finally:
        db.close()

    ch.basic_ack(delivery_tag=method.delivery_tag)


def main():
    Base.metadata.create_all(bind=engine)
    connection = _connect()
    channel = connection.channel()
    channel.queue_declare(queue=settings.reminder_queue, durable=True)
    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue=settings.reminder_queue, on_message_callback=procesar_mensaje)

    print("Worker de recordatorios escuchando...", flush=True)
    channel.start_consuming()


if __name__ == "__main__":
    main()
