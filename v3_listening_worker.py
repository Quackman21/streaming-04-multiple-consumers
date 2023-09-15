import pika
import sys
import time
import json

rabbit_host = 'localhost'
queue_name = 'games_task_queue'

def callback(ch, method, properties, body):
    decoded_body = body.decode()
    game_data = json.loads(decoded_body)
    
    print(f"Away Team: {game_data['away']}, Home Team: {game_data['home']}")
    print(f"Away Score: {game_data['away-score']}, Home Score: {game_data['home-score']}")
    
    print(" [x] Done.")
    ch.basic_ack(delivery_tag=method.delivery_tag)

def main(host, queue):
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=host))
    channel = connection.channel()
    channel.queue_declare(queue=queue, durable=True)
    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue=queue, on_message_callback=callback)

    print(" [*] Waiting for messages. To exit press CTRL+C")
    channel.start_consuming()

if __name__ == "__main__":
    print("Listener is ready. Press CTRL+C to exit.")
    try:
        main(rabbit_host, queue_name)
    except KeyboardInterrupt:
        print("User interrupted the process. Exiting.")
