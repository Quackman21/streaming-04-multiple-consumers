import pika
import csv
import json
import sys

rabbit_host = 'localhost'
queue_name = 'games_task_queue'
csv_file_path = '/Users/ivanquackenbush/Documents/44-608/Streaming-Data/streaming-04-multiple-consumers/games.csv'  # Update this to your file path

def send_message(host, queue, message):
    try:
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=host))
        channel = connection.channel()
        channel.queue_declare(queue=queue, durable=True)

        channel.basic_publish(
            exchange='',
            routing_key=queue,
            body=message,
            properties=pika.BasicProperties(
                delivery_mode=2,  # Make the message persistent
            ),
        )
        print(f" [x] Sent {message}")
        connection.close()
    except Exception as e:
        print(f"Failed to send message: {e}")

def main():
    try:
        with open(csv_file_path, 'r') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                game_data = {
                    'Game': row.get('Game'),
                    'away': row.get('away'),
                    'away-score': row.get('away-score'),
                    'home': row.get('home'),
                    'home-score': row.get('home-score'),
                    'Stadium': row.get('Stadium'),
                    'Attendance': row.get('Attendance'),
                }
                message = json.dumps(game_data)
                send_message(rabbit_host, queue_name, message)
    except FileNotFoundError:
        print(f"CSV file not found at {csv_file_path}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

if __name__ == "__main__":
    print("Emitter is ready. Press CTRL+C to exit.")
    try:
        main()
    except KeyboardInterrupt:
        print("User interrupted the process. Exiting.")
