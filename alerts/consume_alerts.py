from kafka import KafkaConsumer
import json
import sys
import time

def main():
    print("Starting Kafka Alert Consumer...")
    print("Connecting to Kafka broker at kafka:9092...")
    print("Listening to topic: syslog_alerts\n")

    # Create consumer
    consumer = KafkaConsumer(
        "syslog_alerts",
        bootstrap_servers="kafka:9092",
        auto_offset_reset="latest",    # read new messages only
        enable_auto_commit=True,
        group_id="alert-consumer-group",
        value_deserializer=lambda v: v.decode("utf-8")
    )

    print("Connected! Waiting for alerts...\n")

    # Consume messages forever
    try:
        for msg in consumer:
            try:
                data = json.loads(msg.value)
                print("ALERT DETECTED!")
                print(f"Host:          {data.get('host')}")
                print(f"Error Count:   {data.get('error_count')}")
                print(f"Window Start:  {data.get('window_start')}")
                print(f"Window End:    {data.get('window_end')}")
                print("-" * 60)
            except json.JSONDecodeError:
                print("Received non-JSON message:", msg.value)
    except KeyboardInterrupt:
        print("Stopping consumer...")
        sys.exit(0)


if __name__ == "__main__":
    main()
