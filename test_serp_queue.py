"""Test script to send N test queries to serp_req_queue"""

import asyncio
import sys
import os
import uuid
import random

# Add project root to Python path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

from app.helper.rabbitmq_helper import RabbitMQHelper

# Sample test queries
TEST_QUERIES = [
    "best python frameworks 2024",
    "machine learning tutorials",
    "react vs vue comparison",
    "docker kubernetes tutorial",
    "aws lambda best practices",
    "postgresql optimization tips",
    "fastapi vs django",
    "redis caching strategies",
    "microservices architecture patterns",
    "github actions ci cd",
    "typescript advanced features",
    "nodejs performance optimization",
    "graphql vs rest api",
    "mongodb indexing strategies",
    "linux command line tips",
]

# Available search types
SEARCH_TYPES = [
    "google-web",
    "google-news",
    "google-image",
    "bing-web",
    "bing-news",
    "bing-image",
]

QUEUE_NAME = "serp_req_queue"


async def send_test_messages(num_messages: int):
    """Send test messages to serp_req_queue"""

    print(f"\n{'='*50}")
    print(f"Sending {num_messages} test messages to {QUEUE_NAME}")
    print(f"{'='*50}\n")

    rabbitmq = RabbitMQHelper()

    try:
        # Ensure queue exists
        await rabbitmq.ensure_queue_exists(
            QUEUE_NAME,
            durable=True,
            arguments={"x-max-priority": 10}
        )

        sent_count = 0

        for i in range(num_messages):
            # Generate test message
            query = random.choice(TEST_QUERIES)
            query_id = str(uuid.uuid4())
            search_type = random.choice(SEARCH_TYPES)

            message = {
                "query": query,
                "query_id": query_id,
                "search_type": search_type
            }

            # Publish message
            success = await rabbitmq.publish_message(
                queue_name=QUEUE_NAME,
                message=message,
                priority=random.randint(1, 5),
                ensure_queue=False
            )

            if success:
                sent_count += 1
                print(f"[{i+1}/{num_messages}] Sent: {search_type} - {query[:40]}...")
                print(f"         query_id: {query_id}")
            else:
                print(f"[{i+1}/{num_messages}] FAILED to send message")

        print(f"\n{'='*50}")
        print(f"Successfully sent {sent_count}/{num_messages} messages")
        print(f"{'='*50}\n")

    finally:
        await rabbitmq.close()


async def main():
    # Get number of messages from command line arg or default to 5
    num_messages = 5

    if len(sys.argv) > 1:
        try:
            num_messages = int(sys.argv[1])
        except ValueError:
            print(f"Invalid number: {sys.argv[1]}, using default: 5")

    await send_test_messages(num_messages)


if __name__ == "__main__":
    print("\nSERP Queue Test Script")
    print("Usage: python test_serp_queue.py [num_messages]")
    print("Example: python test_serp_queue.py 10\n")

    asyncio.run(main())
