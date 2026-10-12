import json
import typing

from aiokafka import AIOKafkaProducer

from src.application.ports.message_broker import MessageBroker


class KafkaMessageBroker(MessageBroker):
    def __init__(self, producer: AIOKafkaProducer, topic: str) -> None:
        self._producer = producer
        self._topic = topic

    async def send(
        self, payload: dict[str, typing.Any], headers: dict[str, bytes] | None = None
    ) -> None:
        kafka_headers = list(headers.items()) if headers else None
        await self._producer.send_and_wait(
            self._topic, value=payload, headers=kafka_headers
        )


def serialize(value: dict[str, typing.Any]) -> bytes:
    return json.dumps(value, ensure_ascii=False).encode("utf-8")
