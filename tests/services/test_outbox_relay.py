import logging
from typing import Any

import pytest

from src.application.ports.message_broker import MessageBroker
from src.infrastructure.logging.config import configure_logging
from src.infrastructure.messaging.outbox_relay import OutboxRelay
from tests.conftest import FakeMessageBroker, FakeUnitOfWork


class RecordCapture(logging.Handler):
    def __init__(self) -> None:
        super().__init__()
        self.records: list[logging.LogRecord] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.records.append(record)


class RaisingBroker(MessageBroker):
    async def send(
        self, payload: dict[str, Any], headers: dict[str, bytes] | None = None
    ) -> None:
        raise RuntimeError("kafka unavailable")


@pytest.mark.asyncio
async def test_relay_sends_and_marks_published(
    fake_uow: FakeUnitOfWork, fake_broker: FakeMessageBroker
) -> None:
    await fake_uow.outbox.add("ad.created", {"ad_id": 1})
    await fake_uow.outbox.add("ad.updated", {"ad_id": 1})

    relay = OutboxRelay(uow_factory=lambda: fake_uow, broker=fake_broker)
    processed = await relay._process_batch()

    assert processed == 2
    assert fake_broker.sent == [
        {"event": "ad.created", "payload": {"ad_id": 1}},
        {"event": "ad.updated", "payload": {"ad_id": 1}},
    ]
    assert fake_uow.committed
    assert fake_uow.outbox.messages == []


@pytest.mark.asyncio
async def test_relay_noop_when_outbox_empty(
    fake_uow: FakeUnitOfWork, fake_broker: FakeMessageBroker
) -> None:
    relay = OutboxRelay(uow_factory=lambda: fake_uow, broker=fake_broker)
    processed = await relay._process_batch()

    assert processed == 0
    assert fake_broker.sent == []
    assert not fake_uow.committed


@pytest.mark.asyncio
async def test_relay_rolls_back_and_leaves_messages_on_broker_failure(
    fake_uow: FakeUnitOfWork,
) -> None:
    await fake_uow.outbox.add("ad.created", {"ad_id": 1})

    relay = OutboxRelay(uow_factory=lambda: fake_uow, broker=RaisingBroker())

    with pytest.raises(RuntimeError):
        await relay._process_batch()

    assert not fake_uow.committed
    assert fake_uow.rolled_back
    assert len(fake_uow.outbox.messages) == 1


@pytest.mark.asyncio
async def test_relay_logs_publication_with_trace_id(
    fake_uow: FakeUnitOfWork, fake_broker: FakeMessageBroker
) -> None:
    configure_logging()

    trace_id = "test-trace-relay-123"
    await fake_uow.outbox.add("ad.created", {"ad_id": 42})

    fake_uow.outbox.messages[0].trace_id = trace_id

    logger = logging.getLogger("src.infrastructure.messaging.outbox_relay")
    logger.setLevel(logging.INFO)
    handler = RecordCapture()
    handler.setLevel(logging.INFO)
    logger.addHandler(handler)

    try:
        relay = OutboxRelay(uow_factory=lambda: fake_uow, broker=fake_broker)
        processed = await relay._process_batch()

        assert processed == 1
        publish_logs = [
            r for r in handler.records if "published outbox message" in r.getMessage()
        ]
        assert len(publish_logs) == 1
        record = publish_logs[0]
        assert hasattr(record, "trace_id")
        assert record.trace_id == trace_id
        assert "id=1" in record.getMessage()
        assert "event=ad.created" in record.getMessage()
    finally:
        logger.removeHandler(handler)
