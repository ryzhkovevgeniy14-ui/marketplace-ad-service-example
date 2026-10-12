import asyncio
import logging
from typing import Callable

from src.application.ports.message_broker import MessageBroker
from src.application.ports.uow import UnitOfWork
from src.infrastructure.tracing.context import reset_trace_id, set_trace_id

logger = logging.getLogger(__name__)


class OutboxRelay:
    def __init__(
        self,
        uow_factory: Callable[[], UnitOfWork],
        broker: MessageBroker,
        batch_size: int = 100,
        idle_sleep: float = 1.0,
    ) -> None:
        self._uow_factory = uow_factory
        self._broker = broker
        self._batch_size = batch_size
        self._idle_sleep = idle_sleep

    async def run(self) -> None:
        while True:
            try:
                published = await self._process_batch()
            except Exception:
                logger.exception("outbox relay batch failed")
                await asyncio.sleep(self._idle_sleep)
                continue
            if published == 0:
                await asyncio.sleep(self._idle_sleep)

    async def _process_batch(self) -> int:
        async with self._uow_factory() as uow:
            messages = await uow.outbox.fetch_unpublished(self._batch_size)
            if not messages:
                return 0

            for message in messages:
                token = None
                if message.trace_id:
                    token = set_trace_id(message.trace_id)

                try:
                    headers = {}
                    if message.trace_id:
                        headers["trace-id"] = message.trace_id.encode("utf-8")
                    await self._broker.send(
                        {
                            "event": message.event_type,
                            "payload": message.payload,
                        },
                        headers=headers,
                    )
                    logger.info(
                        "published outbox message id=%d event=%s",
                        message.id,
                        message.event_type,
                    )
                finally:
                    if token:
                        reset_trace_id(token)

            await uow.outbox.mark_published([m.id for m in messages])
            await uow.commit()
            logger.info("relayed %d outbox messages", len(messages))
            return len(messages)
