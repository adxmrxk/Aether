"""
WebSocket Real-Time Streaming Server

Provides real-time market sentiment updates via WebSocket:
- Live sentiment score changes
- Price update notifications
- Alert triggers for significant movements
- Multi-client broadcasting

Integrates with Pub/Sub for event sourcing.
"""

import asyncio
import json
import logging
from dataclasses import dataclass, asdict
from datetime import datetime
from enum import Enum
from typing import Any, Optional, Set

from fastapi import WebSocket, WebSocketDisconnect
from google.cloud import pubsub_v1

logger = logging.getLogger(__name__)


class EventType(Enum):
    """Types of real-time events."""
    SENTIMENT_UPDATE = "sentiment_update"
    PRICE_ALERT = "price_alert"
    TREND_CHANGE = "trend_change"
    SYSTEM_STATUS = "system_status"
    HEARTBEAT = "heartbeat"


@dataclass
class StreamEvent:
    """Real-time event structure."""
    event_type: EventType
    symbol: Optional[str]
    data: dict[str, Any]
    timestamp: str

    def to_json(self) -> str:
        """Serialize event to JSON."""
        return json.dumps({
            "type": self.event_type.value,
            "symbol": self.symbol,
            "data": self.data,
            "timestamp": self.timestamp,
        })

    @classmethod
    def sentiment_update(cls, symbol: str, score: float, category: str, reasoning: str) -> "StreamEvent":
        """Create a sentiment update event."""
        return cls(
            event_type=EventType.SENTIMENT_UPDATE,
            symbol=symbol,
            data={
                "sentiment_score": score,
                "sentiment_category": category,
                "reasoning": reasoning,
            },
            timestamp=datetime.utcnow().isoformat(),
        )

    @classmethod
    def price_alert(cls, symbol: str, price: float, change_percent: float, direction: str) -> "StreamEvent":
        """Create a price alert event."""
        return cls(
            event_type=EventType.PRICE_ALERT,
            symbol=symbol,
            data={
                "price_usd": price,
                "change_percent": change_percent,
                "direction": direction,  # "up" or "down"
            },
            timestamp=datetime.utcnow().isoformat(),
        )

    @classmethod
    def heartbeat(cls) -> "StreamEvent":
        """Create a heartbeat event."""
        return cls(
            event_type=EventType.HEARTBEAT,
            symbol=None,
            data={"status": "connected"},
            timestamp=datetime.utcnow().isoformat(),
        )


class ConnectionManager:
    """
    Manages WebSocket connections and broadcasting.

    Features:
    - Multiple subscription channels (per symbol, all)
    - Automatic reconnection handling
    - Connection health monitoring
    """

    def __init__(self):
        # All active connections
        self.active_connections: Set[WebSocket] = set()

        # Symbol-specific subscriptions
        self.symbol_subscriptions: dict[str, Set[WebSocket]] = {}

        # Connection metadata
        self.connection_metadata: dict[WebSocket, dict] = {}

    async def connect(
        self,
        websocket: WebSocket,
        client_id: Optional[str] = None,
        symbols: Optional[list[str]] = None,
    ) -> None:
        """
        Accept a new WebSocket connection.

        Args:
            websocket: WebSocket connection
            client_id: Optional client identifier
            symbols: Optional list of symbols to subscribe to
        """
        await websocket.accept()
        self.active_connections.add(websocket)

        # Store metadata
        self.connection_metadata[websocket] = {
            "client_id": client_id,
            "connected_at": datetime.utcnow().isoformat(),
            "symbols": symbols or [],
        }

        # Subscribe to specific symbols
        if symbols:
            for symbol in symbols:
                symbol = symbol.upper()
                if symbol not in self.symbol_subscriptions:
                    self.symbol_subscriptions[symbol] = set()
                self.symbol_subscriptions[symbol].add(websocket)

        logger.info(f"Client connected: {client_id}, symbols: {symbols}")

        # Send welcome message
        welcome = StreamEvent(
            event_type=EventType.SYSTEM_STATUS,
            symbol=None,
            data={
                "status": "connected",
                "subscriptions": symbols or ["all"],
                "message": "Welcome to AetherFlow real-time stream",
            },
            timestamp=datetime.utcnow().isoformat(),
        )
        await websocket.send_text(welcome.to_json())

    def disconnect(self, websocket: WebSocket) -> None:
        """Remove a WebSocket connection."""
        self.active_connections.discard(websocket)

        # Remove from symbol subscriptions
        for subscribers in self.symbol_subscriptions.values():
            subscribers.discard(websocket)

        # Clean up metadata
        metadata = self.connection_metadata.pop(websocket, {})
        logger.info(f"Client disconnected: {metadata.get('client_id')}")

    async def broadcast(self, event: StreamEvent) -> None:
        """
        Broadcast event to all connected clients.

        Args:
            event: Event to broadcast
        """
        message = event.to_json()
        disconnected = set()

        for connection in self.active_connections:
            try:
                await connection.send_text(message)
            except Exception:
                disconnected.add(connection)

        # Clean up disconnected clients
        for conn in disconnected:
            self.disconnect(conn)

    async def broadcast_to_symbol(self, symbol: str, event: StreamEvent) -> None:
        """
        Broadcast event to clients subscribed to a specific symbol.

        Args:
            symbol: Symbol to broadcast to
            event: Event to broadcast
        """
        symbol = symbol.upper()
        subscribers = self.symbol_subscriptions.get(symbol, set())

        if not subscribers:
            return

        message = event.to_json()
        disconnected = set()

        for connection in subscribers:
            try:
                await connection.send_text(message)
            except Exception:
                disconnected.add(connection)

        # Clean up
        for conn in disconnected:
            self.disconnect(conn)

    async def send_personal(self, websocket: WebSocket, event: StreamEvent) -> None:
        """Send event to a specific client."""
        try:
            await websocket.send_text(event.to_json())
        except Exception:
            self.disconnect(websocket)

    def get_stats(self) -> dict[str, Any]:
        """Get connection statistics."""
        return {
            "total_connections": len(self.active_connections),
            "symbol_subscriptions": {
                symbol: len(subscribers)
                for symbol, subscribers in self.symbol_subscriptions.items()
            },
        }


# Global connection manager
manager = ConnectionManager()


async def heartbeat_task(interval: int = 30) -> None:
    """
    Send periodic heartbeat to all connected clients.

    Helps detect stale connections and keeps connections alive.
    """
    while True:
        await asyncio.sleep(interval)
        if manager.active_connections:
            await manager.broadcast(StreamEvent.heartbeat())


class PubSubEventHandler:
    """
    Handles Pub/Sub messages and broadcasts to WebSocket clients.

    Subscribes to processed market data and forwards relevant events.
    """

    def __init__(self, project_id: str, subscription_id: str):
        self.project_id = project_id
        self.subscription_id = subscription_id
        self.subscriber = pubsub_v1.SubscriberClient()
        self.subscription_path = self.subscriber.subscription_path(
            project_id, subscription_id
        )

    async def start(self) -> None:
        """Start listening to Pub/Sub messages."""
        def callback(message: pubsub_v1.subscriber.message.Message) -> None:
            try:
                data = json.loads(message.data.decode("utf-8"))

                # Create appropriate event
                symbol = data.get("symbol", "UNKNOWN")
                sentiment_score = data.get("sentiment_score")
                price = data.get("price_usd")

                if sentiment_score:
                    event = StreamEvent.sentiment_update(
                        symbol=symbol,
                        score=sentiment_score,
                        category=data.get("sentiment_category", "NEUTRAL"),
                        reasoning=data.get("sentiment_reasoning", ""),
                    )

                    # Schedule broadcast (run in event loop)
                    asyncio.create_task(manager.broadcast_to_symbol(symbol, event))
                    asyncio.create_task(manager.broadcast(event))

                message.ack()

            except Exception as e:
                logger.error(f"Error processing Pub/Sub message: {e}")
                message.nack()

        # Start streaming pull
        streaming_pull = self.subscriber.subscribe(
            self.subscription_path,
            callback=callback,
        )

        logger.info(f"Listening for messages on {self.subscription_path}")

        try:
            streaming_pull.result()
        except Exception as e:
            logger.error(f"Pub/Sub subscriber error: {e}")
            streaming_pull.cancel()


# FastAPI WebSocket endpoint handler
async def websocket_endpoint(
    websocket: WebSocket,
    symbols: Optional[str] = None,
) -> None:
    """
    WebSocket endpoint for real-time market data.

    Query params:
        symbols: Comma-separated list of symbols to subscribe to

    Example:
        ws://api.aether.io/ws/stream?symbols=BTC,ETH
    """
    # Parse symbols
    symbol_list = None
    if symbols:
        symbol_list = [s.strip().upper() for s in symbols.split(",")]

    await manager.connect(websocket, symbols=symbol_list)

    try:
        while True:
            # Wait for client messages (subscriptions, pings)
            data = await websocket.receive_text()

            try:
                message = json.loads(data)
                action = message.get("action")

                if action == "subscribe":
                    # Add symbol subscription
                    new_symbols = message.get("symbols", [])
                    for symbol in new_symbols:
                        symbol = symbol.upper()
                        if symbol not in manager.symbol_subscriptions:
                            manager.symbol_subscriptions[symbol] = set()
                        manager.symbol_subscriptions[symbol].add(websocket)

                    await manager.send_personal(websocket, StreamEvent(
                        event_type=EventType.SYSTEM_STATUS,
                        symbol=None,
                        data={"subscribed": new_symbols},
                        timestamp=datetime.utcnow().isoformat(),
                    ))

                elif action == "unsubscribe":
                    # Remove symbol subscription
                    remove_symbols = message.get("symbols", [])
                    for symbol in remove_symbols:
                        symbol = symbol.upper()
                        if symbol in manager.symbol_subscriptions:
                            manager.symbol_subscriptions[symbol].discard(websocket)

                elif action == "ping":
                    await manager.send_personal(websocket, StreamEvent(
                        event_type=EventType.HEARTBEAT,
                        symbol=None,
                        data={"pong": True},
                        timestamp=datetime.utcnow().isoformat(),
                    ))

            except json.JSONDecodeError:
                pass

    except WebSocketDisconnect:
        manager.disconnect(websocket)
