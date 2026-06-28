# websocket-feed-handler

Cliente WebSocket de alta velocidad con reconexión automática para recibir **trades en tiempo real** desde exchanges de criptomonedas. Es el primer módulo del ecosistema [quant-core-infra](https://github.com/juanmmm21/quant-core-infra) y actúa como capa de ingesta para proyectos posteriores como `order-book-reconstructor` y `market-data-lakehouse`.

Repositorio: [github.com/juanmmm21/websocket-feed-handler](https://github.com/juanmmm21/websocket-feed-handler)

---

## Objetivo

Este proyecto demuestra dominio de:

- Programación asíncrona avanzada (`asyncio` + `websockets`)
- Conexiones persistentes con tolerancia a fallos
- Parseo estructurado de mensajes JSON en caliente
- Diseño desacoplado mediante adapters por exchange

El feed emite eventos normalizados (`TradeEvent`) independientemente del exchange de origen, de modo que el resto del ecosistema no dependa de formatos propietarios.

---

## Exchanges soportados

| Exchange | Stream / Canal | URL base | Formato de símbolo |
|----------|----------------|----------|--------------------|
| **Binance** | `<symbol>@trade` | `wss://stream.binance.com:9443` | `BTCUSDT` |
| **Coinbase** | canal `matches` | `wss://ws-feed.exchange.coinbase.com` | `BTC-USD` |

La documentación oficial de Binance para trade streams está en [Binance Spot WebSocket Streams](https://developers.binance.com/docs/binance-spot-api-docs/web-socket-streams).

---

## Arquitectura

```text
┌─────────────────────┐
│   CLI / Consumer    │
└──────────┬──────────┘
           │ TradeEventHandler
           ▼
┌─────────────────────┐
│ ResilientWebSocket  │  reconexión exponencial + jitter
│       Feed          │
└──────────┬──────────┘
           │ ExchangeFeedAdapter
     ┌─────┴─────┐
     ▼           ▼
┌─────────┐ ┌──────────┐
│ Binance │ │ Coinbase │
│ Adapter │ │ Adapter  │
└─────────┘ └──────────┘
```

### Componentes principales

| Módulo | Responsabilidad |
|--------|-----------------|
| `models.py` | Tipos de dominio: `TradeEvent`, `FeedConfig`, `ReconnectPolicy` |
| `protocols.py` | Contratos (`ExchangeFeedAdapter`, `TradeMessageParser`, `FeedHandler`) |
| `feed_client.py` | Cliente genérico con reconexión y callbacks |
| `exchanges/` | Adapters y parsers específicos por exchange |
| `reconnect.py` | Cálculo de backoff exponencial con jitter |

### Decisiones técnicas

- **Precisión financiera:** precios y cantidades se parsean como `Decimal`, nunca `float`.
- **Interfaces primero:** cada exchange implementa un adapter; añadir soporte nuevo no requiere tocar el cliente base.
- **Errores explícitos:** fallos de red y parseo se registran y activan reconexión; nunca se silencian excepciones.
- **Memoria:** el cliente reutiliza el parser por sesión y evita crear objetos innecesarios en el hot path.

---

## Requisitos

- Python **3.11+**
- `pip` o cualquier gestor compatible con `pyproject.toml`

---

## Instalación

```bash
cd websocket-feed-handler
python -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
```

---

## Uso rápido (CLI)

### Binance

```bash
websocket-feed-handler --exchange binance --symbols BTCUSDT ETHUSDT
```

### Coinbase

```bash
websocket-feed-handler --exchange coinbase --symbols BTC-USD ETH-USD
```

Salida esperada (ejemplo):

```text
2026-06-28 12:00:01 INFO trade exchange=binance symbol=BTCUSDT id=123456 price=67234.50 qty=0.015 side=buy
```

---

## Uso programático

```python
import asyncio
import logging

from websocket_feed_handler import ExchangeId, FeedConfig, ResilientWebSocketFeed, TradeEvent
from websocket_feed_handler.exchanges import create_exchange_adapter

logging.basicConfig(level=logging.INFO)


async def on_trade(trade: TradeEvent) -> None:
    print(trade.symbol, trade.price, trade.quantity, trade.side.value)


async def main() -> None:
    config = FeedConfig(
        exchange=ExchangeId.BINANCE,
        symbols=("BTCUSDT",),
    )
    adapter = create_exchange_adapter(config.exchange)
    feed = ResilientWebSocketFeed(config=config, adapter=adapter)

    await feed.run(on_trade=on_trade)


if __name__ == "__main__":
    asyncio.run(main())
```

---

## Política de reconexión

Por defecto, `ReconnectPolicy` aplica:

| Parámetro | Valor por defecto |
|-----------|-------------------|
| Retardo inicial | 1.0 s |
| Retardo máximo | 60.0 s |
| Multiplicador | 2.0x |
| Jitter | 10 % |

La reconexión se detiene limpiamente con `await feed.stop()` o señales `SIGINT` / `SIGTERM` en la CLI.

---

## Desarrollo

### Ejecutar tests

```bash
pytest -q
```

### Lint y tipos

```bash
ruff check src tests
mypy src
```

---

## Roadmap del módulo

- [ ] Soporte para streams combinados avanzados de Binance (`aggTrade`, `bookTicker`)
- [ ] Métricas de latencia por mensaje (timestamp exchange vs recepción local)
- [ ] Publicación opcional a cola interna (`asyncio.Queue` / Redis / NATS) para `order-book-reconstructor`
- [ ] Tests de integración con mock WebSocket server

---

## Integración con quant-core-infra

```text
websocket-feed-handler  →  order-book-reconstructor  →  market-data-lakehouse
         │                           │
         └────── trade events ───────┘
```

Este módulo es la **fuente de verdad en tiempo real** del pipeline. Los proyectos downstream consumirán `TradeEvent` sin acoplarse a Binance ni Coinbase.

---

## Licencia

MIT
