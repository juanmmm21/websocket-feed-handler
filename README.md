# websocket-feed-handler

Cliente WebSocket de alta velocidad con reconexión automática para recibir **trades en tiempo real** desde exchanges de criptomonedas. Es el **primer módulo** del ecosistema [quant-core-infra](https://github.com/juanmmm21/quant-core-infra) y actúa como capa de ingesta: todo lo que ocurre después (libro de órdenes, lakehouse, señales, backtest) depende de los ticks normalizados que emite este proyecto.

Repositorio: [github.com/juanmmm21/websocket-feed-handler](https://github.com/juanmmm21/websocket-feed-handler)

---

## Qué es y qué problema resuelve

Los exchanges publican trades en formatos JSON distintos, con campos, precisiones y convenciones de símbolo propietarias. Construir lógica de trading directamente sobre esos payloads acopla el sistema a un proveedor concreto y hace imposible cambiar de exchange sin reescribir código.

Este módulo resuelve ese problema con un **adaptador por exchange** que traduce mensajes crudos a un único tipo de dominio: `TradeEvent`. El cliente WebSocket se encarga de la conexión persistente, la reconexión con backoff y el parseo en caliente; el resto del ecosistema solo ve eventos normalizados.

---

## Rol en quant-core-infra

```text
[Binance / Coinbase WebSocket]
            │
            ▼
   websocket-feed-handler  ◄── estás aquí (ingesta de trades)
            │
            ├──► order-book-reconstructor  (profundidad L2, otro canal WS)
            ├──► market-data-lakehouse     (agregación OHLCV + Parquet)
            └──► event-driven-backtester   (replay histórico desde JSONL)
```

Es la **fuente de verdad en tiempo real** para precios y volumen ejecutado. No persiste datos por sí mismo: delega el almacenamiento a `market-data-lakehouse` o a consumidores propios.

---

## Objetivo

Este proyecto demuestra dominio de:

- Programación asíncrona avanzada (`asyncio` + `websockets`)
- Conexiones persistentes con tolerancia a fallos
- Parseo estructurado de mensajes JSON en el hot path
- Diseño desacoplado mediante adapters e interfaces (`Protocol`)

---

## Cómo funciona (ciclo de vida)

1. **Configuración:** se define exchange, símbolos y política de reconexión en `FeedConfig`.
2. **Conexión:** `ResilientWebSocketFeed` abre el WebSocket usando la URL y mensajes de suscripción del adapter.
3. **Recepción:** cada mensaje JSON pasa por el parser del exchange (`TradeMessageParser`).
4. **Normalización:** el parser devuelve un `TradeEvent` o `None` si el mensaje no es un trade.
5. **Entrega:** el callback `on_trade` recibe el evento tipado con `Decimal` en precio y cantidad.
6. **Fallo de red:** se registra el error, se cierra la conexión y se programa reconexión con backoff exponencial + jitter.
7. **Parada limpia:** `await feed.stop()` o `SIGINT`/`SIGTERM` en la CLI terminan el bucle sin dejar sockets huérfanos.

---

## Exchanges soportados

| Exchange | Stream / Canal | URL base | Formato de símbolo |
|----------|----------------|----------|--------------------|
| **Binance** | `<symbol>@trade` | `wss://stream.binance.com:9443` | `BTCUSDT` |
| **Coinbase** | canal `matches` | `wss://ws-feed.exchange.coinbase.com` | `BTC-USD` |

Documentación oficial Binance: [Binance Spot WebSocket Streams](https://developers.binance.com/docs/binance-spot-api-docs/web-socket-streams).

---

## Arquitectura

```text
┌─────────────────────┐
│   CLI / Consumer    │
└──────────┬──────────┘
           │ TradeEventHandler (async callback)
           ▼
┌─────────────────────┐
│ ResilientWebSocket  │  reconexión exponencial + jitter
│       Feed          │
└──────────┬──────────┘
           │ ExchangeFeedAdapter (Protocol)
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
| `models.py` | `TradeEvent`, `FeedConfig`, `ReconnectPolicy`, `ExchangeId` |
| `protocols.py` | Contratos: `ExchangeFeedAdapter`, `TradeMessageParser`, `FeedHandler` |
| `feed_client.py` | Cliente genérico con reconexión, ping y callbacks |
| `exchanges/binance.py` | Parser y adapter para stream `@trade` |
| `exchanges/coinbase.py` | Parser y adapter para canal `matches` |
| `reconnect.py` | Cálculo de backoff exponencial con jitter aleatorio |

### Decisiones técnicas

- **Precisión financiera:** precios y cantidades son `Decimal`, nunca `float`.
- **Interfaces primero:** añadir un exchange nuevo = implementar un adapter; el cliente base no cambia.
- **Errores explícitos:** fallos de red y parseo se registran y activan reconexión; nunca `except: pass`.
- **Memoria:** el parser se reutiliza por sesión; se evita crear objetos innecesarios por mensaje.

---

## Modelo de datos: `TradeEvent`

| Campo | Tipo | Descripción |
|-------|------|-------------|
| `exchange` | `ExchangeId` | `binance` o `coinbase` |
| `symbol` | `str` | Símbolo normalizado del exchange |
| `trade_id` | `str` | Identificador único del trade en el exchange |
| `price` | `Decimal` | Precio de ejecución |
| `quantity` | `Decimal` | Cantidad ejecutada |
| `side` | `TradeSide` | `buy` o `sell` (lado agresor) |
| `event_time` | `datetime` | Timestamp UTC del exchange |

Ejemplo JSONL compatible con `market-data-lakehouse`:

```json
{
  "exchange": "binance",
  "symbol": "BTCUSDT",
  "trade_id": "12345",
  "price": "67234.50",
  "quantity": "0.015",
  "side": "buy",
  "event_time": "2024-01-01T12:00:01Z"
}
```

---

## Requisitos

- Python **3.11+**
- `pip` compatible con `pyproject.toml`

---

## Instalación

```bash
cd websocket-feed-handler
python -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
```

---

## Uso CLI

### Binance

```bash
websocket-feed-handler --exchange binance --symbols BTCUSDT ETHUSDT
```

### Coinbase

```bash
websocket-feed-handler --exchange coinbase --symbols BTC-USD ETH-USD
```

### Opciones útiles

| Flag | Descripción |
|------|-------------|
| `--exchange` | `binance` o `coinbase` |
| `--symbols` | Lista de símbolos separados por espacio |
| `--log-level` | `DEBUG`, `INFO`, `WARNING`, `ERROR` |

Salida esperada:

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

`ReconnectPolicy` por defecto:

| Parámetro | Valor | Efecto |
|-----------|-------|--------|
| `initial_delay_seconds` | 1.0 s | Primer reintento tras desconexión |
| `max_delay_seconds` | 60.0 s | Tope del backoff |
| `backoff_multiplier` | 2.0 | Duplica el retardo en cada intento |
| `jitter_ratio` | 0.1 | Aleatoriedad ±10 % para evitar thundering herd |

La reconexión se detiene con `await feed.stop()` o señales `SIGINT` / `SIGTERM` en la CLI.

---

## Desarrollo

```bash
pytest -q
ruff check src tests
mypy src
```

---

## Roadmap del módulo

- [ ] Streams avanzados Binance (`aggTrade`, `bookTicker`)
- [ ] Métricas de latencia (timestamp exchange vs recepción local)
- [ ] Publicación opcional a cola (`asyncio.Queue` / Redis / NATS)
- [ ] Tests de integración con mock WebSocket server

---

## Licencia

MIT
