"""Tests for websocket.bridge module."""
import pytest
import json
from unittest.mock import AsyncMock, MagicMock
from src.websocket.bridge import (
    _parse_trade,
    _parse_quote,
    _parse_index,
    _safe_float,
    _safe_int,
    PARSERS,
)


class TestSafeHelpers:
    """Tests for _safe_float and _safe_int."""

    def test_safe_float_valid(self):
        assert _safe_float("123.45") == 123.45
        assert _safe_float(123) == 123.0
        assert _safe_float(0) == 0.0

    def test_safe_float_none(self):
        assert _safe_float(None) == 0.0

    def test_safe_float_invalid(self):
        assert _safe_float("not a number") == 0.0
        assert _safe_float([]) == 0.0

    def test_safe_int_valid(self):
        assert _safe_int("123") == 123
        assert _safe_int(123.5) == 123
        assert _safe_int(0) == 0

    def test_safe_int_none(self):
        assert _safe_int(None) == 0

    def test_safe_int_invalid(self):
        assert _safe_int("abc") == 0
        assert _safe_int([]) == 0


class TestParseTrade:
    """Tests for _parse_trade."""

    def test_parse_valid_trade(self):
        raw = json.dumps({
            "Symbol": "VNM",
            "LastPrice": 25000,
            "Change": 150,  # 0.15 VND
            "RatioChange": 0.6,  # 0.6%
            "TotalVol": 1000000,
            "TotalVal": 25000000000,
            "Time": "09:30:15"
        })
        result = _parse_trade(raw)

        assert result is not None
        assert result["type"] == "price_update"
        assert result["symbol"] == "VNM"
        assert result["last_price"] == 25000.0
        assert result["change"] == 0.15  # 150 / 1000
        assert result["ratio_change"] == 0.6
        assert result["volume"] == 1000000
        assert result["value"] == 25000000000.0
        assert result["time"] == "09:30:15"

    def test_parse_trade_missing_symbol(self):
        raw = json.dumps({"LastPrice": 100})
        result = _parse_trade(raw)
        assert result is None

    def test_parse_trade_invalid_json(self):
        result = _parse_trade("not json")
        assert result is None

    def test_parse_trade_change_scaling(self):
        """Should correctly scale Change from integer to float."""
        raw = json.dumps({
            "Symbol": "ABC",
            "LastPrice": 1000,
            "Change": -500,  # -0.5 VND
            "RatioChange": -0.25,
            "TotalVol": 100,
            "TotalVal": 100000,
            "Time": "10:00:00"
        })
        result = _parse_trade(raw)
        assert result is not None
        assert result["change"] == -0.5


class TestParseQuote:
    """Tests for _parse_quote."""

    def test_parse_valid_quote(self):
        raw = json.dumps({
            "Symbol": "VNM",
            "BidPrice1": 24990, "BidVol1": 1000,
            "BidPrice2": 24980, "BidVol2": 500,
            "AskPrice1": 25000, "AskVol1": 800,
            "AskPrice2": 25010, "AskVol2": 1200,
            "Time": "09:30:15"
        })
        result = _parse_quote(raw)

        assert result is not None
        assert result["type"] == "orderbook_update"
        assert result["symbol"] == "VNM"
        assert len(result["bids"]) >= 2
        assert len(result["asks"]) >= 2
        assert result["bids"][0]["price"] == 24990.0
        assert result["bids"][0]["volume"] == 1000
        assert result["asks"][0]["price"] == 25000.0
        assert result["time"] == "09:30:15"

    def test_parse_quote_missing_symbol(self):
        raw = json.dumps({"BidPrice1": 100})
        result = _parse_quote(raw)
        assert result is None

    def test_parse_quote_empty_orderbook(self):
        raw = json.dumps({"Symbol": "ABC"})
        result = _parse_quote(raw)
        assert result is not None
        assert result["bids"] == []
        assert result["asks"] == []

    def test_parse_quote_handles_none_values(self):
        raw = json.dumps({
            "Symbol": "ABC",
            "BidPrice1": None,
            "BidVol1": 100,
            "AskPrice1": 200,
            "AskVol1": None
        })
        result = _parse_quote(raw)
        assert result is not None
        # Should skip None price/volume pairs
        assert len(result["bids"]) == 0
        assert len(result["asks"]) == 0


class TestParseIndex:
    """Tests for _parse_index."""

    def test_parse_valid_index(self):
        raw = json.dumps({
            "IndexId": "VNINDEX",
            "IndexValue": 1250.5,
            "Change": 1250,  # 1.25 points
            "RatioChange": 0.1,  # 0.1%
            "Advances": 150,
            "Declines": 100,
            "Time": "09:30:15"
        })
        result = _parse_index(raw)

        assert result is not None
        assert result["type"] == "index_update"
        assert result["index_id"] == "VNINDEX"
        assert result["index_value"] == 1250.5
        assert result["change"] == 1.25  # 1250 / 1000
        assert result["ratio_change"] == 0.1
        assert result["advances"] == 150
        assert result["declines"] == 100
        assert result["time"] == "09:30:15"

    def test_parse_index_missing_index_id(self):
        raw = json.dumps({"IndexValue": 100})
        result = _parse_index(raw)
        # Returns None because index_id is empty string (not falsy check)
        assert result is None

    def test_parse_index_negative_change(self):
        raw = json.dumps({
            "IndexId": "HNX",
            "IndexValue": 300.0,
            "Change": -300,  # -0.3 points
            "RatioChange": -0.1,
            "Advances": 50,
            "Declines": 80
        })
        result = _parse_index(raw)
        assert result is not None
        assert result["change"] == -0.3


class TestParsersDict:
    """Tests for PARSERS dict."""

    def test_parsers_has_required_topics(self):
        assert "market_data_trade" in PARSERS
        assert "market_data_quote" in PARSERS
        assert "index_data" in PARSERS

    def test_parsers_callable(self):
        assert callable(PARSERS["market_data_trade"])
        assert callable(PARSERS["market_data_quote"])
        assert callable(PARSERS["index_data"])


class TestKafkaBridgeLogic:
    """Tests for message routing logic in kafka_bridge_loop."""

    @pytest.mark.asyncio
    async def test_trade_message_routed_to_symbol_broadcast(self):
        """Should broadcast trade updates to symbol subscribers."""
        from src.websocket.manager import ws_manager

        # Mock the manager
        original_broadcast = ws_manager.broadcast_to_symbol
        ws_manager.broadcast_to_symbol = AsyncMock()

        # Simulate a trade message
        msg = {
            "topic": "market_data_trade",
            "value": {
                "Content": json.dumps({
                    "Symbol": "VNM",
                    "LastPrice": 25000,
                    "Change": 100,
                    "RatioChange": 0.4,
                    "TotalVol": 1000000,
                    "TotalVal": 25000000000,
                    "Time": "09:30:15"
                })
            }
        }

        # Simulate the message processing logic
        raw = msg["value"]["Content"]
        ws_msg = _parse_trade(raw)

        if ws_msg:
            msg_type = ws_msg.get("type")
            symbol = ws_msg.get("symbol")
            if msg_type == "price_update" and symbol:
                await ws_manager.broadcast_to_symbol(symbol, ws_msg)

        ws_manager.broadcast_to_symbol.assert_called_once()
        call_args = ws_manager.broadcast_to_symbol.call_args
        assert call_args[0][0] == "VNM"
        assert call_args[0][1]["type"] == "price_update"

        # Restore
        ws_manager.broadcast_to_symbol = original_broadcast

    @pytest.mark.asyncio
    async def test_quote_message_routed_to_symbol_broadcast(self):
        """Should broadcast quote updates to symbol subscribers."""
        from src.websocket.manager import ws_manager

        original_broadcast = ws_manager.broadcast_to_symbol
        ws_manager.broadcast_to_symbol = AsyncMock()

        msg = {
            "topic": "market_data_quote",
            "value": {
                "Content": json.dumps({
                    "Symbol": "FPT",
                    "BidPrice1": 100,
                    "BidVol1": 500,
                    "AskPrice1": 101,
                    "AskVol1": 300
                })
            }
        }

        raw = msg["value"]["Content"]
        ws_msg = _parse_quote(raw)

        if ws_msg:
            msg_type = ws_msg.get("type")
            symbol = ws_msg.get("symbol")
            if msg_type == "orderbook_update" and symbol:
                await ws_manager.broadcast_to_symbol(symbol, ws_msg)

        ws_manager.broadcast_to_symbol.assert_called_once()
        assert ws_manager.broadcast_to_symbol.call_args[0][0] == "FPT"

        ws_manager.broadcast_to_symbol = original_broadcast

    @pytest.mark.asyncio
    async def test_index_message_routed_to_broadcast_all(self):
        """Should broadcast index updates to all connections."""
        from src.websocket.manager import ws_manager

        original_broadcast = ws_manager.broadcast_all
        ws_manager.broadcast_all = AsyncMock()

        msg = {
            "topic": "index_data",
            "value": {
                "Content": json.dumps({
                    "IndexId": "VNINDEX",
                    "IndexValue": 1250.5,
                    "Change": 100,
                    "RatioChange": 0.08,
                    "Advances": 200,
                    "Declines": 150
                })
            }
        }

        raw = msg["value"]["Content"]
        ws_msg = _parse_index(raw)

        if ws_msg:
            msg_type = ws_msg.get("type")
            if msg_type == "index_update":
                await ws_manager.broadcast_all(ws_msg)

        ws_manager.broadcast_all.assert_called_once()
        broadcast_msg = ws_manager.broadcast_all.call_args[0][0]
        assert broadcast_msg["type"] == "index_update"
        assert broadcast_msg["index_id"] == "VNINDEX"

        ws_manager.broadcast_all = original_broadcast

    @pytest.mark.asyncio
    async def test_candlestick_message_passed_directly(self):
        """Should pass candlestick messages directly without parsing."""
        from src.websocket.manager import ws_manager

        original_broadcast = ws_manager.broadcast_to_symbol
        ws_manager.broadcast_to_symbol = AsyncMock()

        # Candlestick messages are sent directly (no Content wrapper)
        msg = {
            "topic": "candlestick_updates",
            "value": {
                "type": "candlestick_update",
                "symbol": "VNM",
                "interval": "1m",
                "timestamp": 1234567890,
                "open": 25000,
                "high": 25100,
                "low": 24990,
                "close": 25050,
                "volume": 10000
            }
        }

        raw = msg["value"]
        ws_msg = None

        # Simulate the candlestick branch
        if msg["topic"] == "candlestick_updates":
            if isinstance(raw, dict):
                ws_msg = raw

        if ws_msg:
            msg_type = ws_msg.get("type")
            symbol = ws_msg.get("symbol")
            if msg_type == "candlestick_update" and symbol:
                await ws_manager.broadcast_to_symbol(symbol, ws_msg)

        ws_manager.broadcast_to_symbol.assert_called_once()
        broadcast_msg = ws_manager.broadcast_to_symbol.call_args[0][1]
        assert broadcast_msg["type"] == "candlestick_update"
        assert broadcast_msg["symbol"] == "VNM"

        ws_manager.broadcast_to_symbol = original_broadcast

    @pytest.mark.asyncio
    async def test_message_without_content_skipped(self):
        """Should skip messages without Content field."""
        from src.websocket.manager import ws_manager

        original_broadcast = ws_manager.broadcast_to_symbol
        ws_manager.broadcast_to_symbol = AsyncMock()

        msg = {
            "topic": "market_data_trade",
            "value": {"SomeOtherField": "value"}  # No Content
        }

        raw = msg["value"]
        ws_msg = None

        if isinstance(raw, dict) and "Content" in raw:
            content_str = raw.get("Content", "")
            if content_str:
                parser = PARSERS.get(msg["topic"])
                if parser:
                    ws_msg = parser(content_str)

        assert ws_msg is None
        ws_manager.broadcast_to_symbol.assert_not_called()

        ws_manager.broadcast_to_symbol = original_broadcast

    @pytest.mark.asyncio
    async def test_unknown_topic_skipped(self):
        """Should skip messages from unknown topics."""
        from src.websocket.manager import ws_manager

        original_broadcast = ws_manager.broadcast_to_symbol
        ws_manager.broadcast_to_symbol = AsyncMock()

        msg = {
            "topic": "unknown_topic",
            "value": {"Content": json.dumps({"Symbol": "VNM"})}
        }

        raw = msg["value"]
        ws_msg = None

        if isinstance(raw, dict) and "Content" in raw:
            content_str = raw.get("Content", "")
            if content_str:
                parser = PARSERS.get(msg["topic"])  # None for unknown topic
                if parser:
                    ws_msg = parser(content_str)

        assert ws_msg is None
        ws_manager.broadcast_to_symbol.assert_not_called()

        ws_manager.broadcast_to_symbol = original_broadcast
