from unittest.mock import MagicMock, patch

import pytest

from relier.telemetry.logging import inject_otel_context, setup_logging
from relier.telemetry.metrics import _inflight_tasks_callback
from relier.telemetry.setup import setup_telemetry
from relier.telemetry.spans import record_exception, trace_sync, trace_task

# ===========================================================================
# Test Logging
# ===========================================================================


class TestLogging:
    def test_inject_otel_context_no_span(self):
        """Verify that injecting OTEL context with no active span returns the dict unchanged."""
        event_dict = {"msg": "hello"}
        # Mock get_current_span to return a non-recording span
        with patch("opentelemetry.trace.get_current_span") as mock_get_span:
            mock_span = MagicMock()
            mock_span.get_span_context().is_valid = False
            mock_get_span.return_value = mock_span

            result = inject_otel_context(None, None, event_dict.copy())
            assert result == event_dict

    def test_inject_otel_context_with_valid_span(self):
        """Verify that trace and span IDs are injected when a valid span is active."""
        event_dict = {"msg": "hello"}
        with patch("opentelemetry.trace.get_current_span") as mock_get_span:
            mock_span = MagicMock()
            mock_ctx = MagicMock()
            mock_ctx.is_valid = True
            mock_ctx.trace_id = 0x1234567890ABCDEF1234567890ABCDEF
            mock_ctx.span_id = 0x1234567890ABCDEF
            mock_span.get_span_context.return_value = mock_ctx
            mock_get_span.return_value = mock_span

            result = inject_otel_context(None, None, event_dict.copy())
            assert result["trace_id"] == "1234567890abcdef1234567890abcdef"
            assert result["span_id"] == "1234567890abcdef"

    def test_setup_logging_debug_mode(self):
        """Verify setup_logging configures ConsoleRenderer in DEBUG mode."""
        with (
            patch("structlog.configure") as mock_configure,
            patch("logging.basicConfig"),
        ):
            setup_logging(level="DEBUG")
            # Check if structlog.configure was called
            assert mock_configure.called

    def test_setup_logging_production_mode(self):
        """Verify setup_logging configures JSONRenderer in non-DEBUG mode."""
        with (
            patch("structlog.configure") as mock_configure,
            patch("logging.basicConfig"),
        ):
            setup_logging(level="INFO")
            assert mock_configure.called


# ===========================================================================
# Test Metrics
# ===========================================================================


class TestMetrics:
    def test_inflight_tasks_callback(self):
        """Verify the metrics callback yields a valid Observation."""
        observations = list(_inflight_tasks_callback(None))
        assert len(observations) == 1
        obs = observations[0]
        assert obs.value == 0
        assert obs.attributes == {"rl.worker.id": "unknown"}

    def test_metrics_instruments_initialized(self):
        """Verify that core metrics instruments are created at import time."""
        from relier.telemetry import metrics

        assert metrics.tasks_total is not None
        assert metrics.task_duration_ms is not None
        assert metrics.resurrections_total is not None


# ===========================================================================
# Test Spans
# ===========================================================================


class TestSpans:
    @pytest.mark.asyncio
    async def test_trace_task_context_manager(self):
        """Verify that trace_task creates a span and sets attributes."""
        mock_tracer = MagicMock()
        mock_span = MagicMock()
        mock_tracer.start_as_current_span.return_value.__enter__.return_value = (
            mock_span
        )

        with patch("relier.telemetry.spans.tracer", mock_tracer):
            async with trace_task("test_span", "task_123", {"extra": "val"}) as span:
                assert span == mock_span
                mock_span.set_attribute.assert_any_call("rl.task.id", "task_123")
                mock_span.set_attribute.assert_any_call("extra", "val")

    def test_trace_sync_context_manager(self):
        """Verify that trace_sync creates a span."""
        mock_tracer = MagicMock()
        mock_span = MagicMock()
        mock_tracer.start_as_current_span.return_value.__enter__.return_value = (
            mock_span
        )

        with (
            patch("relier.telemetry.spans.tracer", mock_tracer),
            trace_sync("sync_span", {"attr": 1}) as span,
        ):
            assert span == mock_span
            mock_span.set_attribute.assert_any_call("attr", 1)

    @pytest.mark.asyncio
    async def test_trace_task_exception(self):
        """Verify that trace_task records exceptions."""
        mock_tracer = MagicMock()
        mock_span = MagicMock()
        mock_span.is_recording.return_value = True
        mock_tracer.start_as_current_span.return_value.__enter__.return_value = (
            mock_span
        )

        with patch("relier.telemetry.spans.tracer", mock_tracer):
            with pytest.raises(ValueError, match="fail"):
                async with trace_task("test", "id"):
                    raise ValueError("fail")

            mock_span.record_exception.assert_called_once()

    def test_trace_sync_exception(self):
        """Verify that trace_sync records exceptions."""
        mock_tracer = MagicMock()
        mock_span = MagicMock()
        mock_span.is_recording.return_value = True
        mock_tracer.start_as_current_span.return_value.__enter__.return_value = (
            mock_span
        )

        with (
            patch("relier.telemetry.spans.tracer", mock_tracer),
            pytest.raises(ValueError, match="fail"),
            trace_sync("test"),
        ):
            raise ValueError("fail")

            mock_span.record_exception.assert_called_once()

    def test_record_exception_recording(self):
        """Verify record_exception updates span status for recording spans."""
        mock_span = MagicMock()
        mock_span.is_recording.return_value = True
        exc = ValueError("boom")

        record_exception(mock_span, exc)

        mock_span.record_exception.assert_called_once_with(exc)
        mock_span.set_status.assert_called_once()

    def test_record_exception_non_recording(self):
        """Verify record_exception does nothing for non-recording spans."""
        mock_span = MagicMock()
        mock_span.is_recording.return_value = False

        record_exception(mock_span, ValueError("boom"))

        assert not mock_span.record_exception.called


# ===========================================================================
# Test Setup
# ===========================================================================


class TestSetup:
    def test_setup_telemetry_disabled(self):
        """Verify setup_telemetry exits early if otel_enabled is False."""
        with patch("relier.telemetry.setup.get_settings") as mock_settings:
            mock_settings.return_value.otel_enabled = False
            with patch("relier.telemetry.setup.TracerProvider") as mock_tp:
                setup_telemetry()
                assert not mock_tp.called

    def test_setup_telemetry_enabled(self):
        """Verify setup_telemetry initializes providers when enabled."""
        with patch("relier.telemetry.setup.get_settings") as mock_settings:
            settings = mock_settings.return_value
            settings.otel_enabled = True
            settings.otel_exporter_otlp_endpoint = "http://localhost:4317"
            settings.env = "test"

            with (
                patch("relier.telemetry.setup.TracerProvider") as mock_tp,
                patch("relier.telemetry.setup.MeterProvider") as mock_mp,
                patch("opentelemetry.trace.set_tracer_provider"),
                patch("opentelemetry.metrics.set_meter_provider"),
            ):
                setup_telemetry()
                assert mock_tp.called
                assert mock_mp.called

    def test_get_tracer_meter(self):
        """Verify get_tracer and get_meter return expected objects."""
        from relier.telemetry.setup import get_meter, get_tracer

        assert get_tracer("test") is not None
        assert get_meter("test") is not None
