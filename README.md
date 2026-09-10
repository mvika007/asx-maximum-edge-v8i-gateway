# ASX MAXIMUM EDGE V8-I Data Gateway V3.5.1

Diagnostic hardening build based on the V3.x timestamped iTick architecture.

Adds `asx_websocket_connection_diagnostic` with stage telemetry for preflight, WebSocket handshake, provider connected message, authentication observation, subscription send/ack, first market event, exact exception type/message, elapsed timing, and control/market message counts.

Retains the V3.5 background-job acceptance architecture and execution authorization as a separate, default-denied control.

Do not commit the real iTick token. Configure it only through the deployment environment.
