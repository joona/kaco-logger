# KACO Logger

Logger pulls realtime electricity production information from KACO Blueplanet inverter via HTTP interface and publishes data to MQTT channel.

Logger also polls cumulative total production and daily production amounts from the inverter filesystem.

Logger does not try to calculate production values on the fly, but relies on values reported by the inverter during the sampling time.

# Configuration

| ENV Variable | Description | Default |
| --- | --- | -- |
| KACO_URL | Inverter web interface base URL | null |
| MQTT_URL | URI for MQTT server | null |
| SOURCE_NAME | MQTT source name for logger | pv |
| INTERVAL_REALTIME | Polling interval for realtime production information in seconds | 5 |
| INTERVAL_TOTAL | Polling interval for total production information in seconds | 60 |

# MQTT topics

Logger sends messages to following topics. Integer and float values will be converted to string format upon sending.

| Topic | Description |
| --- | --- |
| `production/$sourceName/current_power` | Current production power in watts (W) |
| `production/$sourceName/current_power_kw` | Current production power in kilowatts (kW) |
| `production/$sourceName/daily` | Daily electricity production counter in kilowatt hours (kWh) |
| `production/$sourceName/total` | Cumulative total electricity production counter in kilowatt hours (kWh) |
