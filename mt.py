#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Continuous BLE sender for Pico W 2 (MicroPython + aioble)
- Linux / BlueZ
- Bleak 0.22.3
- Explicit discovery + settle; auto-reconnect

Protocol:
    Frame = <4-byte little-endian length> + <JSON payload bytes>
"""

import asyncio
import json
import time
import warnings
from typing import Dict, Tuple

from bleak import BleakScanner, BleakClient, BleakError

# ====== GATT UUIDs (must match your Pico firmware) ======
SERVICE_UUID = "6E400001-B5A3-F393-E0A9-E50E24DCCA9E"
RX_UUID      = "6E400002-B5A3-F393-E0A9-E50E24DCCA9E"  # Write (PC -> Pico)
# TX_UUID    = "6E400003-B5A3-F393-E0A9-E50E24DCCA9E"  # Notify (optional Pico -> PC), not used here

# ====== Target identification ======
TARGET_ADDRESS = "2C:CF:67:E4:D5:10"    # your Pico MAC (recommended)
TARGET_NAME    = "PicoJSON"             # fallback match by name

# ====== Behavior tuning ======
SEND_INTERVAL_SEC = 0.20          # keep < watchdog timeout; 200 ms is plenty
DEFAULT_CHUNK = 20                # safe for ATT MTU 23; raise later if MTU>23 (e.g., 180)
RETRY_DELAY_SEC = 1.0
POST_CONNECT_SETTLE_SEC = 0.15    # small delay after connect before discovery (helps BlueZ)
DISCOVERY_RETRIES = 3
DISCOVERY_RETRY_DELAY = 0.2
ALWAYS_WRITE_WITH_RESPONSE = True  # start reliable; set False after Pico enables write_no_response

# Silence FutureWarnings since we intentionally call get_services() on 0.22.x
warnings.filterwarnings("ignore", category=FutureWarning)


def chunker(data: bytes, size: int):
    for i in range(0, len(data), size):
        yield data[i:i + size]


async def find_device():
    """Scan using a detection callback (gives AdvertisementData; avoids deprecated metadata)."""
    print("Scanning …")
    found: Dict[str, Tuple[object, object]] = {}

    def detection_callback(device, adv):
        found[device.address.upper()] = (device, adv)

    scanner = BleakScanner(detection_callback)
    await scanner.start()
    await asyncio.sleep(5.0)
    await scanner.stop()

    # 1) Prefer exact MAC
    if TARGET_ADDRESS:
        addr = TARGET_ADDRESS.upper()
        if addr in found:
            return found[addr][0]

    # 2) Fallback by name
    for dev, _adv in found.values():
        if getattr(dev, "name", "") == TARGET_NAME:
            return dev

    # 3) Fallback by advertised service UUID
    for dev, adv in found.values():
        svc_uuids = getattr(adv, "service_uuids", None) or []
        if any(u.lower() == SERVICE_UUID.lower() for u in svc_uuids):
            return dev

    raise RuntimeError("Device not found. Is it advertising?")


async def do_service_discovery(client: BleakClient):
    """
    Explicitly trigger discovery and verify RX characteristic exists.
    In Bleak 0.22.x, the services property may not trigger discovery on BlueZ,
    so we call get_services() (deprecated in future versions, but stable here).
    """
    last_exc = None
    for _ in range(DISCOVERY_RETRIES):
        try:
            await client.get_services()   # triggers discovery on 0.22.3
            svcs = client.services
            if not svcs:
                last_exc = RuntimeError("Services collection empty after discovery")
                await asyncio.sleep(DISCOVERY_RETRY_DELAY)
                continue
            rx_found = any(
                (c.uuid or "").lower() == RX_UUID.lower()
                for s in svcs for c in s.characteristics
            )
            if not rx_found:
                last_exc = RuntimeError("RX characteristic not found after discovery")
                await asyncio.sleep(DISCOVERY_RETRY_DELAY)
                continue
            return  # success
        except Exception as e:
            last_exc = e
            await asyncio.sleep(DISCOVERY_RETRY_DELAY)
    raise last_exc or RuntimeError("Service discovery failed")


async def send_loop_once():
    dev = await find_device()
    dev_name = getattr(dev, "name", "") or ""
    print(f"Connecting to {dev.address}: {dev_name}".strip())

    async with BleakClient(dev) as client:
        # Use property (no deprecation warning)
        if not client.is_connected:
            raise RuntimeError("Failed to connect")
        print("Connected")

        # Let BlueZ settle before discovery/GATT ops
        await asyncio.sleep(POST_CONNECT_SETTLE_SEC)

        # Force discovery & verify RX exists
        await do_service_discovery(client)

        seq = 0
        t0 = time.time()

        while True:
            if not client.is_connected:
                raise BleakError("Disconnected")

            now = time.time()
            payload_obj = {
                "seq": seq,
                "ts": now,                               # seconds since epoch
                "since_start_ms": int((now - t0) * 1000),
                "value": (seq % 100),
            }
            payload_bytes = json.dumps(payload_obj, separators=(",", ":")).encode("utf-8")
            frame = len(payload_bytes).to_bytes(4, "little") + payload_bytes

            try:
                # Reliability first: response=True for all frames.
                # After confirming Pico has write_no_response=True, set ALWAYS_WRITE_WITH_RESPONSE=False
                # to switch to response=False for throughput (keep a short settle + discovery above).
                for ch in chunker(frame, DEFAULT_CHUNK):
                    await client.write_gatt_char(RX_UUID, ch, response=ALWAYS_WRITE_WITH_RESPONSE)
            except (BleakError, asyncio.CancelledError, Exception) as e:
                print("Write failed/disconnected:", type(e).__name__, e)
                break

            seq += 1
            await asyncio.sleep(SEND_INTERVAL_SEC)


async def main():
    while True:
        try:
            await send_loop_once()
        except KeyboardInterrupt:
            print("Ctrl-C on PC: exiting.")
            return
        except Exception as e:
            print("Top-level error:", type(e).__name__, e)
        print(f"Reconnecting in {RETRY_DELAY_SEC}s …")
        await asyncio.sleep(RETRY_DELAY_SEC)


if __name__ == "__main__":
    asyncio.run(main())

