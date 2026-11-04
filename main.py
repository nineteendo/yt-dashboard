import asyncio
import csv
import logging
import xml.etree.ElementTree as ET
from collections import deque
from contextlib import asynccontextmanager, suppress
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import httpx
import jsonyx as json
from fastapi import FastAPI, Request, Response
from fastapi.responses import EventSourceResponse, FileResponse

# --------------------------------------------------
# Configuration
# --------------------------------------------------

CHANNEL_LIMIT = 3000
BATCH_SIZE = 10
EVENT_HISTORY = 1444

LEASE_SECONDS = 864000
RENEW_BEFORE = timedelta(days=1)
CONFIRM_TIMEOUT = 5
KEEPALIVE_TIMEOUT = 15
RETRY_DELAY = 60

BASE_DIR = Path(__file__).resolve().parent
CHANNELS_FILE = BASE_DIR / "channels.csv"
EXPIRY_FILE = BASE_DIR / "expiry.json"
ALL_SHORTS_PAGE = BASE_DIR / "shorts" / "index.html"
LATEST_SHORT_PAGE = BASE_DIR / "shorts" / "latest.html"
ALL_VIDEOS_PAGE = BASE_DIR / "videos" / "index.html"
LATEST_VIDEO_PAGE = BASE_DIR / "videos" / "latest.html"

ATOM_NS = "http://www.w3.org/2005/Atom"
YT_NS = "http://www.youtube.com/xml/schemas/2015"
HUB_URL = "https://pubsubhubbub.appspot.com/subscribe"
TOPIC_BASE_URL = "https://www.youtube.com/xml/feeds/videos.xml"
CALLBACK_URL = "https://brim-panther-laziness.ngrok-free.dev/callback"
SHORTS_BASE_URL = "https://www.youtube.com/shorts/"

# --------------------------------------------------
# Logging
# --------------------------------------------------

log = logging.getLogger("uvicorn.error")
log.setLevel(logging.DEBUG)

# --------------------------------------------------
# Application state (memory only)
# --------------------------------------------------

events: deque[tuple[int, str, str]] = deque(maxlen=EVENT_HISTORY)
current_event_id = 0
event_condition = asyncio.Condition()
pending: dict[str, asyncio.Future[Any]] = {}
channels: list[dict[str, str]] = []


# --------------------------------------------------
# WebSub subscription
# --------------------------------------------------

def _read_expiry():
    if not EXPIRY_FILE.exists():
        return None

    value = json.read(EXPIRY_FILE)
    return datetime.fromisoformat(value)


def _save_expiry(expiry: datetime):
    value = expiry.astimezone(UTC).isoformat()
    temporary = EXPIRY_FILE.with_suffix(".tmp")
    json.write(value, temporary)
    temporary.replace(EXPIRY_FILE)
    log.debug("Subscription expiry saved: %s", value)


async def _subscribe_one(channel_id: str, http: httpx.AsyncClient):
    topic = f"{TOPIC_BASE_URL}?channel_id={channel_id}"
    future = asyncio.get_running_loop().create_future()
    pending[topic] = future
    payload = {
        "hub.callback": CALLBACK_URL,
        "hub.topic": topic,
        "hub.mode": "subscribe",
        "hub.verify": "async",
        "hub.lease_seconds": str(LEASE_SECONDS),
    }
    try:
        response = await http.post(HUB_URL, data=payload)
        response.raise_for_status()
        return await asyncio.wait_for(future, timeout=CONFIRM_TIMEOUT)
    except Exception as error:
        log.warning("Subscription failed for %s: %s", channel_id, error)
        return None
    finally:
        pending.pop(topic, None)


async def _subscribe_all():
    log.debug(
        "Starting subscription batch: %d channels, batch_size=%d",
        len(channels),
        BATCH_SIZE
    )
    expiries = {}
    async with httpx.AsyncClient() as http:
        while len(expiries) < len(channels):
            missing = [
                channel
                for channel in channels
                if channel["id"] not in expiries
            ]
            for start in range(0, len(missing), BATCH_SIZE):
                batch = missing[start: start + BATCH_SIZE]
                results = await asyncio.gather(
                    *[_subscribe_one(channel["id"], http) for channel in batch]
                )
                for channel, expiry in zip(batch, results):
                    if expiry is not None:
                        expiries[channel["id"]] = expiry

                log.debug(
                    "Confirmed %d / %d subscriptions",
                    len(expiries),
                    len(channels)
                )

            if len(expiries) < len(channels):
                log.warning(
                    "%d subscriptions still pending. Retrying in %d seconds.",
                    len(channels) - len(expiries),
                    RETRY_DELAY
                )
                await asyncio.sleep(RETRY_DELAY)

    earliest_expiry = min(expiries.values())
    _save_expiry(earliest_expiry)
    log.debug("All subscriptions confirmed.")


async def _subscription_manager():
    while True:
        if (expiry := _read_expiry()) is not None:
            renewal_time = expiry - RENEW_BEFORE
            if (now := datetime.now(UTC)) < renewal_time:
                delay = (renewal_time - now).total_seconds()
                log.debug("Next renewal in %.1f hours", delay / 3600)
                await asyncio.sleep(delay)

        await _subscribe_all()


@asynccontextmanager
async def _lifespan(_app: FastAPI):
    with CHANNELS_FILE.open("r", encoding="utf-8-sig", newline="") as file:
        all_channels = list(csv.DictReader(file))
        channels[:] = all_channels[0:CHANNEL_LIMIT]

    log.debug("Loaded %d channels.", len(channels))
    subscription_task = asyncio.create_task(_subscription_manager())
    try:
        yield
    finally:
        subscription_task.cancel()
        with suppress(asyncio.CancelledError):
            await subscription_task


app = FastAPI(lifespan=_lifespan)


# --------------------------------------------------
# SSE broadcasting
# --------------------------------------------------

async def _broadcast(event: dict[str, Any]):
    async with event_condition:
        global current_event_id
        current_event_id += 1
        if event["url"].startswith(SHORTS_BASE_URL):
            event_type = "short"
        else:
            event_type = "video"

        message = json.dumps(event, end="", separators=(",", ":"))
        events.append((current_event_id, event_type, message))
        event_condition.notify_all()


@app.get("/events")
async def _events(request: Request):
    async def stream():
        if (last_event_id := request.headers.get("last-event-id")) is None:
            history = request.query_params.get("history", "1")
            try:
                last_event_id = current_event_id - int(history)
            except ValueError:
                log.warning("Invalid history: %s", history)
                last_event_id = current_event_id - 1
        else:
            try:
                last_event_id = int(last_event_id)
            except ValueError:
                log.warning("Invalid last event id: %s", last_event_id)
                last_event_id = current_event_id - 1

        while True:
            start = max(0, len(events) - current_event_id + last_event_id)
            for i in range(start, len(events)):
                event_id, event_type, message = events[i]
                yield (
                    f"id: {event_id}\n"
                    f"event: {event_type}\n"
                    f"data: {message}\n\n"
                )

            last_event_id = current_event_id
            async with event_condition:
                try:
                    await asyncio.wait_for(
                        event_condition.wait(), timeout=KEEPALIVE_TIMEOUT
                    )
                except TimeoutError:
                    yield ": keep-alive\n\n"
                    continue

    return EventSourceResponse(
        stream(),
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


# --------------------------------------------------
# WebSub callback
# --------------------------------------------------

@app.get("/callback")
def _callback_verification(request: Request):
    params = request.query_params
    mode = params.get("hub.mode")
    topic = params.get("hub.topic")
    challenge = params.get("hub.challenge")
    lease_seconds = params.get("hub.lease_seconds")
    if challenge is None:
        return Response(status_code=400)

    if mode == "subscribe" and topic is not None and lease_seconds is not None:
        if (future := pending.get(topic)) and not future.done():
            try:
                future.set_result(
                    datetime.now(UTC) + timedelta(seconds=int(lease_seconds))
                )
            except ValueError:
                log.warning("Invalid lease duration for %s", topic)

        log.debug("Subscription verified: %s", topic)
    elif mode == "unsubscribe" and topic is not None:
        log.debug("Unsubscription verified: %s", topic)

    return Response(challenge, media_type="text/plain")


@app.post("/callback")
async def _callback_notification(request: Request):
    body = await request.body()
    try:
        root = ET.fromstring(body)
    except ET.ParseError:
        log.warning("Received invalid XML notification.")
        return Response(status_code=400)

    for entry in root.findall(f".//{{{ATOM_NS}}}entry"):
        def get_text(namespace, name):
            element = entry.find(f"{{{namespace}}}{name}")
            return None if element is None else element.text

        link = entry.find(f"{{{ATOM_NS}}}link")
        video = {
            "video_id": get_text(YT_NS, "videoId"),
            "channel_id": get_text(YT_NS, "channelId"),
            "title": get_text(ATOM_NS, "title"),
            "published": get_text(ATOM_NS, "published"),
            "updated": get_text(ATOM_NS, "updated"),
            "url": None if link is None else link.get("href"),
        }
        if (
            video["video_id"] is not None
            and video["updated"] is not None
            and video["published"] is not None
            and video["url"] is not None
        ):
            if timedelta(minutes=1) <= (
                datetime.fromisoformat(video["updated"])
                - datetime.fromisoformat(video["published"])
            ) <= timedelta(minutes=1, seconds=5):
                log.debug(
                    "New video: %s (%s)", video["title"], video["channel_id"]
                )
                await _broadcast(video)
            else:
                log.debug(
                    "Ignoring video: %s (%s)",
                    video["title"],
                    video["channel_id"]
                )

    return Response(status_code=204)


# --------------------------------------------------
# Other endpoints
# --------------------------------------------------

@app.get("/shorts")
def _all_shorts():
    return FileResponse(ALL_SHORTS_PAGE)


@app.get("/shorts/latest")
def _latest_short():
    return FileResponse(LATEST_SHORT_PAGE)


@app.get("/videos")
def _all_videos():
    return FileResponse(ALL_VIDEOS_PAGE)


@app.get("/videos/latest")
def _latest_video():
    return FileResponse(LATEST_VIDEO_PAGE)
