import asyncio
import csv
import logging
import time
import xml.etree.ElementTree as ET
from collections import deque
from contextlib import asynccontextmanager, suppress
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Annotated, Any, Literal, TypeVarTuple

import httpx
import jsonyx as json
from fastapi import FastAPI, Query, Request, Response
from fastapi.responses import EventSourceResponse, FileResponse
from pydantic import BaseModel, BeforeValidator

# --------------------------------------------------
# Configuration
# --------------------------------------------------

if TYPE_CHECKING:
    # pylint: disable-next=W0404
    from typing import Literal as IntLiteral
else:
    Ts = TypeVarTuple("Ts")
    IntLiteral = Annotated[Literal[*Ts], BeforeValidator(int)]

EVENT_TYPES = ["short", "video"]
THRESHOLDS: list[Literal[50, 500, 5000, 50000]] = [50, 500, 5000, 50000]
CHANNEL_LIMIT = max(THRESHOLDS)
BATCH_SIZE = 10
EVENT_HISTORY = 1444

LEASE_SECONDS = 864000
RENEW_BEFORE = timedelta(days=1)
CONFIRM_TIMEOUT = 5
KEEPALIVE_TIMEOUT = 15

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
CALLBACK_URL = "https://nicezombies.site/callback"
SHORTS_BASE_URL = "https://www.youtube.com/shorts/"

# --------------------------------------------------
# Logging
# --------------------------------------------------

log = logging.getLogger("uvicorn.error")
log.setLevel(logging.DEBUG)

# --------------------------------------------------
# Application state (memory only)
# --------------------------------------------------

event_windows: dict[tuple[str, int], deque[tuple[float, str]]] = {
    (event_type, threshold): deque()
    for threshold in THRESHOLDS for event_type in EVENT_TYPES
}
best_event_windows: dict[tuple[str, int], deque[tuple[float, str]]] = {
    (event_type, threshold): deque()
    for threshold in THRESHOLDS for event_type in EVENT_TYPES
}
event_queues: dict[tuple[str, int], deque[str]] = {
    (event_type, threshold): deque(maxlen=EVENT_HISTORY)
    for threshold in THRESHOLDS for event_type in EVENT_TYPES
}
event_counts = {
    (event_type, threshold): 0
    for threshold in THRESHOLDS for event_type in EVENT_TYPES
}
event_conditions = {
    (event_type, threshold): asyncio.Condition()
    for threshold in THRESHOLDS for event_type in EVENT_TYPES
}
pending: dict[str, asyncio.Future[Any]] = {}
channels: dict[str, int] = {}


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
    if (future := pending.get(topic)) is None:
        future = asyncio.get_running_loop().create_future()
        pending[topic] = future
    elif future.done():
        pending.pop(topic, None)
        return future.result()

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
    except httpx.HTTPError as error:
        log.error(
            "Subscription failed for %s (%s: %s)",
            channel_id,
            type(error).__qualname__,
            error
        )
        pending.pop(topic, None)
        await asyncio.sleep(CONFIRM_TIMEOUT)
        return None

    try:
        expiry = await asyncio.wait_for(
            asyncio.shield(future), timeout=CONFIRM_TIMEOUT
        )
    except TimeoutError:
        log.warning("Subscription timed out for %s", channel_id)
        return None

    pending.pop(topic, None)
    return expiry


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
                channel for channel in channels if channel not in expiries
            ]
            for start in range(0, len(missing), BATCH_SIZE):
                batch = missing[start: start + BATCH_SIZE]
                results = await asyncio.gather(
                    *[_subscribe_one(channel, http) for channel in batch]
                )
                for channel, expiry in zip(batch, results):
                    if expiry is not None:
                        expiries[channel] = expiry

                log.debug(
                    "Confirmed %d / %d subscriptions (%d pending)",
                    len(expiries),
                    len(channels),
                    len(pending)
                )

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
        all_channel_ids = [channel["id"] for channel in csv.DictReader(file)]
        channels.clear()
        channels.update({
            channel_id: i
            for i, channel_id in enumerate(all_channel_ids[0:CHANNEL_LIMIT])
        })

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
    if event["url"].startswith(SHORTS_BASE_URL):
        event_type = "short"
    else:
        event_type = "video"

    loop = asyncio.get_running_loop()
    now = time.monotonic()
    message = json.dumps(event, end="", separators=(",", ":"))
    for threshold in THRESHOLDS:
        event_key = event_type, threshold
        if channels[event["channel_id"]] < threshold:
            event_window = event_windows[event_key]
            event_window.append((now, message))
            loop.call_later(60, event_window.popleft)
            if len(event_window) > len(best_event_windows[event_key]):
                best_event_windows[event_key] = event_window.copy()

            async with (event_condition := event_conditions[event_key]):
                event_counts[event_key] += 1
                event_queues[event_key].append(message)
                event_condition.notify_all()


class _EventParams(BaseModel):
    top: IntLiteral[50, 500, 5000, 50000] = CHANNEL_LIMIT
    history: int = EVENT_HISTORY
    replay: bool = False


EventParams = Annotated[_EventParams, Query()]


async def _events(event_type: str, params: EventParams):
    async def stream():
        if params.replay:
            yield (
                "event: history\n"
                "data: 0\n\n"
            )
            last_timestamp = None
            for timestamp, message in best_event_windows[event_key]:
                if last_timestamp is not None:
                    await asyncio.sleep(timestamp - last_timestamp)

                yield (
                    f"event: {event_type}\n"
                    f"data: {message}\n\n"
                )
                last_timestamp = timestamp

            yield (
                "event: done\n"
                "data: \n\n"
            )
            return

        event_count = event_counts[event_key]
        last_event_id = event_count - params.history
        yield "retry: 5000\n\n"
        yield (
            "event: history\n"
            f"data: {max(0, event_count - max(0, last_event_id))}\n\n"
        )
        while True:
            event_queue = event_queues[event_key]
            event_count = event_counts[event_key]
            start = max(0, len(event_queue) - event_count + last_event_id)
            for i in range(start, len(event_queue)):
                yield (
                    f"event: {event_type}\n"
                    f"data: {event_queue[i]}\n\n"
                )

            last_event_id = event_count
            async with (event_condition := event_conditions[event_key]):
                try:
                    await asyncio.wait_for(
                        event_condition.wait(), timeout=KEEPALIVE_TIMEOUT
                    )
                except TimeoutError:
                    yield ": keep-alive\n\n"
                    continue

    event_key = event_type, params.top
    return EventSourceResponse(
        stream(),
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@app.get("/shorts/events")
async def _short_events(params: EventParams):
    return await _events("short", params)


@app.get("/videos/events")
async def _video_events(params: EventParams):
    return await _events("video", params)


# --------------------------------------------------
# WebSub callback
# --------------------------------------------------

@app.get("/callback")
def _callback_verification(
    mode: Literal["subscribe", "unsubscribe"] = Query(alias="hub.mode"),
    topic: str = Query(alias="hub.topic"),
    challenge: str = Query(alias="hub.challenge"),
    lease_seconds: int = Query(LEASE_SECONDS, alias="hub.lease_seconds")
):
    if mode == "subscribe":
        if (future := pending.get(topic)) and not future.done():
            future.set_result(
                datetime.now(UTC) + timedelta(seconds=lease_seconds)
            )

        log.debug("Subscription verified: %s", topic)
    else:
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
            # pylint: disable-next=W0640
            element = entry.find(f"{{{namespace}}}{name}")  # noqa: B023
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
            and video["channel_id"] is not None
            and video["title"] is not None
            and video["published"] is not None
            and video["updated"] is not None
            and video["url"] is not None
        ):
            if (
                video["channel_id"] in channels
                and timedelta(minutes=1) <= (
                    datetime.fromisoformat(video["updated"])
                    - datetime.fromisoformat(video["published"])
                ) <= timedelta(minutes=1, seconds=5)
            ):
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
def _all_shorts(_params: EventParams):
    return FileResponse(ALL_SHORTS_PAGE)


@app.get("/shorts/latest")
def _latest_short(_params: EventParams):
    return FileResponse(LATEST_SHORT_PAGE)


@app.get("/videos")
def _all_videos(_params: EventParams):
    return FileResponse(ALL_VIDEOS_PAGE)


@app.get("/videos/latest")
def _latest_video(_params: EventParams):
    return FileResponse(LATEST_VIDEO_PAGE)
