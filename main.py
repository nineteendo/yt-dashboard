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

Threshold = IntLiteral[50, 500, 5_000, 50_000]

UPLOAD_TYPES = ["short", "video"]
THRESHOLDS: list[Threshold] = [50, 500, 5_000, 50_000]
OLD_CHANNEL_LIMIT = 50_000
CHANNEL_LIMIT = max(THRESHOLDS)
BATCH_SIZE = 25
UPLOAD_HISTORY = 1444

LEASE_SECONDS = 864000
RENEW_BEFORE = timedelta(days=1)
CONFIRM_TIMEOUT = 1
KEEPALIVE_TIMEOUT = 15

BASE_DIR = Path(__file__).resolve().parent
CHANNEL_ICON = BASE_DIR / "NZ1.png"
CHANNELS_FILE = BASE_DIR / "channels.csv"
EXPIRY_FILE = BASE_DIR / "expiry.json"
LIVE_SUBSCRIPTIONS_PAGE = BASE_DIR / "index.html"
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

upload_windows: dict[tuple[str, int], deque[tuple[float, str]]] = {
    (upload_type, threshold): deque()
    for threshold in THRESHOLDS for upload_type in UPLOAD_TYPES
}
best_upload_windows: dict[tuple[str, int], deque[tuple[float, str]]] = {
    (upload_type, threshold): deque()
    for threshold in THRESHOLDS for upload_type in UPLOAD_TYPES
}
upload_queues: dict[tuple[str, int], deque[str]] = {
    (upload_type, threshold): deque(maxlen=UPLOAD_HISTORY)
    for threshold in THRESHOLDS for upload_type in UPLOAD_TYPES
}
upload_counts = {
    (upload_type, threshold): 0
    for threshold in THRESHOLDS for upload_type in UPLOAD_TYPES
}
upload_conditions = {
    (upload_type, threshold): asyncio.Condition()
    for threshold in THRESHOLDS for upload_type in UPLOAD_TYPES
}
subscription_condition = asyncio.Condition()
pending: dict[str, asyncio.Future[Any]] = {}
expiries: dict[str, float] = {}
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
    new_channels = list(channels)[OLD_CHANNEL_LIMIT:]
    log.debug(
        "Starting subscription batch: %d channels, batch_size=%d",
        len(new_channels),
        BATCH_SIZE
    )
    async with httpx.AsyncClient() as http:
        while len(expiries) < len(new_channels):
            missing = [
                channel for channel in new_channels if channel not in expiries
            ]
            for start in range(0, len(missing), BATCH_SIZE):
                batch = missing[start: start + BATCH_SIZE]
                results = await asyncio.gather(
                    *[_subscribe_one(channel, http) for channel in batch]
                )
                async with subscription_condition:
                    for channel, expiry in zip(batch, results):
                        if expiry is not None:
                            expiries[channel] = expiry
                            subscription_condition.notify_all()

                log.debug(
                    "Confirmed %d / %d subscriptions (%d pending)",
                    OLD_CHANNEL_LIMIT + len(expiries),
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
# Subscription broadcasting
# --------------------------------------------------

@app.get("/events")
async def _subscription_events():
    async def stream():
        yield "retry: 5000\n\n"
        yield (
            f"event: goal\n"
            f"data: {len(channels)}\n\n"
        )
        while True:
            yield (
                f"event: subscriptions\n"
                f"data: {OLD_CHANNEL_LIMIT + len(expiries)}\n\n"
            )
            while True:
                async with subscription_condition:
                    try:
                        await asyncio.wait_for(
                            subscription_condition.wait(),
                            timeout=KEEPALIVE_TIMEOUT
                        )
                        break
                    except TimeoutError:
                        yield ": keep-alive\n\n"

    return EventSourceResponse(
        stream(),
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


# --------------------------------------------------
# Upload broadcasting
# --------------------------------------------------

async def _broadcast_upload(upload: dict[str, Any]):
    if upload["url"].startswith(SHORTS_BASE_URL):
        upload_type = "short"
    else:
        upload_type = "video"

    loop = asyncio.get_running_loop()
    now = time.monotonic()
    message = json.dumps(upload, end="", separators=(",", ":"))
    for threshold in THRESHOLDS:
        upload_key = upload_type, threshold
        if channels[upload["channel_id"]] < threshold:
            upload_window = upload_windows[upload_key]
            upload_window.append((now, message))
            loop.call_later(60, upload_window.popleft)
            if len(upload_window) > len(best_upload_windows[upload_key]):
                best_upload_windows[upload_key] = upload_window.copy()

            async with (upload_condition := upload_conditions[upload_key]):
                upload_counts[upload_key] += 1
                upload_queues[upload_key].append(message)
                upload_condition.notify_all()


class _EventParams(BaseModel):
    top: Threshold = CHANNEL_LIMIT
    history: int = UPLOAD_HISTORY
    replay: bool = False


EventParams = Annotated[_EventParams, Query()]


async def _upload_events(upload_type: str, params: EventParams):
    async def stream():
        if params.replay:
            yield (
                "event: history\n"
                "data: 0\n\n"
            )
            replay_offset = None
            for timestamp, message in best_upload_windows[upload_key]:
                if replay_offset is None:
                    replay_offset = time.monotonic() - timestamp
                else:
                    new_timestamp = timestamp + replay_offset
                    await asyncio.sleep(new_timestamp - time.monotonic())

                yield (
                    f"event: {upload_type}\n"
                    f"data: {message}\n\n"
                )

            yield (
                "event: done\n"
                "data: \n\n"
            )
            return

        upload_count = upload_counts[upload_key]
        last_upload_id = upload_count - params.history
        yield "retry: 5000\n\n"
        yield (
            "event: history\n"
            f"data: {max(0, upload_count - max(0, last_upload_id))}\n\n"
        )
        while True:
            upload_queue = upload_queues[upload_key]
            upload_count = upload_counts[upload_key]
            start = max(0, len(upload_queue) - upload_count + last_upload_id)
            for i in range(start, len(upload_queue)):
                yield (
                    f"event: {upload_type}\n"
                    f"data: {upload_queue[i]}\n\n"
                )

            last_upload_id = upload_count
            async with (upload_condition := upload_conditions[upload_key]):
                try:
                    await asyncio.wait_for(
                        upload_condition.wait(), timeout=KEEPALIVE_TIMEOUT
                    )
                except TimeoutError:
                    yield ": keep-alive\n\n"

    upload_key = upload_type, params.top
    return EventSourceResponse(
        stream(),
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@app.get("/shorts/events")
async def _short_events(params: EventParams):
    return await _upload_events("short", params)


@app.get("/videos/events")
async def _video_events(params: EventParams):
    return await _upload_events("video", params)


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
        upload = {
            "id": get_text(YT_NS, "videoId"),
            "channel_id": get_text(YT_NS, "channelId"),
            "title": get_text(ATOM_NS, "title"),
            "published": get_text(ATOM_NS, "published"),
            "updated": get_text(ATOM_NS, "updated"),
            "url": None if link is None else link.get("href"),
        }
        if (
            upload["id"] is not None
            and upload["channel_id"] is not None
            and upload["title"] is not None
            and upload["published"] is not None
            and upload["updated"] is not None
            and upload["url"] is not None
        ):
            if (
                upload["channel_id"] in channels
                and timedelta(minutes=1) <= (
                    datetime.fromisoformat(upload["updated"])
                    - datetime.fromisoformat(upload["published"])
                ) <= timedelta(minutes=1, seconds=5)
            ):
                log.debug("New upload: %s", upload["title"])
                await _broadcast_upload(upload)
            else:
                log.debug("Ignoring upload: %s", upload["title"])

    return Response(status_code=204)


# --------------------------------------------------
# Other endpoints
# --------------------------------------------------

@app.get("/")
def _live_subscriptions():
    return FileResponse(LIVE_SUBSCRIPTIONS_PAGE)


@app.get("/NZ1.png")
def _channel_icon():
    return FileResponse(CHANNEL_ICON)


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
