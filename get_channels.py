import csv
import time
import urllib.error
import urllib.parse
import urllib.request
from typing import Any

import jsonyx as json

API_KEY = json.read("api_key.json")[0]
API_BASE_URL = "https://www.googleapis.com/youtube/v3/channels"

INPUT_FILE = "channels.csv"
OUTPUT_FILE = "channels-.csv"
BATCH_SIZE = 50
COLUMNS = [
    "id",
    "created",
    "name",
    "handle",
    "country",
    "subscribers",
    "views",
    "videos",
    "for_kids",
    "topics",
    "sub_topics",
]

TOPICS = {
    "/m/04rlf": ("Music", None),
    "/m/02mscn": ("Music", "Christian music"),
    "/m/0ggq0m": ("Music", "Classical music"),
    "/m/01lyv": ("Music", "Country"),
    "/m/02lkt": ("Music", "Electronic music"),
    "/m/0glt670": ("Music", "Hip hop music"),
    "/m/05rwpb": ("Music", "Independent music"),
    "/m/03_d0": ("Music", "Jazz"),
    "/m/028sqc": ("Music", "Music of Asia"),
    "/m/0g293": ("Music", "Music of Latin America"),
    "/m/064t9": ("Music", "Pop music"),
    "/m/06cqb": ("Music", "Reggae"),
    "/m/06j6l": ("Music", "Rhythm and blues"),
    "/m/06by7": ("Music", "Rock music"),
    "/m/0gywn": ("Music", "Soul music"),
    "/m/0bzvm2": ("Gaming", None),
    "/m/025zzc": ("Gaming", "Action game"),
    "/m/02ntfj": ("Gaming", "Action-adventure game"),
    "/m/0b1vjn": ("Gaming", "Casual game"),
    "/m/02hygl": ("Gaming", "Music video game"),
    "/m/04q1x3q": ("Gaming", "Puzzle video game"),
    "/m/01sjng": ("Gaming", "Racing video game"),
    "/m/0403l3g": ("Gaming", "Role-playing video game"),
    "/m/021bp2": ("Gaming", "Simulation video game"),
    "/m/022dc6": ("Gaming", "Sports game"),
    "/m/03hf_rm": ("Gaming", "Strategy video game"),
    "/m/06ntj": ("Sports", None),
    "/m/0jm_": ("Sports", "American football"),
    "/m/018jz": ("Sports", "Baseball"),
    "/m/018w8": ("Sports", "Basketball"),
    "/m/01cgz": ("Sports", "Boxing"),
    "/m/09xp_": ("Sports", "Cricket"),
    "/m/02vx4": ("Sports", "Football"),
    "/m/037hz": ("Sports", "Golf"),
    "/m/03tmr": ("Sports", "Ice hockey"),
    "/m/01h7lh": ("Sports", "Mixed martial arts"),
    "/m/0410tth": ("Sports", "Motorsport"),
    "/m/07bs0": ("Sports", "Tennis"),
    "/m/07_53": ("Sports", "Volleyball"),
    "/m/02jjt": ("Entertainment", None),
    "/m/09kqc": ("Entertainment", "Humor"),
    "/m/02vxn": ("Entertainment", "Movies"),
    "/m/05qjc": ("Entertainment", "Performing arts"),
    "/m/066wd": ("Entertainment", "Professional wrestling"),
    "/m/0f2f9": ("Entertainment", "TV shows"),
    "/m/019_rr": ("Lifestyle", None),
    "/m/032tl": ("Lifestyle", "Fashion"),
    "/m/027x7n": ("Lifestyle", "Fitness"),
    "/m/02wbm": ("Lifestyle", "Food"),
    "/m/03glg": ("Lifestyle", "Hobby"),
    "/m/068hy": ("Lifestyle", "Pets"),
    "/m/041xxh": ("Lifestyle", "Beauty"),
    "/m/07c1v": ("Lifestyle", "Technology"),
    "/m/07bxq": ("Lifestyle", "Tourism"),
    "/m/07yv9": ("Lifestyle", "Vehicles"),
    "/m/098wr": ("Society", None),
    "/m/09s1f": ("Society", "Business"),
    "/m/0kt51": ("Society", "Health"),
    "/m/01h6rj": ("Society", "Military"),
    "/m/05qt0": ("Society", "Politics"),
    "/m/06bvp": ("Society", "Religion"),
    "/m/01k8wb": ("Other", "Knowledge")
}


def _fetch_channels(channel_ids: list[str]):
    params = {
        "part": "snippet,statistics,status,topicDetails",
        "id": ",".join(channel_ids),
        "key": API_KEY,
    }
    url = f"{API_BASE_URL}?{urllib.parse.urlencode(params)}"
    request = urllib.request.Request(url)
    for attempt in range(5):
        try:
            with urllib.request.urlopen(request, timeout=30) as response:
                return json.load(response)
        except urllib.error.HTTPError as error:
            body = error.read().decode("utf-8", errors="replace")
            if error.code in (429, 500, 502, 503, 504):
                delay = 2 ** attempt
                print(f"HTTP {error.code}. Retrying in {delay}s...")
                time.sleep(delay)
                continue

            raise RuntimeError(f"API error {error.code}: {body}") from None
        except urllib.error.URLError as error:
            delay = 2 ** attempt
            print(f"Network error: {error}. Retrying in {delay}s...")
            time.sleep(delay)

    raise RuntimeError("Maximum retries exceeded")


def _get_row(channel: dict[str, Any]):
    snippet = channel.get("snippet", {})
    statistics = channel.get("statistics", {})
    status = channel.get("status", {})
    topic_details = channel.get("topicDetails", {})
    topic_ids = topic_details.get("topicIds", [])
    topics = []
    subtopics = []
    for topic_id in topic_ids:
        if mapping := TOPICS.get(topic_id):
            parent, subtopic = mapping
            if parent not in topics:
                topics.append(parent)

            if subtopic is not None and subtopic not in subtopics:
                subtopics.append(subtopic)

    return {
        "id": channel["id"],
        "created": snippet.get("publishedAt"),
        "name": snippet.get("title"),
        "handle": snippet.get("customUrl"),
        "country": snippet.get("country"),
        "subscribers": statistics.get("subscriberCount", 0),
        "views": statistics.get("viewCount", 0),
        "videos": statistics.get("videoCount", 0),
        "for_kids": status.get("madeForKids", False),
        "topics": ";".join(topics),
        "sub_topics": ";".join(subtopics)
    }


def _main():
    with open(INPUT_FILE, "r", encoding="utf-8-sig", newline="") as file:
        channel_ids = [row["id"] for row in csv.DictReader(file)]

    total = len(channel_ids)
    print(f"Found {total:,} channel IDs.")
    fetched = 0
    missing = 0
    with open(OUTPUT_FILE, "w", newline="", encoding="utf-8-sig") as file:
        writer = csv.DictWriter(file, fieldnames=COLUMNS)
        writer.writeheader()
        for i, start in enumerate(range(0, len(channel_ids), BATCH_SIZE)):
            batch = channel_ids[start: start + BATCH_SIZE]
            response = _fetch_channels(batch)
            for channel in (channels := response.get("items", [])):
                writer.writerow(_get_row(channel))

            fetched += len(channels)
            missing += len(batch) - len(channels)
            file.flush()
            print(
                f"Batch {i}: "
                f"{fetched:,}/{total:,} fetched "
                f"({missing:,} unavailable)"
            )

    print(f"\nFinished. Exported {fetched:,} channels to {OUTPUT_FILE}.")


if __name__ == "__main__":
    _main()
