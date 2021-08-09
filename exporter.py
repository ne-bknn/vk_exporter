import json
import os
import pathlib
import re
import shutil
import sqlite3
import sys
from collections import defaultdict
from collections.abc import Mapping
from getpass import getpass
from typing import Any, Callable, Dict, Hashable, Iterator, List, Tuple, Optional

import requests  # type: ignore
import typer
import vk_api as vk  # type: ignore
from vk_api import audio as vk_audio_api

DEBUG = False
app = typer.Typer()


if DEBUG:
    import IPython  # type: ignore
    from icecream import ic  # type: ignore

    repl = IPython.embed
else:

    def ic(*args):
        return None

    def repl(*args):
        return None


# output helpers
class LLog:
    @classmethod
    def info(cls, s: str) -> None:
        typer.secho(f"[.] {s}")

    @classmethod
    def success(cls, s: str) -> None:
        typer.secho(f"[+] {s}", fg=typer.colors.GREEN)

    @classmethod
    def err(cls, s: str) -> None:
        typer.secho(f"[-] {s}", fg=typer.colors.RED)


llog = LLog


def auth() -> Tuple[vk.vk_api.VkApi, vk.vk_api.VkApiMethod]:
    """Interactively authenticates user and returns api object"""

    def mfa_handler() -> Tuple[str, int]:
        code = input("OTP code: ")
        return code, 0

    if ".passwd" in os.listdir():
        login, password = [line.strip() for line in open(".passwd").readlines()]
    else:
        login = input("Email or phone number: ")
        password = getpass()

    session = vk.VkApi(login, password, auth_handler=mfa_handler)
    session.auth()

    api = session.get_api()

    # silly smoke test
    if api.users.get(user_ids="1")[0]["id"] == 1:
        llog.success("Auth successful")
    else:
        llog.err("User with ID 1 does not have ID 1, weird")

    return session, api


def get_posts(
    page_id: str, n_posts: int, api: vk.vk_api.VkApiMethod
) -> Iterator[List[Dict[str, Any]]]:
    total_posts = api.wall.get(domain=page_id, count=1, offset=0)["count"]

    n_posts = min(n_posts, total_posts) if n_posts != -1 else total_posts
    ic(n_posts)

    step = 100
    offset = 0

    while n_posts != 0:
        step_amount = min(step, n_posts)
        ic(step_amount)

        yield api.wall.get(domain=page_id, count=step_amount, offset=offset * step)[
            "items"
        ]

        n_posts -= step_amount
        offset += 1


def process_post_json(post: Dict[str, Any], api) -> Dict[str, Any]:
    def getter(*args: Hashable) -> Callable[[Mapping[Any, Any]], Any]:
        """Helper to retrieve data from heavily nested JSONs"""

        def _getter(x: Mapping[Any, Any]) -> Any:
            for k in args:
                x = x[k]

            return x

        return _getter

    def download_photo(photo: Dict[str, Any]) -> Dict[str, str]:
        photos = photo["photo"]["sizes"]
        best_pic = max(photos, key=getter("height"))
        url: str = best_pic["url"]
        return {"type": "photo", "url": url}

    def download_audio(audio: Dict[str, Any]) -> Dict[str, Any]:
        audio_id = audio["audio"]["id"]
        owner_id = audio["audio"]["owner_id"]
        return {"type": "audio", "id": audio_id, "owner_id": owner_id}

    def download_video(video: Dict[str, Any]) -> Optional[Dict[str, str]]:
        """Internal VK videos most likely wont be
        accessible if original page is not accessible"""

        ic(video)
        video = video["video"]

        video_id = video["id"]
        owner_id = video["owner_id"]
        try:
            access_key = video["access_key"]
        except KeyError:
            access_key = ""

        full_id = f"{owner_id}_{video_id}" + (
            f"_{access_key}" if access_key != "" else ""
        )
        ic(f"{owner_id}_{video_id}")
        real_video = api.video.get(videos=full_id, count=1, owner_id=owner_id)
        ic(real_video)
        url = real_video["items"][0]["player"]

        return {"type": "video", "url": url}

    text = post["text"]
    post_id = post["id"]

    try:
        attachments = post["attachments"]
    except KeyError:
        attachments = []

    real_attachments: List[Any] = []
    for attachment in attachments:
        if attachment["type"] == "photo":
            real_attachments.append(download_photo(attachment))

        if attachment["type"] == "audio":
            real_attachments.append(download_audio(attachment))

        if attachment["type"] == "video":
            real_attachments.append(download_video(attachment))

    res: Dict[str, Any] = {
        "text": text,
        "attachments": real_attachments,
        "post_id": post_id,
    }

    return res


def url_to_domain(url: str) -> str:
    domain_re = re.compile("^[a-zA-Z0-9_]{4,100}$")
    domain = url.split("/")[-1]
    if not domain_re.match(domain):
        llog.err(f'Something wrong with "{domain}" name')
        sys.exit()

    return domain


def domain_to_id(domain: str, api: vk.vk_api.VkApiMethod) -> int:
    data = api.utils.resolveScreenName(screen_name=domain)
    obj_type: str = data["type"]
    obj_id: int = data["object_id"]

    if obj_type == "group":
        obj_id = -obj_id

    return obj_id


def render_html(db: sqlite3.Connection) -> str:
    pass


def initialize_table(page_id: str) -> sqlite3.Connection:
    db = sqlite3.connect(f"cache/{page_id}/posts.db")
    sql_create_table = """CREATE TABLE IF NOT EXISTS posts (
            id INT NOT NULL PRIMARY KEY,
            text TEXT,
            photos TEXT,
            audios TEXT,
            videos TEXT);"""

    c = db.cursor()
    c.execute(sql_create_table)
    db.commit()

    return db


def save_html(htmls: List[str], page_id: str, post_id: int):
    wd = pathlib.PurePath("cache", page_id, "wikis", str(post_id))
    try:
        pathlib.Path(wd).mkdir(parents=True)
    except FileExistsError:
        if len(htmls) == list(os.listdir(pathlib.Path(wd))):
            llog.info(f"Wiki from {post_id} are downloaded")
            return

    for i, content in enumerate(htmls):
        with open(pathlib.PurePath(wd, str(i)), "w") as f:
            f.write(content)


def save_photos(photo_urls: List[str], page_id: str, post_id: int):
    wd = pathlib.PurePath("cache", page_id, "photos", str(post_id))
    try:
        pathlib.Path(wd).mkdir(parents=True)
    except FileExistsError:
        if len(photo_urls) == list(os.listdir(pathlib.Path(wd))):
            llog.info(f"Photos from {post_id} are downloaded")
            return

    photos = []
    for url in photo_urls:
        req = requests.get(url)
        try:
            req.raise_for_status()
        except requests.exceptions.HTTPError:
            llog.err("Failed fetching an image, URL is logged in err.log")
            with open("err.log", "a") as f:
                f.write(url + "\n")
        else:
            photos.append(req.content)

    for i, content in enumerate(photos):
        with open(pathlib.PurePath(wd, str(i)), "wb") as f:  # type: ignore
            f.write(content)


def save_audios(audio_objs: List[Dict[str, str]], page_id: str, post_id: int, session):
    wd = pathlib.PurePath("cache", page_id, "audios", str(post_id))
    audio_api = vk_audio_api.VkAudio(session)
    try:
        pathlib.Path(wd).mkdir(parents=True)
    except FileExistsError:
        if len(audio_objs) == list(os.listdir(pathlib.Path(wd))):
            llog.info(f"Audios from {post_id} are downloaded")
            return

    audios = []
    for audio_obj in audio_objs:
        content = audio_api.get_audio_by_id(audio_obj["owner_id"], audio_obj["id"])
        audios.append(content)

    audio_files = []
    for data in audios:
        url = data["url"]
        req = requests.get(url)
        try:
            req.raise_for_status()
        except requests.exceptions.HTTPError:
            llog.err("Failed fetching an audio, URL is logged in err.log")
            with open("err.log", "a") as f:
                f.write(url + "\n")
        else:
            audio_files.append(req.content)

    for i, content in enumerate(audio_files):
        with open(pathlib.PurePath(wd, str(i)), "wb") as f:  # type: ignore
            f.write(content)


def extract_wiki(text: str, numeric_page_id: int, api) -> List[str]:
    wiki_re = re.compile(f"https:\/\/vk\.com\/topic{numeric_page_id}_[\d]{{1,20}}")
    urls = wiki_re.findall(text)
    page_ids = [urls.split("_")[-1] for url in urls]
    resps = [
        api.pages.get(owner_id=numeric_page_id, page_id=page_id, need_html=1)
        for page_id in page_ids
    ]
    htmls = []
    for resp in resps:
        htmls.append(resp["html"])

    return htmls


def save_data(
    post: Dict[Any, Any], db: sqlite3.Connection, page_id: str, session
) -> None:
    post = defaultdict(str, post)
    c = db.cursor()
    sql_insert_post = """INSERT INTO posts (id, text, photos, audios, videos) VALUES (?, ?, ?, ?, ?)"""
    text = post["text"]
    post_id = post["post_id"]
    photos = [
        attachment
        for attachment in post["attachments"]
        if attachment["type"] == "photo"
    ]
    audios = [
        attachment
        for attachment in post["attachments"]
        if attachment["type"] == "audio"
    ]
    videos = [
        attachment
        for attachment in post["attachments"]
        if attachment["type"] == "video"
    ]
    try:
        c.execute(
            sql_insert_post,
            [
                post_id,
                text,
                json.dumps(photos),
                json.dumps(audios),
                json.dumps(videos),
            ],
        )
    except sqlite3.IntegrityError:
        llog.info(f"Post {post_id} is already processed")
    else:
        db.commit()

    save_photos([photo["url"] for photo in photos], page_id, post_id)
    save_audios(audios, page_id, post_id, session)

    api = session.get_api()
    htmls = extract_wiki(text, domain_to_id(page_id, api), api)
    save_html(htmls, page_id, post_id)


def init_working_directory(page_id: str):
    pathlib.Path(f"cache/{page_id}").mkdir(parents=True, exist_ok=True)

    for t in ["photos", "videos", "audios", "wikis"]:
        pathlib.Path(f"cache/{page_id}/{t}").mkdir(parents=True, exist_ok=True)


@app.command()
def run(url: str, n_posts: int = -1) -> None:
    """Run full set of actions: get posts, download media, rendering html"""
    session, api = auth()
    page_id = url_to_domain(url)

    init_working_directory(page_id)
    conn = initialize_table(page_id)

    for batch in get_posts(page_id, n_posts, api):
        for post in batch:
            data = process_post_json(post, api)
            save_data(data, conn, page_id, session)

    render_html(conn)

    conn.close()


@app.command()
def get(url: str, n_posts: int = -1, db_path: str = "./cache.db") -> None:
    """Download data only (no files)"""
    pass


@app.command()
def clean(url: str, full: bool = typer.Option(False, "-f")) -> None:
    """Clean the cache"""
    db_path = f"cache/{url_to_domain(url)}/posts.db"
    ic(db_path)
    if not pathlib.Path(db_path).exists():
        llog.info("There is no data associated with this URL")
        llog.info("There is nothing to do")
        return

    db = sqlite3.connect(db_path)
    c = db.cursor()
    sql_drop_table = "DROP TABLE IF EXISTS posts"
    c.execute(sql_drop_table)
    db.commit()

    llog.success("Dropped posts database")

    if full:
        shutil.rmtree(f"cache/{url_to_domain(url)}")
        llog.success("Deleted downloaded media")


@app.command()
def render(url: str) -> None:
    """Render HTML with data from DB"""
    db_path = f"cache/{url_to_domain(url)}/posts.db"
    if not pathlib.Path(db_path).exists():
        llog.err("There is no data associated with this URL")
        return

    llog.info("Rendering {url}")


if __name__ == "__main__":
    app()
