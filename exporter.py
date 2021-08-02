import json
from collections import defaultdict
import re
import sqlite3
import sys
from collections.abc import Mapping
from getpass import getpass
from typing import Any, Callable, Dict, Hashable, Iterator, List, Tuple

import typer
import vk_api as vk  # type: ignore

DEBUG = True
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


def auth() -> vk.vk_api.VkApiMethod:
    """Interactively authenticates user and returns api object"""

    def mfa_handler() -> Tuple[str, int]:
        code = input("OTP code: ")
        return code, 0

    login = input("Email or phone number: ")
    password = getpass()

    session = vk.VkApi(login, password, auth_handler=mfa_handler)
    session.auth()

    api = session.get_api()

    # silly smoke test
    if api.users.get(user_ids="1")[0]["id"] == 1:
        llog.success("Auth successful")

    return api


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


def process_post_json(post: Dict[str, Any]) -> Dict[str, Any]:
    def getter(*args: Hashable) -> Callable[[Mapping[Any, Any]], Any]:
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
        return {"type": "audio", "url": "not_implemented"}

    text = post["text"]
    post_id = post["id"]
    attachments = post["attachments"]
    real_attachments: List[Any] = []
    for attachment in attachments:
        if attachment["type"] == "photo":
            real_attachments.append(download_photo(attachment))

        if attachment["type"] == "audio":
            real_attachments.append(download_audio(attachment))

    res: Dict[str, Any] = {
        "text": text,
        "attachments": real_attachments,
        "post_id": post_id,
    }

    return res


def url_to_domain(url: str) -> str:
    screenname_re = re.compile("^[a-zA-Z0-9_]{4,100}$")
    screenname = url.split("/")[-1]
    if not screenname_re.match(screenname):
        llog.err('Something wrong with "{screenname}" name')
        sys.exit()

    return screenname


def domain_to_id(domain: str, api: vk.vk_api.VkApiMethod) -> int:
    data = api.utils.resolveScreenName(screen_name=domain)["response"]
    obj_type: str = data["type"]
    obj_id: int = data["object_id"]

    if obj_type == "group":
        obj_id = -obj_id

    return obj_id


def render_html(db: sqlite3.Connection) -> str:
    pass


def initialize_table(db_path: str) -> sqlite3.Connection:
    db = sqlite3.connect(db_path)
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


def save_data(post: Dict[Any, Any], db: sqlite3.Connection) -> None:
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
    c.execute(
        sql_insert_post,
        [post_id, text, json.dumps(photos), json.dumps(audios), json.dumps(videos)],
    )
    db.commit()


@app.command()
def run(url: str, n_posts: int = -1, db_path: str = "./cache.db") -> None:
    """Run full set of actions: getting posts and rendering html"""
    api = auth()
    page_id = url_to_domain(url)

    conn = initialize_table(db_path)

    for batch in get_posts(page_id, n_posts, api):
        for post in batch:
            data = process_post_json(post)
            save_data(data, conn)

    render_html(conn)

    conn.close()


@app.command()
def get(url: str, n_posts: int = -1, db_path: str = "./cache.db") -> None:
    """Download data data only (no files)"""
    pass

@app.command()
def clean(db_path: str = "./cache.db") -> None:
    """Clean the cache"""
    db = sqlite3.connect(db_path)
    c = db.cursor()
    sql_drop_table = "DROP TABLE IF EXISTS posts"
    c.execute(sql_drop_table)


@app.command()
def render(db_path: str = "./cache.db") -> None:
    """Render HTML with data from DB"""
    pass


if __name__ == "__main__":
    app()
