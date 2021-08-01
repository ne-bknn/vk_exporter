import vk_api as vk  # type: ignore
from getpass import getpass
import typer
import sqlite3
import sys
import re

DEBUG = True
app = typer.Typer()


if DEBUG:
    from icecream import ic  # type: ignore

    import IPython  # type: ignore

    repl = IPython.embed
else:
    ic = lambda *args: None
    repl = lambda *args: None

# output helpers
class LLog:
    @classmethod
    def info(cls, s):
        typer.secho(f"[.] {s}")

    @classmethod
    def success(cls, s):
        typer.secho(f"[+] {s}", fg=typer.colors.GREEN)

    @classmethod
    def err(cls, s):
        typer.secho(f"[-] {s}", fg=typer.colors.RED)


llog = LLog


def auth() -> vk.vk_api.VkApiMethod:
    """Interactively authenticates user and returns api object"""

    def mfa_handler():
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


def get_posts(page_id: str, n_posts: int, api: vk.vk_api.VkApiMethod) -> list:
    total_posts = api.wall.get(domain=page_id, count=1, offset=0)["count"]

    n_posts = min(n_posts, total_posts) if n_posts != -1 else total_posts
    ic(n_posts)

    step = 100
    offset = 0

    while n_posts != 0:
        step_amount = min(step, n_posts)
        ic(step_amount)

        yield api.wall.get(domain=page_id, count=step_amount, offset=offset * step)['items']

        n_posts -= step_amount
        offset += 1

def process_post_json(post: dict) -> dict:
    print(post)
    print("=================================")
    return {}


def url_to_domain(url: str, api: vk.vk_api.VkApiMethod) -> str:
    screenname_re = re.compile("^[a-zA-Z0-9_]{4,100}$")
    screenname = url.split("/")[-1]
    if not screenname_re.match(screenname):
        llog.err('Something wrong with "{screenname}" name')
        sys.exit()

    return screenname

def domain_to_id(domain: str, api: vk.vk_api.VkApiMethod) -> str:
    data = api.utils.resolveScreenName(screen_name=domain)["response"]
    obj_type = data["type"]
    obj_id = data["object_id"]

    if obj_type == "group":
        obj_id = -obj_id

    return obj_id


def render_html(db) -> str:
    pass


def save_data(post: dict, db):
    pass

@app.command()
def run(url: str, n_posts: int = -1, db: str = "./cache.db"):
    """Run full set of actions: getting posts and rendering html"""
    api = auth()
    page_id = url_to_domain(url, api)
    conn = None

    for batch in get_posts(page_id, n_posts, api):
        #print(batch)
        for post in batch:
            data = process_post_json(post)
            save_data(data, conn)
    
    render_html(conn)


@app.command()
def get(url: str, n_posts: int = -1, db: str = "./cache.db"):
    """Download data data only (no files)"""
    pass

@app.command()
def render(db: str = "./cache.db"):
    """Render HTML with data from DB"""
    pass

if __name__ == "__main__":
    app()
