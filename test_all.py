import os
import shutil

from typer.testing import CliRunner

from exporter import app

runner = CliRunner()


def test_run_my_page():
    try:
        shutil.rmtree("./cache/ne_bknn")
    except FileNotFoundError:
        pass

    result = runner.invoke(app, ["run", "vk.com/ne_bknn"])

    assert result.exit_code == 0
    assert "[+] Auth successful" in result.stdout
    assert "posts.db" in os.listdir("cache/ne_bknn")


def test_clean_my_page_database():
    result = runner.invoke(app, ["clean", "vk.com/ne_bknn"])

    assert result.exit_code == 0
    assert "[+] Dropped posts database" in result.stdout


def test_clean_my_page_media():
    result = runner.invoke(app, ["clean", "vk.com/ne_bknn", "-f"])

    assert result.exit_code == 0
    assert "[+] Deleted downloaded media" in result.stdout
    assert "ne_bknn" not in os.listdir("cache")
