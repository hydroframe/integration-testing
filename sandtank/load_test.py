"""
    Run a load test of using sandtank simulate execution from a browser.
    This runs one test, but this can be called from a script starting
    several parallel process to simulate many users running sandtank.
"""
import sys
import time
from playwright.sync_api import Playwright, sync_playwright, expect

def run(job_num:int, playwright: Playwright, sandtank_url) -> None:
    """
    Execute a simple scenario of using standtank calling it from a browser
    and clicking to simulate a user runing sandtank.
    Parameters:
        job_num:    a job number to be printed it to know which job this is
        playwright: An open playright session used for simulating a browser
        sandtank_url: A url to the sandtank url to be tested.

    This uses the python playwright module to simulate using a browser.
    This allow testing of an application just as user would with clicking.
    """
    try:
        browser = playwright.chromium.launch(headless=False)
        context = browser.new_context()
        page = context.new_page()
        page.goto(sandtank_url)
        page.get_by_text("0").nth(3).click()
        page.locator("div:nth-child(2) > div:nth-child(3) > .v-btn").click()
        page.locator("div:nth-child(2) > div:nth-child(3) > .v-btn").dblclick()
        page.get_by_role("button", name="Run").click()
        time.sleep(10)
        page.locator("div:nth-child(9) > div:nth-child(3) > .v-btn").click()
        page.locator("div:nth-child(9) > div:nth-child(3) > .v-btn").dblclick()
        page.get_by_role("button", name="Run").click()
        time.sleep(10)
        print(f"Scenario with {job_num} users finished successfully.")
    except Exception as e:
        print(f"Exception raised in scenario #{job_num}")
    context.close()
    browser.close()

def main():
    """Main routine to start the load test simulating one user running sandtank."""
    
    job_num = sys.argv[1] if len(sys.argv) > 1 else 1
    sandtank_url = sys.argv[2] if len(sys.argv) > 2 else "https://sandtank-test.hydroframe.org"
    if not sandtank_url.endswith("/"):
        sandtank_url = sandtank_url + "/"
    with sync_playwright() as playwright:
        run(job_num, playwright, sandtank_url)


if __name__ == "__main__":
    main()
