import asyncio
import aiohttp
from collections import deque

class CrawlerNode:
    def __init__(self, url_queue, page_store):
        self.url_queue = url_queue
        self.page_store = page_store
        self.task_queue = asyncio.Queue()
        self.worker_tasks = []

    async def run(self):
        await asyncio.gather(
            self.worker_loop(),
            self.feed_queue()
        )

    async def worker_loop(self):
        while True:
            url = await self.task_queue.get()
            try:
                page = await self.fetch_page(url)
                self.page_store.add_page(page)
                self.url_queue.add_urls(page.outlinks)
            except Exception as e:
                print(f'Error fetching {url}: {e}')
            finally:
                self.task_queue.task_done()

    async def feed_queue(self):
        while True:
            url = await self.url_queue.get_url()
            await self.task_queue.put(url)

    async def fetch_page(self, url):
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                content = await response.text()
                return Page(url, content)

class Page:
    def __init__(self, url, content):
        self.url = url
        self.content = content
        self.outlinks = self.extract_outlinks(content)

    def extract_outlinks(self, content):
        # Implement logic to extract outlinks from page content
        return ['https://example.com/link1', 'https://example.com/link2']
