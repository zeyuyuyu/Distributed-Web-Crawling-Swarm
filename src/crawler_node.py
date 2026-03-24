import zmq
import time
from typing import Dict, List, Optional
from dataclasses import dataclass
from urllib.parse import urlparse

@dataclass
class CrawlTask:
    url: str
    depth: int
    domain: str
    visited: List[str]

class CrawlerNode:
    def __init__(self, node_id: str, coordinator_address: str):
        self.node_id = node_id
        self.context = zmq.Context()
        self.task_socket = self.context.socket(zmq.DEALER)
        self.task_socket.setsockopt_string(zmq.IDENTITY, node_id)
        self.task_socket.connect(coordinator_address)
        
        self.result_socket = self.context.socket(zmq.PUSH)
        self.result_socket.connect(coordinator_address.replace('5555', '5556'))
        
        self.active_tasks: Dict[str, CrawlTask] = {}
        self.is_running = False

    def start(self):
        self.is_running = True
        self.register_with_coordinator()
        
        while self.is_running:
            try:
                if self.task_socket.poll(timeout=1000):
                    msg = self.task_socket.recv_json()
                    self.handle_message(msg)
            except Exception as e:
                print(f"Error in node {self.node_id}: {str(e)}")

    def register_with_coordinator(self):
        msg = {
            'type': 'register',
            'node_id': self.node_id,
            'capacity': 10  # Max parallel tasks
        }
        self.task_socket.send_json(msg)

    def handle_message(self, msg: Dict):
        msg_type = msg.get('type')
        
        if msg_type == 'crawl_task':
            task = CrawlTask(
                url=msg['url'],
                depth=msg['depth'],
                domain=urlparse(msg['url']).netloc,
                visited=msg.get('visited', [])
            )
            self.process_task(task)
        
        elif msg_type == 'stop':
            self.is_running = False

    def process_task(self, task: CrawlTask):
        # Simulate crawling
        time.sleep(1)
        
        result = {
            'type': 'crawl_result',
            'node_id': self.node_id,
            'url': task.url,
            'links': [f'http://{task.domain}/page{i}' for i in range(3)],
            'status': 'success'
        }
        
        self.result_socket.send_json(result)

    def stop(self):
        self.is_running = False
        self.task_socket.close()
        self.result_socket.close()
        self.context.term()

if __name__ == '__main__':
    import sys
    node = CrawlerNode(
        node_id=sys.argv[1] if len(sys.argv) > 1 else f'node_{time.time()}',
        coordinator_address='tcp://localhost:5555'
    )
    try:
        node.start()
    except KeyboardInterrupt:
        node.stop()