import time
from collections import deque
from multiprocessing import Pool
from concurrent.futures import ThreadPoolExecutor
import asyncio
from functools import partial
from typing import Dict, Set, List, Optional, Tuple
from .web import get_json_data
from .person import Person


class ActorGraphSearch:
    def __init__(self, pool_size: int = 16):
        self.pool_size = pool_size
        self.visited_actors: Set[str] = set()
        self.visited_movies: Set[str] = set()
        self.enqueued_actors: Set[str] = set()
        self.process_pool: Pool = None
        self.thread_pool: ThreadPoolExecutor = None

    async def fetch_batch_data(self, urls: List[str]) -> List[Optional[Dict]]:
        """Fetch multiple URLs concurrently using the process pool."""
        if not urls:
            return []

        loop = asyncio.get_event_loop()
        fetch_tasks = []
        for url in urls:
            fetch_tasks.append(
                loop.run_in_executor(
                    self.thread_pool,
                    partial(self.process_pool.apply_async, get_json_data, (url,))
                )
            )
        results = await asyncio.gather(*fetch_tasks)
        return [result.get(timeout=100) if result else None for result in results]

    async def process_person_batch(self, batch: List[Tuple[Person, str]]) -> List[Person]:
        """Process a batch of persons and their target URLs concurrently."""
        person_urls = [person.url for person, _ in batch]
        person_data_list = await self.fetch_batch_data(person_urls)

        new_actors = []
        movie_urls = set()

        # First pass: collect all movie URLs
        for (current_person, target_url), person_data in zip(batch, person_data_list):
            if not person_data:
                continue

            current_person.name = person_data['name']
            for movie in person_data.get('movies', []):
                if movie and movie['url'] not in self.visited_movies:
                    movie_urls.add(movie['url'])

        # Fetch all movie data in parallel
        movie_data_list = await self.fetch_batch_data(list(movie_urls))
        movie_data_map = {url: data for url, data in zip(movie_urls, movie_data_list)}

        # Second pass: process movies and create new actors
        for (current_person, target_url), person_data in zip(batch, person_data_list):
            if not person_data:
                continue

            for movie in person_data.get('movies', []):
                if not movie or movie['url'] in self.visited_movies:
                    continue

                movie_data = movie_data_map.get(movie['url'])
                if not movie_data:
                    continue

                self.visited_movies.add(movie['url'])
                cast = movie_data['cast'] + movie_data['crew']

                for connected_person in cast:
                    actor_url = connected_person['url']
                    if (actor_url in self.visited_actors or
                            actor_url in self.enqueued_actors):
                        continue

                    current_person.role = movie['role']
                    actor = Person(
                        url=actor_url,
                        name=None,
                        movie_url=movie['url'],
                        movie_name=movie['name'],
                        role=connected_person['role'],
                        parent_person=current_person,
                        degree=current_person.degree + 1
                    )

                    if actor_url == target_url:
                        return [actor]

                    new_actors.append(actor)
                    self.enqueued_actors.add(actor_url)

        return new_actors

    async def find_degrees_of_separation(self, person1_url: str, person2_url: str) -> float:
        """Find the degrees of separation between two actors."""
        BATCH_SIZE = 5  # Adjust based on your API limits and system capabilities

        self.process_pool = Pool(processes=self.pool_size)
        self.thread_pool = ThreadPoolExecutor(max_workers=self.pool_size * 2)

        try:
            que = deque([(Person(url=person1_url, degree=0), person2_url)])
            current_batch = []

            while que:
                while len(current_batch) < BATCH_SIZE and que:
                    current_batch.append(que.popleft())

                if not current_batch:
                    break

                # Process current batch
                new_actors = await self.process_person_batch(current_batch)

                # If target found
                if new_actors and new_actors[0].url == person2_url:
                    new_actors[0].show_result()
                    return new_actors[0].degree

                # Add new actors to queue
                que.extend((actor, person2_url) for actor in new_actors)

                # Mark current batch as visited
                for person, _ in current_batch:
                    self.visited_actors.add(person.url)

                current_batch = []

            return float("infinity")

        finally:
            self.process_pool.close()
            self.process_pool.join()
            self.thread_pool.shutdown()


def find_degrees_of_separation(person1_url: str, person2_url: str) -> float:
    """Entry point function that sets up and runs the async search."""
    if person1_url == person2_url:
        return 0
    searcher = ActorGraphSearch()
    return asyncio.run(searcher.find_degrees_of_separation(person1_url, person2_url))