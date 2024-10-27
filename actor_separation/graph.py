import time
from collections import deque
from multiprocessing import Pool
from .web import get_json_data, check_valid_url
from .person import Person

def find_degrees_of_separation(person1_url, person2_url):

    #Initialize data structures
    pool = Pool(processes=4)
    visited_actors = set()
    visited_movies = set()
    enqueued_actors = set()
    que = deque()
    person_zero = Person(url=person1_url, degree=0)
    que.append(person_zero)

    #Start BFS (Breadth first search)
    while que:
        current_person = que.popleft()
        if current_person.url == person2_url:
            current_person.show_result()
            return

        #check if visited
        if current_person.url in visited_actors:
            continue
        visited_actors.add(current_person.url)

        #fetch all movies of this actor
        async_result = pool.apply_async(get_json_data, [current_person.url])
        current_person_data = async_result.get(timeout=100)

        #current_person_data = get_json_data(current_person.url)
        if current_person_data is None:
            continue
        current_person.name= current_person_data['name']
        movies = current_person_data['movies']

        #fetch connected people from each movie
        cast_members = []
        #collect movie urls
        for movie in movies:
            if movie is None or movie['url'] in visited_movies:
                continue
            visited_movies.add(movie['url'])
            current_movie_data = [movie['name'], movie['role'], movie['url'], []]
            result = pool.apply_async(get_json_data, [movie['url']])
            movie_data = result.get(timeout=100)
            #movie_data = get_json_data(movie['url'])
            if movie_data is None:
                continue
            current_movie_data[3] += movie_data['cast'] + movie_data['crew']
            cast_members.append(current_movie_data)

        for movie_name, movie_role, movie_url, cast in cast_members:
            for connected_person in cast:
                actor_url = connected_person['url']
                if actor_url in visited_actors or actor_url in enqueued_actors:
                    continue
                current_person.role = movie_role
                actor = Person(url = actor_url,
                              name = None,
                              movie_url=movie_url,
                              movie_name=movie_name,
                              role = connected_person['role'],
                              parent_person = current_person,
                              degree = current_person.degree + 1
                              )
                if actor.url == person2_url:
                    actor.show_result()
                    return
                que.append(actor)
                enqueued_actors.add(actor_url)