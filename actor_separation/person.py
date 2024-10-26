from .web import get_json_data

class Person:
    def __init__(self, url = None, name = None, movie_url = None, movie_name = None, role = None,
                 parent_person = None, degree = float("inf")):
        self.name = name
        self.url = url
        self.movie_url = movie_url
        self.movie_name = movie_name
        self.role = role
        self.parent_person= parent_person
        self.degree = degree


    def show_result(self):
        result_list = []
        print(f"\nDegrees of Separation: {self.degree}\n")

        actor = self
        while actor is not None:
            result_list.append(actor)
            actor = actor.parent_person
        result_list.reverse()

        for i in range(len(result_list)):
            actor = result_list[i]
            if actor.parent_person is None:
                continue
            if actor.name is None:
                actor_data = get_json_data(actor.url)
                if actor_data is None:
                    return False
                actor.name = actor_data['name']
            print(f"{i}. Movie: {actor.movie_name}")
            print(f"{actor.parent_person.role}: {actor.parent_person.name}")
            print(f"{actor.role}: {actor.name}")
            print("\n")