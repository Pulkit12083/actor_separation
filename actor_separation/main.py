import argparse
from .graph import find_degrees_of_separation
from .web import check_valid_url

def main():
    parser = argparse.ArgumentParser(description="Find degrees of separation between people")
    parser.add_argument("person1_url", help="URL of the first person")
    parser.add_argument("person2_url", help="URL of the second person")
    args = parser.parse_args()

    if not check_valid_url(args.person1_url):
        print("Invalid URL for Person-1")
        return

    if not check_valid_url(args.person2_url):
        print("Invalid URL for Person-2")
        return

    find_degrees_of_separation(args.person1_url, args.person2_url)


if __name__ == '__main__':
    main()
