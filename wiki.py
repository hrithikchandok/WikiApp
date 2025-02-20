import requests
import json
import time
from collections import defaultdict, deque

url = "https://stream.wikimedia.org/v2/stream/revision-create"


domain_updates = defaultdict(set)

user_edit_counts = defaultdict(dict)

event_queue = deque()


def print_reports():
    """Prints reports for domains and user activity."""

    print("\n" + "=" * 50)
    print(f"Total number of Wikipedia Domains Updated: {len(domain_updates)}\n")

    sorted_domains = sorted(domain_updates.items(), key=lambda x: len(x[1]), reverse=True)
    for domain, pages in sorted_domains:
        print(f"{domain}: {len(pages)} pages updated")

    print("=" * 50 + "\n")

    for domain, users in user_edit_counts.items():
        if users:
            print(f"\nUsers who made changes to {domain}\n")
            sorted_users = sorted(users.items(), key=lambda x: x[1], reverse=True)
            for user, edit_count in sorted_users:
                print(f"{user}: {edit_count}")
            print("=" * 50 + "\n")


def clean_old_data():
    """Removes data older than 5 minutes from tracking dictionaries."""
    current_time = time.time()

    while event_queue:
        event_time, domain, page_title, user_name = event_queue[0]

        if current_time - event_time > 300:
            event_queue.popleft()
            domain_updates[domain].discard(page_title)

            if not domain_updates[domain]:
                del domain_updates[domain]

        else:
            break


def process_stream():
    """Fetches real-time Wikipedia revision data and updates the report every minute."""
    with requests.get(url, stream=True) as response:
        if response.status_code != 200:
            print(f"Error: {response.status_code}")
            return

        start_time = time.time()

        for line in response.iter_lines():
            if line:
                try:
                    line = line.decode('utf-8')
                    if line.startswith("data: "):
                        line = line[6:]

                    data = json.loads(line)  # Parse JSON


                    domain = data.get("meta", {}).get("domain")
                    page_title = data.get("page_title")
                    performer = data.get("performer", {})

                    user_name = performer.get("user_text")
                    user_is_bot = performer.get("user_is_bot", False)
                    user_edit_count = performer.get("user_edit_count", 0)

                    if domain and page_title:
                        current_time = time.time()
                        domain_updates[domain].add(page_title)
                        event_queue.append((current_time, domain, page_title, user_name))  # Track event


                        if user_name and not user_is_bot:
                            if user_name not in user_edit_counts[domain] or user_edit_counts[domain][
                                user_name] < user_edit_count:
                                user_edit_counts[domain][user_name] = user_edit_count


                    if time.time() - start_time >= 60:
                        clean_old_data()
                        print_reports()
                        start_time = time.time()

                except json.JSONDecodeError:
                    continue 

if __name__ == "__main__":
    process_stream()

