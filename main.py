import os
import requests
import dataclasses
import inspect
from dotenv import load_dotenv
import argparse
import logging
from neo4j import GraphDatabase, Driver

load_dotenv()
arg_parser = argparse.ArgumentParser()
arg_parser.add_argument("--user_id", type=int, help="Fetch data for specified user_id")
arg_parser.add_argument(
    "--query",
    type=str,
    default=None,
    help="Run query over database",
    choices=["users", "groups", "top_5_users", "top_5_groups", "mutual", "all"],
)

logging.basicConfig(level=logging.INFO)


class FromDictMixin:
    @classmethod
    def from_dict(cls, payload: dict):
        return cls(
            **{
                k: v
                for k, v in payload.items()
                if k in inspect.signature(cls).parameters
            }
        )


@dataclasses.dataclass()
class User(FromDictMixin):
    id: int
    first_name: str
    last_name: str
    can_access_closed: bool
    is_closed: bool
    sex: int
    city: dict | None = None
    followers: list["User"] = dataclasses.field(default_factory=lambda: [])
    groups: list["Group"] = dataclasses.field(default_factory=lambda: [])


@dataclasses.dataclass()
class Group(FromDictMixin):
    id: int
    name: str


def make_request(
    token: str,
    method: str,
    payload: dict | None = None,
    allow_empty: bool = False,
) -> dict | None:
    formdata = dict(access_token=token, v="5.199")
    if payload:
        formdata.update(**payload)

    response = requests.post(f"https://api.vk.com/method/{method}", data=formdata)
    response.raise_for_status()
    data = response.json()

    if "error" in data.keys():
        logging.warning(f"{method} failed with:\n{data}")
        raise RuntimeError

    if not data["response"] and not allow_empty:
        logging.warning(
            f"Response was empty and not allowed to be empty. Method: {method}\nData:{data}",
        )
        raise RuntimeError("response was empty and not allowed to be empty")

    return data


def get_users(token: str, user_ids: list[int] | None = None) -> list[User]:
    data = make_request(
        token,
        "users.get",
        dict(user_ids=",".join([str(i) for i in user_ids]), fields="sex,city")
        if user_ids
        else dict(fields="sex,city"),
        allow_empty=True,
    )

    if not data:
        return None

    return [User.from_dict(item) for item in data["response"]]


def get_followers(token: str, user_id: int | None = None) -> list[int]:
    data = make_request(
        token,
        "users.getFollowers",
        dict(user_id=user_id) if user_id else None,
        allow_empty=True,
    )

    if data["response"]["count"] == 0:
        return None

    return data["response"]["items"]


def get_groups(token: str, user_id: int | None = None) -> list[Group]:
    data = make_request(
        token,
        "groups.get",
        dict(user_id=user_id, extended="1") if user_id else dict(extended="1"),
        allow_empty=True,
    )

    if not data:
        return None

    return [Group.from_dict(item) for item in data["response"]["items"]]


def fetch_recursive(
    token: str,
    user: User,
    max_depth: int = 2,
    depth: int = 0,
) -> User | None:
    logging.debug(f"Fetching for {user.first_name} {user.last_name} ({user.id})")
    if depth > max_depth:
        logging.debug(f"Reach end for {user.first_name} {user.last_name} ({user.id})")
        return None

    try:
        followers_ids = get_followers(token, user.id)
        followers = get_users(token, followers_ids)
        groups = get_groups(token, user.id)
    except Exception:
        logging.debug(
            f"Error on request for {user.first_name} {user.last_name} ({user.id})",
        )
        return None

    user.followers = followers
    user.groups = groups

    for follower in user.followers:
        fetch_recursive(token, follower, max_depth, depth + 1)

    return user


def create_user_node(tx, user: User):
    tx.run(
        """
        MERGE (u:User {id: $id})
        ON CREATE SET u.screen_name = $screen_name,
                      u.name = $name,
                      u.sex = $sex,
                      u.home_town = $home_town
        """,
        {
            "id": user.id,
            "screen_name": f"{user.first_name}_{user.last_name}",
            "name": f"{user.first_name} {user.last_name}",
            "sex": "Male" if user.sex == 2 else "Female",
            "home_town": user.city.get("title") if user.city else "",
        },
    )


def create_group_node(tx, group: Group):
    tx.run(
        """
        MERGE (g:Group {id: $group_id})
        ON CREATE SET g.name = $group_name
        """,
        {"group_id": group.id, "group_name": group.name},
    )


def connect_follower(tx, follower: User, followed: User):
    tx.run(
        """
        MATCH (follower:User), (followed:User)
        WHERE follower.id = $follower_id AND followed.id = $followed_id
        MERGE (follower)-[:Follow]->(followed)
        """,
        {"follower_id": follower.id, "followed_id": followed.id},
    )


def subscribe_to_group(tx, subscriber: User, group: Group):
    tx.run(
        """
        MATCH (subscriber:User), (group:Group)
        WHERE subscriber.id = $subscriber_id AND group.id = $group_id
        MERGE (subscriber)-[:Subscribe]->(group)
        """,
        {"subscriber_id": subscriber.id, "group_id": group.id},
    )


def process_user(tx, user: User) -> None:
    create_user_node(tx, user)

    for follower in user.followers:
        connect_follower(tx, follower, user)
        process_user(tx, follower)

    for group in user.groups:
        create_group_node(tx, group)
        subscribe_to_group(tx, user, group)


def write_data_to_neo4j(driver: Driver, user: User) -> None:
    with driver.session() as session:
        session.execute_write(process_user, user)


def get_all_users(session):
    result = session.execute_read(lambda tx: tx.run("MATCH (u:User) RETURN u").data())
    return [record["u"] for record in result]


def get_all_groups(session):
    result = session.execute_read(lambda tx: tx.run("MATCH (g:Group) RETURN g").data())
    return [record["g"] for record in result]


def get_top_5_users_by_followers_count(session):
    result = session.execute_read(
        lambda tx: tx.run("""
        MATCH (u:User)<-[f:Follow]-()
        WITH u, COUNT(f) AS followersCount
        RETURN u, followersCount
        ORDER BY followersCount DESC
        LIMIT 5
    """).data()
    )
    return [(record["u"], record["followersCount"]) for record in result]


def get_top_5_most_popular_groups(session):
    result = session.execute_read(
        lambda tx: tx.run("""
        MATCH (:User)-[s:Subscribe]->(g:Group)
        WITH g, COUNT(s) AS subscribersCount
        RETURN g, subscribersCount
        ORDER BY subscribersCount DESC
        LIMIT 5
    """).data()
    )
    return [(record["g"], record["subscribersCount"]) for record in result]


def get_mutual_followers(session):
    result = session.execute_read(
        lambda tx: tx.run("""
        MATCH (u1:User)-[:Follow]->(u2:User),
              (u2)-[:Follow]->(u1)
        RETURN u1, u2
    """).data()
    )
    return [(record["u1"], record["u2"]) for record in result]


def main(
    token: str,
    user_id: int,
    neo4j_uri: str,
    neo4j_user: str,
    neo4j_password: str,
) -> None:
    user = get_users(token, [user_id] if user_id else None)[0]

    if not user:
        raise RuntimeError("cant fetch user")

    logging.info("Run recursive fetch...")
    user = fetch_recursive(token, user, max_depth=2)
    driver = GraphDatabase.driver(neo4j_uri, auth=(neo4j_user, neo4j_password))

    try:
        write_data_to_neo4j(driver, user)
    finally:
        driver.close()


def run_queries(
    neo4j_uri: str,
    neo4j_user: str,
    neo4j_password: str,
    query: str,
) -> None:
    driver = GraphDatabase.driver(neo4j_uri, auth=(neo4j_user, neo4j_password))

    try:
        if query == "users" or query == "all":
            users = get_all_users(driver.session())
            print(f"Всего пользователей:{len(users)}")
        if query == "groups" or query == "all":
            groups = get_all_groups(driver.session())
            print(f"\nВсего групп:{len(groups)}")
        if query == "top_5_users" or query == "all":
            print("\nТоп 5 пользователей по количеству фолловеров:")
            top_users = get_top_5_users_by_followers_count(driver.session())
            for user, count in top_users:
                print(f"{user['name']} ({count})")
        if query == "top_5_groups" or query == "all":
            print("\nТоп 5 самых популярных групп:")
            popular_groups = get_top_5_most_popular_groups(driver.session())
            for group, count in popular_groups:
                print(f"{group['name']} ({count})")
        if query == "mutual" or query == "all":
            print("\nПользователи, которые фоловят друг друга:")
            mutual_followers = get_mutual_followers(driver.session())
            for user1, user2 in mutual_followers:
                print(f"{user1['name']} и {user2['name']}")
    finally:
        driver.close()


if __name__ == "__main__":
    token = os.environ.get("ACCESS_TOKEN", None)
    neo4j_uri = os.environ.get("NEO4J_URI", None)
    neo4j_user = os.environ.get("NEO4J_USER", None)
    neo4j_password = os.environ.get("NEO4J_PASSWORD", None)

    if not token:
        raise ValueError("no token")

    args = arg_parser.parse_args()
    user_id = args.user_id or None

    if args.query:
        run_queries(neo4j_uri, neo4j_user, neo4j_password, args.query)
    else:
        main(token, user_id, neo4j_uri, neo4j_user, neo4j_password)
