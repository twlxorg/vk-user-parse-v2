import os
import requests
import dataclasses
import inspect
from dotenv import load_dotenv
import argparse
import json

load_dotenv()
arg_parser = argparse.ArgumentParser()
arg_parser.add_argument("--user_id", type=int, help="Fetch data for specified user_id")
arg_parser.add_argument("--output", type=str, help="Output file path")


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


@dataclasses.dataclass(frozen=True)
class User(FromDictMixin):
    id: int
    first_name: str
    last_name: str
    can_access_closed: bool
    is_closed: bool


@dataclasses.dataclass(frozen=True)
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
        print(f"{method} failed with:")
        print(data)
        raise RuntimeError

    if not data["response"] and not allow_empty:
        raise RuntimeError("response was empty and not allowed to be empty")

    return data


def get_users(token: str, user_ids: list[int] | None = None) -> list[User] | None:
    data = make_request(
        token,
        "users.get",
        dict(user_ids=",".join([str(i) for i in user_ids])) if user_ids else None,
        allow_empty=True,
    )

    if not data:
        return None

    return [User.from_dict(item) for item in data["response"]]


def get_followers(token: str, user_id: int | None = None) -> list[int] | None:
    data = make_request(
        token,
        "users.getFollowers",
        dict(user_id=user_id) if user_id else None,
        allow_empty=True,
    )

    if data["response"]["count"] == 0:
        return None

    return data["response"]["items"]


def get_groups(token: str, user_id: int | None = None) -> None:
    data = make_request(
        token,
        "groups.get",
        dict(user_id=user_id, extended="1") if user_id else dict(extended="1"),
        allow_empty=True,
    )

    if not data:
        return None

    return [Group.from_dict(item) for item in data["response"]["items"]]


def generate_report(user: User, followers: list[User], groups: list[Group]) -> dict:
    report = {}

    report["user_id"] = user.id
    report["username"] = f"{user.first_name} {user.last_name}"
    report["account_status"] = "Private" if user.is_closed else "Public"
    report["followers"] = []
    report["followers_count"] = 0
    report["groups"] = []
    report["groups_count"] = 0

    # Информация о подписчиках
    if followers:
        report["followers_count"] = len(followers)
        for follower in followers:
            status = "Private" if follower.is_closed else "Public"
            report["followers"].append(
                {
                    "user_id": follower.id,
                    "username": f"{follower.first_name} {follower.last_name}",
                    "account_status": status,
                }
            )
    if groups:
        report["groups_count"] = len(groups)
        for group in groups:
            report["groups"].append({"id": group.id, "name": group.name})

    return report


def main(token: str, user_id: int, output: str) -> None:
    user = get_users(token, [user_id] if user_id else None)[0]
    followers_ids = get_followers(token, user_id)
    followers = get_users(token, followers_ids)
    groups = get_groups(token, user_id)

    report = generate_report(user, followers, groups)
    with open(output, "w", encoding="utf-8") as f:
        f.write(json.dumps(report, indent=4, ensure_ascii=False))


if __name__ == "__main__":
    token = os.environ.get("ACCESS_TOKEN", None)

    if not token:
        raise ValueError("no token")

    args = arg_parser.parse_args()
    user_id = args.user_id or None
    output = args.output or "report.json"

    main(token, user_id, output)
    print("Report was saved to:", output)
