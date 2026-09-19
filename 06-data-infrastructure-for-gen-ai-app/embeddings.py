from week6_common import get_embedding, get_genai_client


def main():
    client = get_genai_client()

    pairs = [
        (
            "Remote work allows employees to be more flexible and productive.",
            "Work from home is very productive for me",
        ),
        ("Hello", "Hey"),
    ]

    for left, right in pairs:
        vec_left = get_embedding(client, left).values
        vec_right = get_embedding(client, right).values
        print(f"{left!r}: dimension={len(vec_left)}, first_values={vec_left[:8]}")
        print(f"{right!r}: dimension={len(vec_right)}, first_values={vec_right[:8]}")


if __name__ == "__main__":
    main()
