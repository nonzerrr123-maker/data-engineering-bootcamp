import numpy as np

from week6_common import get_embedding, get_genai_client


def cosine_similarity(vec1, vec2):
    vec1, vec2 = np.array(vec1), np.array(vec2)
    return float(np.dot(vec1, vec2) / (np.linalg.norm(vec1) * np.linalg.norm(vec2)))


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
        print(f"Similarity score ({left!r} vs {right!r}): {cosine_similarity(vec_left, vec_right):.6f}")


if __name__ == "__main__":
    main()
