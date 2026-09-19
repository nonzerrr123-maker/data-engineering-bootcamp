from week6_common import ask_gemini, get_genai_client


CONTEXT = """
Skooldio offers a full refund within 14 days of booking, provided no services have been consumed.
After 14 days, a 50% refund is possible upon review. Skooldio has a remote work policy but it has to
review on a case by case basis. Let's say if no meeting on that day, the employee can work from home.
"""


def main():
    client = get_genai_client()
    question = "What are the benefits of remote work?"
    prompt_with_context = f"""
You are a helpful assistant. Use the following context to answer the question.

Context:
{CONTEXT}

Question:
{question}
"""
    print(ask_gemini(client, prompt=prompt_with_context))


if __name__ == "__main__":
    main()
