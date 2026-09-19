from week6_common import ask_gemini, get_genai_client


def main():
    client = get_genai_client()

    # Standalone prompt (prompt without extra context)
    question = "What are the benefits of remote work?"
    response = ask_gemini(client, prompt=question)
    print(response)

    # Workshop extension: to experiment with a System Instruction, use:
    #
    # response = ask_gemini(
    #     client,
    #     prompt=question,
    #     system_instruction=[
    #         "You are a bad manager.",
    #         "Your mission is to get people work in the office.",
    #     ],
    # )
    # print(response)


if __name__ == "__main__":
    main()
