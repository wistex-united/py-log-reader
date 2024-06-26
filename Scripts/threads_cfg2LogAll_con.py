# This script is used to dump all representations provided in thread.cfg
# Yuhao Li modifed on Jun 25 2024
import json
import re


def convert_to_json(input_text):
    input_text = re.sub(r"^\s*//.*\n", "", input_text, flags=re.MULTILINE)

    # Step 1: Replace all ; with ,
    json_text = input_text.replace(";", ",")

    # Step 2: Replace all = with :
    json_text = json_text.replace("=", ":")

    # Step 3: Remove , before } or ]
    json_text = re.sub(r",(\s*[}\]])", r"\1", json_text)

    # Step 4: Add quotes around keys and string values
    json_text = re.sub(r"(\w+)\s*:", r'"\1":', json_text)
    json_text = re.sub(r":\s*(\w+)", r': "\1"', json_text)
    json_text = re.sub(r"\[\s*(\w+)", r'["\1"', json_text)
    json_text = re.sub(r",\s*(\w+)", r',"\1"', json_text)
    json_text = re.sub(r"(\w+)\s*:", r'"\1":', json_text)

    # Step 5: Fix specific keys that should not be quoted as string values
    json_text = re.sub(r":\s*\"(\d+)\"", r": \1", json_text)

    # Step 6: Handle special arrays
    json_text = re.sub(r'(\[\s*\n)(\s*")', r"\1", json_text)
    json_text = re.sub(r'(\s*"\])', r" \1", json_text)

    return json_text


def write_con(json_file):
    # load json
    with open(json_file, "r") as json_file, open("LogAll.con", "w") as con_file:
        con_file.write("dr annotation\n")
        con_file.write("dr timing\n")

        threads = json.load(json_file)
        for thread in threads["threads"]:
            name = thread["name"]
            reprs = thread["representationProviders"]
            for repr in reprs:
                # print(f"for {name} dr representation {repr['representation']}")
                con_file.write(
                    f"for {name} dr representation:{repr['representation']}\n"
                )


def main():
    with open("threads.cfg", "r") as f:
        input_text = f.read()
    output_text = convert_to_json("{" + input_text + "}")
    # print(output_text)
    with open("threads.json", "w") as f:
        f.write(output_text)

    write_con("threads.json")


if __name__ == "__main__":
    main()
