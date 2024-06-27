# This script is used to dump all representations provided in thread.cfg
# Yuhao Li modified on Jun 25 2024
import json
import re

def convertToJson(inputText):
    inputText = re.sub(r"^\s*//.*\n", "", inputText, flags=re.MULTILINE)

    # Step 1: Replace all ; with ,
    jsonText = inputText.replace(";", ",")

    # Step 2: Replace all = with :
    jsonText = jsonText.replace("=", ":")

    # Step 3: Remove , before } or ]
    jsonText = re.sub(r",(\s*[}\]])", r"\1", jsonText)

    # Step 4: Add quotes around keys and string values
    jsonText = re.sub(r"(\w+)\s*:", r'"\1":', jsonText)
    jsonText = re.sub(r":\s*(\w+)", r': "\1"', jsonText)
    jsonText = re.sub(r"\[\s*(\w+)", r'["\1"', jsonText)
    jsonText = re.sub(r",\s*(\w+)", r',"\1"', jsonText)
    jsonText = re.sub(r"(\w+)\s*:", r'"\1":', jsonText)

    # Step 5: Fix specific keys that should not be quoted as string values
    jsonText = re.sub(r":\s*\"(\d+)\"", r": \1", jsonText)

    # Step 6: Handle special arrays
    jsonText = re.sub(r'(\[\s*\n)(\s*")', r"\1", jsonText)
    jsonText = re.sub(r'(\s*"\])', r" \1", jsonText)

    return jsonText

def writeCon(jsonFile):
    # load json
    with open(jsonFile, "r") as json_file, open("LogAll.con", "w") as con_file:
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
        inputText = f.read()
    outputText = convertToJson("{" + inputText + "}")
    # print(outputText)
    with open("threads.json", "w") as f:
        f.write(outputText)

    writeCon("threads.json")

if __name__ == "__main__":
    main()
