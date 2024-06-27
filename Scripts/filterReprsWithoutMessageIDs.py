import json
import re


def filterRepresentationTerms(file_path):
    representationTerms = []

    # Read the file
    with open(file_path, "r") as file:
        lines = file.readlines()
        lines = lines[0].split(" ")
        lines = [re.sub(r"//.*", "", line).strip() for line in lines]

    # Regular expression to find terms that start with "representation:"
    representation_pattern = re.compile(r"^representation:(\w+)")

    # Iterate over each line and search for the pattern
    for line in lines:
        match = representation_pattern.search(line)
        if match:
            representationTerms.append(match.group(1))

    return representationTerms


def filderMessageIDs(filePath):
    capture = False

    # Read the file
    with open(filePath, "r") as file:
        lines = file.readlines()

    lines = [re.sub(r"//.*", "", line).strip() for line in lines]
    # Regular expression to find the header ENUM(MessageID, {
    enumHeaderPattern = re.compile(r"ENUM\(MessageID,\s*{\s*")
    enumEndPattern = re.compile(r"}\s*;\s*")

    idPattern = re.compile(r"\bid(\w*)")

    # Iterate over each line to find the enum block

    idTerms = []
    if len(lines) < 2:
        return []
    for i in range(len(lines) - 1):
        line1 = lines[i]
        line2 = lines[i + 1]

        line = line1 + line2
        if capture:
            if enumEndPattern.search(line):
                break
            idMatch = idPattern.search(line2)
            if idMatch and not idMatch.group(1).startswith("NumOf"):
                idTerms.append(idMatch.group(1))
        elif enumHeaderPattern.search(line):
            capture = True

    return idTerms


def main():
    # filePath = "dr_output.txt"  # Update this with the path to your file
    # representationTerms = filterRepresentationTerms(filePath)
    with open("threads.json", "r") as file:
        conf = json.load(file)

    representationTerms = set()
    for thread in conf["threads"]:
        for repr in thread["representationProviders"]:
            representationTerms.add(repr["representation"])
    # print(f"Filtered representation terms ({len(representationTerms)}):")
    # for term in representationTerms:
    #     print(term)
    filePath = "MessageIDs.h"
    idTerms = filderMessageIDs(filePath)
    # print(f"Filtered enum lines ({len(enum_lines)}):")
    # for line in enum_lines:
    #     print(line)

    result = set(representationTerms)
    result -= set(idTerms)
    result = sorted(list(result))

    result = [f"  id{term}," for term in result]
    # for term in result:
    #     print(term)
    with open("MessageIDsMore.txt", "w") as file:
        file.write("\n".join(result))


if __name__ == "__main__":
    main()
