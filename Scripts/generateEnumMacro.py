def generateEnumMacros(end):
    start = 1
    macros = []

    # Generate ENUM_TUPLE_SIZE_II macro
    macroSizeII = (
        "#define _ENUM_TUPLE_SIZE_II( \\\n  "
        + ", ".join(["a0"] + [f"a{i}" for i in range(start, end + 1)])
        + ",...) a{}\n".format(end)
    )
    macros.append(macroSizeII)

    # Generate ENUM_TUPLE_SIZE_III macro
    macroSizeIII = (
        "#define _ENUM_TUPLE_SIZE_III \\\n  "
        + ", ".join([str(i) for i in range(end, start - 1, -1)])
        + "\n"
    )
    macros.append(macroSizeIII)

    # Generate ENUM_REMOVELAST_n macros
    for n in range(1, end + 1):
        if n == 1:
            macros.append(f"#define _ENUM_REMOVELAST_{n}(a1)\n")
        else:
            params = ", ".join([f"a{i}" for i in range(1, n + 1)])
            removedLast = ", ".join([f"a{i}" for i in range(1, n)])
            macros.append(f"#define _ENUM_REMOVELAST_{n}({params}) {removedLast}\n")

    result= "".join(macros)
    with open(f"EnumMacros{end}.h", "w") as f:
        f.write(result)


if __name__ == "__main__":
    macroCode = generateEnumMacros(400)
    print(macroCode)
