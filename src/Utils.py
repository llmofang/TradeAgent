def read_commands(filename):
    lines = []
    f = open(filename)
    try:
        fileline = f.readlines()
        for line in fileline:
            line = line.rstrip('\n')
            lines.append(list(line.split(' ')))
    finally:
        f.close()
    return lines

