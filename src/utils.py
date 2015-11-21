import win32clipboard as w
import win32con


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


def get_text_from_clipboard():
    w.OpenClipboard()
    text = w.GetClipboardData(win32con.CF_TEXT)
    w.CloseClipboard()
    return text
