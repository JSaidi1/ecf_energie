from pathlib import Path

def read_key_value(path: str | Path) -> dict[str, float | str]:
    """
    Reads a file 'key=value'. Convert to float if possible, otherwise keep the value as a string.

    Ignore empty lines and those that begin with #.
    """
    path = Path(path)
    data: dict[str, float | str] = {}

    def try_float(value: str):
        v = value.replace(",", ".").strip()
        try:
            return float(v)
        except ValueError:
            return value.strip()

    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if "=" not in line:
            continue

        key, value = line.split("=", 1)
        data[key.strip()] = try_float(value)

    return data
