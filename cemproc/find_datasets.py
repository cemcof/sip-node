import argparse
import pathlib
import re
import sys
from pathlib import Path
import yaml

# Das|Demo|Guerra|Hassan|Kabanov|Soares|Zafar
# /storage/brno14-ceitec/shared/cemcof/internal/DATA
def extract_data_old(filepath: pathlib.Path):
    with filepath.open('r') as f:
        data = yaml.safe_load(f)
        try:
            op, user = data["Operator"]["Fullname"], data['User']['Fullname']
            tech = data.get('Technique', '-')
            dt = data['DtCreated']
            return op, user, tech, dt, filepath.parent
        except Exception as e:
            print(f"Error reading {filepath} {e}", file=sys.stderr)
            print(data, file=sys.stderr)

def search_yaml(directory, regex_pattern):
    pattern = re.compile(regex_pattern)

    yamls = {
        'SPA.yml': extract_data_old,
        'TOMO.yml': extract_data_old,
        'TRANSFER.yml': extract_data_old
    }

    for yaml_filename in yamls:
        for yaml_path in Path(directory).glob("*/" + yaml_filename):
            try:
                if pattern.search(yaml_path.read_text()):
                    operator, user, type, datetime, path = yamls[yaml_filename](yaml_path)
                    # Print as csv row
                    print(f"{operator};{user};{type};{datetime};{path}")
            except Exception as e:
                print(f"Error reading {yaml_path}: {e}", file=sys.stderr)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Search for a regex pattern in YAML files within a directory.")
    parser.add_argument("directory", help="Directory to search")
    parser.add_argument("regex", help="Regular expression to search for")

    args = parser.parse_args()

    search_yaml(args.directory, args.regex)
