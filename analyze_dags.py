import ast
import sys

def check_top_level_code(files):
    valid_files = []
    invalid_files = []
    for file_path in files:
        try:
            with open(file_path, "r") as file:
                tree = ast.parse(file.read(), filename=file_path)
                has_top_level_code = any(isinstance(node, (ast.FunctionDef, ast.ClassDef)) for node in tree.body)
                if has_top_level_code:
                    invalid_files.append(file_path)
                else:
                    valid_files.append(file_path)
        except SyntaxError:
            invalid_files.append(file_path)
            print(f"SyntaxError: Unable to parse {file_path}")
    return valid_files, invalid_files

def test(files):
    valid_files = []
    invalid_files = []
    for file_path in files:
            with open(file_path, "r") as file:
                tree = ast.parse(file.read(), filename=file_path)
                has_top_level_code = any(isinstance(node, (ast.FunctionDef, ast.ClassDef)) for node in tree.body)
                if has_top_level_code:
                    invalid_files.append(file_path)
                else:
                    valid_files.append(file_path)
    return valid_files, invalid_files

# Example usage:
if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python analyze_dags.py")
        sys.exit(1)

    files = sys.argv[1:]
    valid_files, invalid_files = check_top_level_code(files)

    # Print valid and invalid filenames
    print("Valid files without top-level code:")
    for file in valid_files:
        print(file)

    print("\nInvalid files with top-level code:")
    for file in invalid_files:
        print(file)
