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
    print("Valid files are: ", valid_files)
    print("invalic files are: ", invalid_files)
    return valid_files, invalid_files
