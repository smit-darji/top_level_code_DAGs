import ast
import os

def check_top_level_code(directory):
    files_with_top_level_code = []
    for root, dirs, files in os.walk(directory):
        for filename in files:
            if filename.endswith(".py"):
                file_path = os.path.join(root, filename)
                with open(file_path, "r") as file:
                    try:
                        tree = ast.parse(file.read(), filename=file_path)
                        for node in tree.body:
                            if isinstance(node, (ast.FunctionDef, ast.ClassDef)):
                                files_with_top_level_code.append(filename)
                                break
                    except SyntaxError:
                        print(f"SyntaxError: Unable to parse {filename}")
    return files_with_top_level_code


# Example usage:
directory = "dags"
files_with_top_level_code = check_top_level_code(directory)

# Print each filename on a new line
for file in files_with_top_level_code:
    print(file)
