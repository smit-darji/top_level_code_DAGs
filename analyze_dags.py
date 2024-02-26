import ast
import os

def check_top_level_code(directory):
    valid_files = []
    invalid_files = []
    for root, dirs, files in os.walk(directory):
        for filename in files:
            if filename.endswith(".py"):
                file_path = os.path.join(root, filename)
                with open(file_path, "r") as file:
                    try:
                        tree = ast.parse(file.read(), filename=file_path)
                        has_top_level_code = any(isinstance(node, (ast.FunctionDef, ast.ClassDef)) for node in tree.body)
                        if has_top_level_code:
                            invalid_files.append(filename)
                        else:
                            valid_files.append(filename)
                    except SyntaxError:
                        invalid_files.append(filename)
                        print(f"SyntaxError: Unable to parse {filename}")
    return valid_files, invalid_files


# Example usage:
directory = "dags"
valid_files, invalid_files = check_top_level_code(directory)

# Print valid and invalid filenames
print("Valid files with-out top-level code:")
for file in valid_files:
    print(file)

print("\nInvalid files top-level code::")
for file in invalid_files:
    print(file)
