import os
import ast
from tabulate import tabulate
import sys


def check_top_level_code(file_path):
    results = []
    try:
        with open(file_path, 'r') as file:
            code = file.read()
            tree = ast.parse(code)

            # Initialize variables to track line number and top-level code presence
            top_level_line = None
            top_level_found = False

            for node in tree.body:
                # Ignore default_args dictionary
                if isinstance(node, ast.Assign) and len(node.targets) == 1 and isinstance(node.targets[0], ast.Name) and node.targets[0].id == 'default_args':
                    continue

                # Ignore DAG creation with specified default_args
                if (isinstance(node, ast.Assign)
                        and len(node.targets) == 1
                        and isinstance(node.targets[0], ast.Name)
                        and node.targets[0].id == 'dag'
                        and isinstance(node.value, ast.Call)
                        and isinstance(node.value.func, ast.Name)
                        and node.value.func.id == 'DAG'
                        and any(isinstance(kw, ast.keyword) and kw.arg == 'default_args' for kw in node.value.keywords)):
                    continue

                if not top_level_found and not isinstance(node, (ast.FunctionDef, ast.ClassDef, ast.Import, ast.ImportFrom, ast.With)):
                    if not (isinstance(node, ast.Expr)
                            and isinstance(node.value, ast.Call)
                            and isinstance(node.value.func, ast.Name)
                            and node.value.func.id == "task"):
                        # Record the line number of the first top-level code found
                        top_level_line = node.lineno
                        top_level_found = True

            # Append the result to the list
            results.append((os.path.basename(file_path), "Yes" if top_level_found else "No", top_level_line))
    except Exception as e:
        print("Error:", e)
    return results




if __name__ == "__main__":
    files = ["dags/dag1.py", "dags/fail.py"]
    
    # Check top-level code in each script
    results = []
    for file_path in files:
        results.extend(check_top_level_code(file_path))

    # Print results as a table
    print(tabulate(results, headers=["File Name", "Top-Level Code", "Line Number"], tablefmt="grid"))
