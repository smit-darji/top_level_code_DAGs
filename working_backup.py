import os
import ast
from tabulate import tabulate


def check_top_level_code(folder_path):
    results = []
    try:
        for filename in os.listdir(folder_path):
            script_path = os.path.join(folder_path, filename)
            if os.path.isfile(script_path):
                with open(script_path, 'r') as file:
                    code = file.read()
                    tree = ast.parse(code)

                    # Initialize variables to track line number and top-level code presence
                    top_level_line = None
                    top_level_found = False

                    for node in tree.body:
                        if not top_level_found and not isinstance(node, (ast.FunctionDef, ast.ClassDef, ast.Import, ast.ImportFrom, ast.With)):
                            if not (isinstance(node, ast.Expr)
                                    and isinstance(node.value, ast.Call)
                                    and isinstance(node.value.func, ast.Name)
                                    and node.value.func.id == "task"):
                                # Record the line number of the first top-level code found
                                top_level_line = node.lineno
                                top_level_found = True

                    # Append the result to the list
                    results.append((filename, "Yes" if top_level_found else "No", top_level_line))
    except Exception as e:
        print("Error:", e)
    return results


def main():
    # Provide the full path to the DAG folder
    dag_folder_path = "dags"

    if not os.path.isdir(dag_folder_path):
        print("Invalid folder path.")
        return

    # Check top-level code in each script in the folder
    results = check_top_level_code(dag_folder_path)

    # Print results as a table
    print(tabulate(results, headers=["File Name", "Top-Level Code", "Line Number"], tablefmt="grid"))


if __name__ == "__main__":
    main()
