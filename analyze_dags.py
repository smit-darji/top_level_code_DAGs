import os
import ast


def has_top_level_code(file_path):
    with open(file_path, 'r') as f:
        code = f.read()
        try:
            parsed_ast = ast.parse(code)
            for node in parsed_ast.body:
                if isinstance(node, (ast.FunctionDef, ast.ClassDef)):
                    continue
                else:
                    return True
            return False
        except SyntaxError:
            return True


def analyze_dag_files(directory):
    results = []
    for root, dirs, files in os.walk(directory):
        for file in files:
            if file.endswith(".py"):
                file_path = os.path.join(root, file)
                if has_top_level_code(file_path):
                    results.append((file_path, "Contains top-level code"))
                else:
                    results.append((file_path, "Does not contain top-level code"))
    return results


dag_directory = "top_level_code_DAGs/dags"
results = analyze_dag_files(dag_directory)
for file_path, result in results:
    print(f"File: {file_path}")
    print(result)
    print()
