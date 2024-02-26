# Example script to check for top-level code in DAGs folder
import os


def check_dags_for_top_level_code():
    dags_folder = "dags"  # Assuming your DAGs folder is named 'dags'
    for root, dirs, files in os.walk(dags_folder):
        for file in files:
            if file.endswith(".py"):
                with open(os.path.join(root, file), "r") as f:
                    for line in f:
                        # Example: Check for import statements
                        if line.strip().startswith("import") or line.strip().startswith("from"):
                            return True
    return False


if __name__ == "__main__":
    has_top_level_code = check_dags_for_top_level_code()
    print(has_top_level_code)
