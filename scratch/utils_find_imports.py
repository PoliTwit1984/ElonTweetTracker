import os
import ast


def find_imports(directory):
    imports = set()
    for root, dirs, files in os.walk(directory):
        if '.venv' in dirs:
            dirs.remove('.venv')  # don't visit .venv directories
        for file in files:
            if file.endswith('.py'):
                file_path = os.path.join(root, file)
                with open(file_path, 'r') as f:
                    try:
                        tree = ast.parse(f.read())
                    except SyntaxError:
                        print(f"Couldn't parse {file_path}")
                        continue
                for node in ast.walk(tree):
                    if isinstance(node, ast.Import):
                        for alias in node.names:
                            imports.add(alias.name.split('.')[0])
                    elif isinstance(node, ast.ImportFrom):
                        if node.level == 0:  # absolute import
                            imports.add(node.module.split('.')[0])
    return imports


project_dir = '.'
project_imports = find_imports(project_dir)

print("Imports found in your project:")
for imp in sorted(project_imports):
    print(imp)

with open('project_imports.txt', 'w') as f:
    for imp in sorted(project_imports):
        f.write(f"{imp}\n")

print("Project imports have been written to project_imports.txt")
