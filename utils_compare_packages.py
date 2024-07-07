def read_file(filename):
    with open(filename, 'r') as f:
        return set(line.strip().split('==')[0].lower() for line in f)


installed = read_file('all_installed_packages.txt')
used = read_file('project_imports.txt')

unused = installed - used

print("Packages that might be unused:")
for package in sorted(unused):
    print(package)

print("\nWarning: This list may include dependencies of your directly used packages.")
print("Always verify before uninstalling any package.")
