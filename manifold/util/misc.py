import os, glob, inspect


# Define the inclusion operator
class contains(type): pass

def find_local_modules(filepath):
    modules = []
    for f in glob.glob(os.path.dirname(filepath)+"/*.py"):
        name = os.path.basename(f)[:-3]
        if name != '__init__':
            modules.append(name)
    return modules 
