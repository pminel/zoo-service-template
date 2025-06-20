import shutil
import os

# Path to your template's root
project_dir = os.path.realpath(os.path.curdir)

# Copy the helper file from hook dir into the generated project
shutil.copyfile(
    os.path.join(os.path.dirname(__file__), "cwl_helper.py"),
    os.path.join(project_dir, "cwl_helper.py")
)
