import os
import shutil

src = os.path.join(os.path.dirname(__file__), "cwl_helper.py")
dst = os.path.join(os.getcwd(), "cwl_helper.py")  # this will be the generated folder

shutil.copyfile(src, dst)