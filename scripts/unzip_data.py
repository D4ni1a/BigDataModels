import zipfile
import os

zip_filename = './data/f11.zip'
destination_dir = './data'

os.makedirs(destination_dir, exist_ok=True)
with zipfile.ZipFile(zip_filename, 'r') as archive:
    archive.extractall(path=destination_dir)

os.rename(os.path.join(destination_dir, "f11"),
            os.path.join(destination_dir, "f11_raw"))