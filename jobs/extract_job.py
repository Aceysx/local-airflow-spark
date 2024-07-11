import requests
import zipfile
import os
import shutil


def download_and_extract_zip(skip):
    if skip:
        return None

    extract_to = '/opt/data'
    url = 'https://f001.backblazeb2.com/file/Backblaze-Hard-Drive-Data/data_Q3_2019.zip'
    extract_dir = os.path.join(extract_to, f"extracted_drive_data")

    os.makedirs(extract_dir, exist_ok=True)

    response = requests.get(url)
    if response.status_code == 200:
        zip_path = os.path.join(extract_to, "temp.zip")
        with open(zip_path, 'wb') as file:
            file.write(response.content)

        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(extract_dir)

        macosx_folder = os.path.join(extract_dir, '__MACOSX')
        if os.path.exists(macosx_folder):
            shutil.rmtree(macosx_folder)

        os.remove(zip_path)

        print(f"Files extracted and flattened to {extract_dir}")
        return extract_dir
    else:
        print(f"Failed to download file. Status code: {response.status_code}")
        return None
