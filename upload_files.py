from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive
import os
import concurrent.futures
import time

gauth = GoogleAuth()
gauth.LocalWebserverAuth()
drive = GoogleDrive(gauth)

if __name__ == "__main__":

    # View all folders and file in your Google Drive
    fileList = drive.ListFile({'q': "'root' in parents and trashed=false"}).GetList()
    for file in fileList:
        # print('Title: %s, ID: %s' % (file['title'], file['id']))
        # Get the folder ID that you want
        if file['title'] == "storemaven_data":
            fileID = file['id']

    def mt_file_uploader(file, folder_id=fileID):
        f = drive.CreateFile({
            'title': file,
            'mimeType': 'text/csv',
            'parents': [{'kind': 'drive#fileLink', 'id': folder_id}]
        })
        f.SetContentFile(os.path.join('./data', file))
        f.Upload()

    files_in_local_folder = os.listdir('./data')
    chunks = [files_in_local_folder[x:x + 500] for x in range(0, len(files_in_local_folder), 500)]
    for chunk in chunks:
        with concurrent.futures.ThreadPoolExecutor() as executor:
            executor.map(mt_file_uploader, chunk)

        time.sleep(30)