__author__ = 'dave'

from ftplib import FTP
from tempfile import mkdtemp
from boto.s3.connection import S3Connection
from boto.utils import parse_ts
from boto.s3.key import Key
from os import path
import os
import math
from datetime import datetime
from filechunkio import FileChunkIO

# Requires environmental keys
# AWS_ACCESS_KEY_ID - Your AWS Access Key ID
# AWS_SECRET_ACCESS_KEY - Your AWS Secret Access Key


def sync_ftp_to_s3():
    """
    This task will sync the latest full copy of FBO's xml and any intermediary files. It will overwrite the weekly file.
    We make a personal s3 copy of the data since the FBO ftp service is unreliable and tends to get hammered
    during peak hours.
    """


    temp_dir = mkdtemp()

    conn = S3Connection('AKIAICNDXYCQQG2TWUYQ', 'nX+ECYHB8UFSU20A0Bu7uu0MVy0zsSUYSY19Kz76')
    vitals_bucket = conn.get_bucket('fogmine-data')

    ftp = FTP('ftp.fbo.gov')
    ftp.login()

    sourceModifiedTime = ftp.sendcmd('MDTM datagov/FBOFullXML.xml')[4:]
    sourceModifiedDateTime = datetime.strptime(sourceModifiedTime, "%Y%m%d%H%M%S").strftime("%d %B %Y %H:%M:%S")

    fullFBOKey = vitals_bucket.get_key('/staging/fbo/FBOFullXML.xml')



    # daily_files = [x for x in ftp.nlst() if 'FBOFeed' in x]

    # for f in daily_files:
    #     local_file_path = path.join(temp_dir, f)
    #
    #     #Connect to S3
    #     k = vitals_bucket.get_key('/staging/fbo/'+f)
    #
    #     if k:
    #         continue
    #
    #     fileObj = open(local_file_path, 'wb')
    #     # Download the file a chunk at a time using RETR
    #     ftp.retrbinary('RETR ' + f, fileObj.write)
    #     # Close the file
    #     fileObj.close()
    #
    #     # Put file to S3
    #     k = Key(vitals_bucket)
    #     k.key = '/staging/fbo/'+f
    #     k.set_contents_from_filename(local_file_path)

    if False and not fullFBOKey or parse_ts(fullFBOKey.last_modified) < sourceModifiedDateTime:
        #Update S3 copy with latest
        print "downloading the latest full xml from repository"
        storage_path = path.join(temp_dir, 'FBOFullXML.xml'),
        fileObj = open(storage_path, 'wb')
        # Download the file a chunk at a time using RETR
        ftp.retrbinary('RETR datagov/FBOFullXML.xml', fileObj.write)
        # Close the file
        fileObj.close()
        ftp.close()

        print "uploading the latest full xml to S3"
        # Put file to S3
        source_size = os.stat(storage_path).st_size

        # Create a multipart upload request
        mp = vitals_bucket.initiate_multipart_upload('/staging/fbo/FBOFullXML.xml')

        # Use a chunk size of 50 MiB (feel free to change this)
        chunk_size = 52428800
        chunk_count = int(math.ceil(source_size / chunk_size))

        # Send the file parts, using FileChunkIO to create a file-like object
        # that points to a certain byte range within the original file. We
        # set bytes to never exceed the original file size.
        for i in range(chunk_count + 1):
            print "uploading chunk {0} of {1}".format(i, chunk_count)
            offset = chunk_size * i
            bytes = min(chunk_size, source_size - offset)
            with FileChunkIO(storage_path, 'r', offset=offset,
                                 bytes=bytes) as fp:
                 mp.upload_part_from_file(fp, part_num=i + 1)

        # Finish the upload
        mp.complete_upload()

    else:
        ftp.close()

if __name__ == '__main__':
    sync_ftp_to_s3()
