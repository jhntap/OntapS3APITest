import boto3
import json
import pprint

endpoint = 'http://object.com'
s3client = boto3.client(service_name='s3', endpoint_url=endpoint, verify=False)
succ_cnt = 0

print('==============================================')
print('NetApp ONTAP 9.16')
print('S3 API Multipart Upload Operation Verification')
print('==============================================')
print('\n')

# ONTAP S3 supported action - Multipart Operation 
# AbortMultipartUpload
# CompleteMultipartUpload
# CreateMultipartUpload
# ListMultipartUpload
# ListParts
# UploadPart
# UploadPartCopy

#create bucket for multipart operation
bucket_name = 'test-bucket'
multipart_key_name = 'multipart-object'
multipart_abort_test_key = 'abort-test-object'
s3client.create_bucket(Bucket=bucket_name)
print('{} bucket created for multipart upload operation test'.format(bucket_name))
print('======================================================')
print('\n')

#create bucket for multipart part copy source
#create sample-text.txt(should be larger than 5MiB) on the same directory where this script is before running this method
src_bucket_name = 'src-bucket'
s3client.create_bucket(Bucket=src_bucket_name)
with open('sample-text.txt', 'rb') as sample_text:
        s3client.put_object(Body=sample_text, Key='sample-text.txt', Bucket=src_bucket_name)
print('{} bucket & object created for multipart copy operation test'.format(src_bucket_name))
print('======================================================')
print('\n')

#create multipart upload
print('[CreateMultipartUpload API]')
create_multipart_upload_resp = s3client.create_multipart_upload(Bucket=bucket_name, Key=multipart_key_name)
upload_id = create_multipart_upload_resp['UploadId']
pprint.pprint(create_multipart_upload_resp)
succ_cnt += 1
print('======================================================')
print('\n')

#list multipart
print('[ListMultipartUploads API]')
list_multipart_uploads_resp = s3client.list_multipart_uploads(Bucket=bucket_name)
pprint.pprint(list_multipart_uploads_resp)
succ_cnt += 1
print('======================================================')
print('\n')

#part lists
parts = []

#upload part
#create sample-large.txt(should be larger than 5MiB) on the same directory where this script is before running this method
print('[UploadPart API]')
with open('sample-large.txt', 'rb') as sample_large:
        upload_part_resp = s3client.upload_part(
            Bucket=bucket_name,
            Key=multipart_key_name,
            PartNumber=1,
            Body=sample_large,
            UploadId=upload_id
        )
pprint.pprint(upload_part_resp)
parts.append(
    {
       'PartNumber': 1,
       'ETag': upload_part_resp['ETag']
    }
)
succ_cnt += 1
print('======================================================')
print('\n')

#upload part copy
print('[UploadPartCopy API]')
upload_part_copy_resp = s3client.upload_part_copy(
    Bucket=bucket_name,
    CopySource='/src-bucket/sample-text.txt',
    Key=multipart_key_name,
    PartNumber=2,
    UploadId=upload_id
)
pprint.pprint(upload_part_copy_resp)
tmp_obj = upload_part_copy_resp['CopyPartResult']
parts.append(
    {
       'PartNumber': 2,
       'ETag': tmp_obj['ETag']
    }
)
succ_cnt += 1
print('======================================================')
print('\n')

#list parts
print('[ListParts API]')
list_parts_resp = s3client.list_parts(
    Bucket=bucket_name,
    Key=multipart_key_name,
    UploadId=upload_id
)
pprint.pprint(list_parts_resp)
succ_cnt += 1
print('======================================================')
print('\n')

#part lists for complete multipart upload
multipart_upload = {'Parts': parts}

#complete multipart upload
print('[CompleteMultipartUpload API]')
complete_multipart_upload_resp = s3client.complete_multipart_upload(
    Bucket=bucket_name,
    Key=multipart_key_name,
    UploadId=upload_id,
    MultipartUpload=multipart_upload
)
pprint.pprint(complete_multipart_upload_resp)
succ_cnt += 1
print('======================================================')
print('\n')

#create multipart upload for abortion test
abort_test_resp = s3client.create_multipart_upload(Bucket=bucket_name, Key=multipart_abort_test_key)
abort_test_upload_id = abort_test_resp['UploadId']
print('{} Multipart upload created for abortion test'.format(abort_test_upload_id))
print('======================================================')
print('\n')

# abort multipart upload
print('[AbortMultipartUpload API]')
abort_multipart_upload_resp = s3client.abort_multipart_upload(Bucket=bucket_name, Key=multipart_abort_test_key, UploadId=abort_test_upload_id)
pprint.pprint(abort_multipart_upload_resp)
succ_cnt += 1
print('======================================================')
print('\n')

print('=================================================================')
print("Total count of sucessful S3 Multipart Upload Operation APIs: {}".format(succ_cnt))
print('=================================================================')

print('\n')
s3client.delete_object(Bucket=bucket_name, Key=multipart_key_name)
s3client.delete_bucket(Bucket=bucket_name)
s3client.delete_object(Bucket=src_bucket_name, Key='sample-text.txt')
s3client.delete_bucket(Bucket=src_bucket_name)
print('Buckets & objectes created for multipart operation test are sucessfully deleted')
print('======================================================')
