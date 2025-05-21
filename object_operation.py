import boto3
import json
import pprint

endpoint = 'http://192.168.0.141'
s3client = boto3.client(service_name='s3', endpoint_url=endpoint, verify=False)
succ_cnt = 0

print('======================================')
print('NetApp ONTAP 9.16')
print('S3 API Object Operation Verification')
print('======================================')
print('\n')

#create bucket for object operation
src_bucket_name = 'src-bucket'
s3client.create_bucket(Bucket=src_bucket_name)
print('{} bucket created for object operation test'.format(src_bucket_name))
print('======================================================')
print('\n')

dst_bucket_name = 'dst-bucket'
s3client.create_bucket(Bucket=dst_bucket_name)
print('{} bucket created for object operation test'.format(dst_bucket_name))
print('======================================================')
print('\n')

#put object
#create sample-text.txt on the same directory where this script is before running this method
print('[PutObject API]')
put_object_resp = s3client.put_object(
    Body='sample-text.txt',
    Key='sample-text.txt',
    Bucket=src_bucket_name
)
succ_cnt += 1
pprint.pprint(put_object_resp)
print('======================================================')
print('\n')

#head object
print('[HeadObject API]')
head_object_resp = s3client.head_object(
    Bucket=src_bucket_name,
    Key='sample-text.txt'
)
succ_cnt += 1
pprint.pprint(head_object_resp)
print('======================================================')
print('\n')

#list objects v1
print('[ListObjects v1 API]')
list_objects_resp = s3client.list_objects(Bucket=src_bucket_name)
succ_cnt += 1
pprint.pprint(list_objects_resp)
print('======================================================')
print('\n')

#copy object
print('[CopyObject API]')
copy_object_resp = s3client.copy_object(
    Bucket=dst_bucket_name,
    CopySource='/src-bucket/sample-text.txt',
    Key='copied-sample-text.txt'
)
succ_cnt += 1
pprint.pprint(copy_object_resp)
print('======================================================')
print('\n')

#list objects v2
print('[ListObjects v2 API]')
list_objects_v2_resp = s3client.list_objects_v2(Bucket=dst_bucket_name)
succ_cnt += 1
pprint.pprint(list_objects_v2_resp)
print('======================================================')
print('\n')

#list object versions
print('[ListObjectVersions API]')
list_object_versions_resp = s3client.list_object_versions(Bucket=dst_bucket_name)
succ_cnt += 1
pprint.pprint(list_object_versions_resp)
print('======================================================')
print('\n')

#get object
print('[GetObject API]')
get_object_resp = s3client.get_object(
    Bucket=src_bucket_name,
    Key='sample-text.txt'
)
pprint.pprint(get_object_resp)
# range read
get_object_range_resp = s3client.get_object(
    Bucket=src_bucket_name,
    Key='sample-text.txt',
    Range='bytes=0-9'
)
pprint.pprint(get_object_range_resp)
succ_cnt += 1
print('======================================================')
print('\n')

#get object acl
print('[GetObjectACL API]')
get_object_acl_resp = s3client.get_object_acl(
    Bucket=src_bucket_name,
    Key='sample-text.txt'
)
pprint.pprint(get_object_acl_resp)
succ_cnt += 1
print('======================================================')
print('\n')

#put object tagging
print('[PutObjectTagging API]')
put_object_tagging_resp = s3client.put_object_tagging(
    Bucket=src_bucket_name,
    Key='sample-text.txt',
    Tagging={
        'TagSet': [
            {
                'Key': 'author',
                'Value': 'Shakespeare',
            },
        ],
    },
)
succ_cnt += 1
pprint.pprint(put_object_tagging_resp)
print('======================================================')
print('\n')

#get object tagging
print('[GetObjectTagging API]')
get_object_tagging_resp = s3client.get_object_tagging(
    Bucket=src_bucket_name,
    Key='sample-text.txt'
)
pprint.pprint(get_object_tagging_resp)
succ_cnt += 1
print('======================================================')
print('\n')

#delete object tagging
print('[DeleteObjectTagging API]')
delete_object_tagging_resp = s3client.delete_object_tagging(
    Bucket=src_bucket_name,
    Key='sample-text.txt'
)
pprint.pprint(delete_object_tagging_resp)
succ_cnt += 1
print('======================================================')
print('\n')

#delete object
print('[DeleteObject API]')
delete_object_resp = s3client.delete_object(
    Bucket=src_bucket_name,
    Key='sample-text.txt'
)
pprint.pprint(delete_object_resp)
succ_cnt += 1
print('======================================================')
print('\n')

#delete multiple objects
print('[DeleteObjects API]')
delete_objects_resp = s3client.delete_objects(
    Bucket=dst_bucket_name,
    Delete={
        'Objects': [
            {
                'Key': 'copied-sample-text.txt',
            },
        ],
        'Quiet': False,
    },
)
pprint.pprint(delete_objects_resp)
succ_cnt += 1
print('======================================================')
print('\n')

print('======================================================')
print("total count of sucessful S3 Object Operation APIs: {}".format(succ_cnt))
print('======================================================')

print('\n')
s3client.delete_bucket(Bucket=src_bucket_name)
s3client.delete_bucket(Bucket=dst_bucket_name)
print('Buckets created for object operation test are sucessfully deleted')
print('======================================================')
