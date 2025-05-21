import boto3
import uuid
import json
import pprint

endpoint = 'http://object.com'
s3client = boto3.client(service_name='s3', endpoint_url=endpoint, verify=False)
succ_cnt = 0

print('======================================')
print('NetApp ONTAP 9.16')
print('S3 API Bucket Operation Verification')
print('======================================')
print('\n')

# ONTAP S3 Supported Action - Bucket Operation
# CreateBucket o
# DeleteBucket o
# DeleteBucketPolicy o
# GetBucketAcl o
# GetBucketLifecycleConfiguration o
# GetBucketLocation o
# GetBucketPolicy o
# GetBucketVersioning o
# HeadBucket o
# ListAllMyBuckets ONTAP REST???
# ListBuckets o
# ListBucketVersioning ONTAP REST???
# PutBucket ONTAP REST???
# PutBucketLifecycleConfiguration o
# PutBucketPolicy o
# PutBucketVersioning o
# PutBucketCors
# GetBucketCors
# DeleteBucketCors

#create bucket
print('[CreateBucket API]')
bucket_name = 'test-bucket-{}'.format(uuid.uuid4())
create_bucket_resp = s3client.create_bucket(Bucket=bucket_name)
tmp_bucket_name = 'bkbucket'
s3client.create_bucket(Bucket=tmp_bucket_name)
succ_cnt += 1
pprint.pprint(create_bucket_resp)
print('======================================================')
print('\n')

#head bucket
print('[HeadBucket API]')
head_bucket_resp = s3client.head_bucket(Bucket=bucket_name)
succ_cnt += 1
pprint.pprint(head_bucket_resp)
print('======================================================')
print('\n')

#list buckets
print('[ListBuckets API]')
list_buckets_resp = s3client.list_buckets()
succ_cnt += 1
pprint.pprint(list_buckets_resp)
print('======================================================')
print('\n')

#get bucket locations
print('[GetBucketLocation API]')
get_bucket_location_resp = s3client.get_bucket_location(Bucket=bucket_name)
succ_cnt += 1
pprint.pprint(get_bucket_location_resp)
print('======================================================')
print('\n')

#get bucket ACL
print('[GetBucketACL API]')
get_bucket_acl_resp = s3client.get_bucket_acl(Bucket=bucket_name)
succ_cnt += 1
pprint.pprint(get_bucket_acl_resp)
print('======================================================')
print('\n')

#set bucket lifecycle configuration
print('[PutBucketLifecycleConfiguration API]')
put_bucket_lifecycle_config_resp = s3client.put_bucket_lifecycle_configuration(
    Bucket=bucket_name,
    LifecycleConfiguration={
        'Rules': [
            {
                'ID': 'rule2',
                'Filter': {
                        'Prefix': 'catagory2/'
                },
                'NoncurrentVersionExpiration': {
                        'NoncurrentDays': 50
                },
                'Status': 'Enabled'
            }
        ]
    }
)
succ_cnt += 1
pprint.pprint(put_bucket_lifecycle_config_resp)
print('======================================================')
print('\n')

#get bucket lifecycle configuration
print('[GetBucketLifecycleConfiguration API]')
get_bucket_lifecycle_config_resp = s3client.get_bucket_lifecycle_configuration(Bucket=bucket_name)
succ_cnt += 1
pprint.pprint(get_bucket_lifecycle_config_resp)
print('======================================================')
print('\n')

#delete bucket lifecycle configuration
print('[DeleteBucketLifecycleConfiguration API]')
delete_bucket_lifecycle_config_resp = s3client.delete_bucket_lifecycle(Bucket=bucket_name)
succ_cnt += 1
pprint.pprint(delete_bucket_lifecycle_config_resp)
print('======================================================')
print('\n')

#set bucket policy
# bkbucket must be created before running this method (included in CreateBucket section)
print('[PutBucketPolicy API]')
put_bucket_policy_resp = s3client.put_bucket_policy(
    Bucket='bkbucket',
    Policy='{"Statement": [{"Effect": "Allow","Principal": "*", "Action": [ "s3:PutObject","s3:GetObject"], "Resource": ["arn:aws:s3:::bkbucket/*" ] } ]}',
)
succ_cnt += 1
pprint.pprint(put_bucket_policy_resp)
print('======================================================')
print('\n')

#get bucket policy
print('[GetBucketPolicy API]')
get_bucket_policy_resp = s3client.get_bucket_policy(Bucket='bkbucket')
succ_cnt += 1
pprint.pprint(get_bucket_policy_resp)
print('======================================================')
print('\n')

#delete bucket policy
print('[DeleteBucketPolicy API]')
delete_bucket_policy_resp = s3client.delete_bucket_policy(Bucket='bkbucket')
succ_cnt += 1
pprint.pprint(delete_bucket_policy_resp)
print('======================================================')
print('\n')

#set bucket versioning
print('[PutBucketVersioning API]')
put_bucket_versioning_resp = s3client.put_bucket_versioning(
    Bucket=bucket_name,
    VersioningConfiguration={
        'Status': 'Enabled'
    }
)
succ_cnt += 1
pprint.pprint(put_bucket_versioning_resp)
print('======================================================')
print('\n')

#get bucket versioning
print('[GetBucketVersioning API]')
get_bucket_versioning_resp = s3client.get_bucket_versioning(Bucket=bucket_name)
succ_cnt += 1
pprint.pprint(get_bucket_versioning_resp)
print('======================================================')
print('\n')

#set bucket cors
print('[PutBucketCors API]')
put_bucket_cors_resp = s3client.put_bucket_cors(
    Bucket=bucket_name,
    CORSConfiguration={
        'CORSRules': [
            {
                'AllowedHeaders': [
                    '*',
                ],
                'AllowedMethods': [
                    'PUT',
                    'POST',
                    'DELETE',
                ],
                'AllowedOrigins': [
                    'http://www.example.com',
                ],
                'ExposeHeaders': [
                    'x-amz-server-side-encryption',
                ],
                'MaxAgeSeconds': 3000,
            },
            {
                'AllowedHeaders': [
                    'Authorization',
                ],
                'AllowedMethods': [
                    'GET',
                ],
                'AllowedOrigins': [
                    '*',
                ],
                'MaxAgeSeconds': 3000,
            },
        ],
    }
)
succ_cnt += 1
pprint.pprint(put_bucket_cors_resp)
print('======================================================')
print('\n')

#get bucket cors
print('[GetBucketCors API]')
get_bucket_cors_resp = s3client.get_bucket_cors(Bucket=bucket_name)
succ_cnt += 1
pprint.pprint(get_bucket_cors_resp)
print('======================================================')
print('\n')

#delete bucket cors
print('[DeleteBucketCors API]')
delete_bucket_cors_resp = s3client.delete_bucket_cors(Bucket=bucket_name)
succ_cnt += 1
pprint.pprint(delete_bucket_cors_resp)
print('======================================================')
print('\n')

#delete bucket
print('[DeleteBucket API]')
delete_bucket_resp = s3client.delete_bucket(Bucket=bucket_name)
s3client.delete_bucket(Bucket=tmp_bucket_name)
succ_cnt += 1
pprint.pprint(delete_bucket_resp)
print('======================================================')
print('\n')

print('======================================================')
print("total count of sucessful S3 Bucket Operation APIs: {}".format(succ_cnt))
print('======================================================')
