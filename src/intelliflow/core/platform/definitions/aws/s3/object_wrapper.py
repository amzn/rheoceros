# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import logging
from typing import Sequence

from botocore.exceptions import ClientError

from intelliflow.core.platform.definitions.aws.common import exponential_retry

logger = logging.getLogger(__name__)


"""
Refer 
https://github.com/awsdocs/aws-doc-sdk-examples/blob/master/python/example_code/s3/s3_basics/object_wrapper.py
"""


def build_object_key(folders: Sequence[str], object_name: str) -> str:
    return "/".join(folders + [object_name])


def object_exists(s3, bucket, object_key):
    try:
        exponential_retry(
            s3.meta.client.head_object,
            ["403"],  # retry on 'Forbidden': eventual consistency during app activation
            Bucket=bucket.name,
            Key=object_key,
        )
    except ClientError as ex:
        if ex.response["Error"]["Code"] == "404":
            return False
        logger.exception("Couldn't head-object '%s' from bucket '%s'! Exception: '%s'.", object_key, bucket.name, str(ex))
        raise

    return True


def put_file(bucket, object_key, file_name):
    """
    Upload data to a bucket and identify it with the specified object key.
    :param bucket: The bucket to receive the data.
    :param object_key: The key of the object in the bucket.
    :param file_name: The name of the file to be uploaded. it is opened in read bytes mode.
    """
    try:
        put_data = open(file_name, "rb")
    except IOError:
        logger.exception("Expected file name, got '%s'.", file_name)
        raise

    try:
        put_object(bucket, object_key, put_data)
        return True
    finally:
        if getattr(put_data, "close", None):
            put_data.close()


def put_object(bucket, object_key, put_data):
    """
    Upload data to a bucket and identify it with the specified object key.
    :param bucket: The bucket to receive the data.
    :param object_key: The key of the object in the bucket.
    :param put_data: The data as str to upload.
    """
    try:
        obj = bucket.Object(object_key)
        obj.put(Body=put_data)
        obj.wait_until_exists()
        logger.info("Put object '%s' to bucket '%s'.", object_key, bucket.name)
        return True
    except ClientError:
        logger.exception("Couldn't put object '%s' to bucket '%s'.", object_key, bucket.name)
        raise


def get_object(bucket, object_key):
    """
    Gets an object from a bucket.
    :param bucket: The bucket that contains the object.
    :param object_key: The key of the object to retrieve.
    :return: The object data in bytes.
    """
    try:
        body = bucket.Object(object_key).get()["Body"].read()
        logger.info("Got object '%s' from bucket '%s'.", object_key, bucket.name)
    except ClientError:
        logger.exception(("Couldn't get object '%s' from bucket '%s'.", object_key, bucket.name))
        raise
    else:
        return body


def list_objects(bucket, prefix=None, limit=None):
    """
    Lists the objects in a bucket, optionally filtered by a prefix.
    :param bucket: The bucket to query.
    :param prefix: When specified, only objects that start with this prefix are listed.
    :param limit: Limit the number of objects to be retrieved. If not specified, returns all of the objects.
    :return: The list of objects.
    """
    try:
        if prefix is None:
            objects = bucket.objects.all()
        else:
            objects = bucket.objects.filter(Prefix=prefix)

        if limit is not None:
            objects = objects.limit(limit)
        logger.info(f"Listed objects from bucket {bucket.name!r}")
    except ClientError:
        logger.exception("Couldn't list objects for bucket '%s'.", bucket.name)
        raise
    else:
        return objects


def copy_object(source_bucket, source_object_key, dest_bucket, dest_object_key):
    """
    Copies an object from one bucket to another.
    :param source_bucket: The bucket that contains the source object.
    :param source_object_key: The key of the source object.
    :param dest_bucket: The bucket that receives the copied object.
    :param dest_object_key: The key of the copied object.
    :return: The new copy of the object.
    """
    try:
        obj = dest_bucket.Object(dest_object_key)
        obj.copy_from(CopySource={"Bucket": source_bucket.name, "Key": source_object_key})
        obj.wait_until_exists()
        logger.info("Copied object from %s/%s to %s/%s.", source_bucket.name, source_object_key, dest_bucket.name, dest_object_key)
    except ClientError:
        logger.exception(
            "Couldn't copy object from %s/%s to %s/%s.", source_bucket.name, source_object_key, dest_bucket.name, dest_object_key
        )
        raise
    else:
        return obj


def delete_object(bucket, object_key):
    """
    Removes an object from a bucket.
    :param bucket: The bucket that contains the object.
    :param object_key: The key of the object to delete.
    """
    try:
        obj = bucket.Object(object_key)
        obj.delete()
        obj.wait_until_not_exists()
        logger.info("Deleted object '%s' from bucket '%s'.", object_key, bucket.name)
        return True
    except ClientError:
        logger.exception("Couldn't delete object '%s' from bucket '%s'.", object_key, bucket.name)
        raise


def delete_objects(bucket, object_keys) -> bool:
    """
    Removes a list of objects from a bucket.
    This operation is done as a batch in a single request.
    :param bucket: The bucket that contains the objects.
    :param object_keys: The list of keys that identify the objects to remove.
    :return: The response that contains data about which objects were deleted
             and any that could not be deleted.
    """
    try:
        response = bucket.delete_objects(Delete={"Objects": [{"Key": key} for key in object_keys]})
        if "Deleted" in response:
            pass
        if "Errors" in response:
            return False
    except ClientError:
        logger.exception("Couldn't delete objects '%s' from bucket '%s'.", object_keys, bucket.name)
        raise
    else:
        return True


def delete_folder(bucket, folder_prefix) -> bool:
    completely_wiped_out = True
    objects_in_folder = list_objects(bucket, folder_prefix)
    keys = []
    for object in objects_in_folder:
        key = object.key
        keys.append(key)
        if len(keys) == 50:
            if not delete_objects(bucket, keys):
                completely_wiped_out = False
            keys = []
    if keys:
        if not delete_objects(bucket, keys):
            completely_wiped_out = False
    return completely_wiped_out


def empty_bucket(bucket):
    """
    Remove all objects from a bucket.
    :param bucket: The bucket to empty.
    """
    try:
        bucket.object_versions.delete()
        bucket.objects.delete()
        logger.info("Emptied bucket '%s'.", bucket.name)
        return True
    except ClientError:
        logger.exception("Couldn't empty bucket '%s'.", bucket.name)
        raise


def put_acl(bucket, object_key, email):
    """
    Applies an ACL to an object that grants read access to an AWS user identified
    by email address.
    :param bucket: The bucket that contains the object.
    :param object_key: The key of the object to update.
    :param email: The email address of the user to grant access.
    """
    try:
        acl = bucket.Object(object_key).Acl()
        # Putting an ACL overwrites the existing ACL, so append new grants
        # if you want to preserve existing grants.
        grants = acl.grants if acl.grants else []
        grants.append({"Grantee": {"Type": "AmazonCustomerByEmail", "EmailAddress": email}, "Permission": "READ"})
        acl.put(AccessControlPolicy={"Grants": grants, "Owner": acl.owner})
        logger.info("Granted read access to %s.", email)
        return True
    except ClientError:
        logger.exception("Couldn't add ACL to object '%s'.", object_key)
        raise


def get_acl(bucket, object_key):
    """
    Gets the ACL of an object.
    :param bucket: The bucket that contains the object.
    :param object_key: The key of the object to retrieve.
    :return: The ACL of the object.
    """
    try:
        acl = bucket.Object(object_key).Acl()
        logger.info("Got ACL for object %s owned by %s.", object_key, acl.owner["DisplayName"])
    except ClientError:
        logger.exception("Couldn't get ACL for object %s.", object_key)
        raise
    else:
        return acl
