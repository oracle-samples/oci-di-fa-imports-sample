/*
 * Copyright (c) 2022 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl/
 */

package oci

import (
	"context"
	"io"
	"log"

	"github.com/oracle/oci-go-sdk/common"
	"github.com/oracle/oci-go-sdk/common/auth"
	"github.com/oracle/oci-go-sdk/objectstorage"
)

// Client interface for oci services
type Client interface {
	ListObjects(namespace, sourceBucketName, prefix string) ([]objectstorage.ObjectSummary, error)
	GetObject(namespace, sourceBucketName, name string) (objectstorage.GetObjectResponse, error)
	PutObject(namespace string, targetBucketName string, objectName string, objectUpload io.ReadCloser) error
	DeleteObject(namespace, sourceBucketName, objectName string) error
}

type client struct {
	objectStorageClient objectstorage.ObjectStorageClient
}

// CreateNewClient returns the structure used as the client, authenticated with the Resource Principal
func CreateNewClient() (Client, error) {
	// for the function, we need to use ResourcePrincipal
	provider, err := auth.ResourcePrincipalConfigurationProvider()
	if err != nil {
		return nil, err
	}

	objClient, err := objectstorage.NewObjectStorageClientWithConfigurationProvider(provider)
	if err != nil {
		return nil, err
	}

	client := &client{objectStorageClient: objClient}

	return client, nil
}

// ListObjects from a bucket in a namespace , filtering with the prefix
func (c *client) ListObjects(namespace, sourceBucketName, prefix string) ([]objectstorage.ObjectSummary, error) {
	listObjectReq := objectstorage.ListObjectsRequest{
		NamespaceName: common.String(namespace),
		BucketName:    common.String(sourceBucketName),
		Prefix:        common.String(prefix),
	}

	log.Print("Starting to list objects")

	listObjectRes, err := c.objectStorageClient.ListObjects(context.Background(), listObjectReq)

	if err != nil {
		log.Printf("error listing objects %v", err)

		return nil, err
	}

	return listObjectRes.ListObjects.Objects, nil
}

// GetObject from a bucket in a namespace by the name
func (c *client) GetObject(namespace, sourceBucketName, name string) (objectstorage.GetObjectResponse, error) {
	getObjReq := objectstorage.GetObjectRequest{
		NamespaceName: common.String(namespace),
		BucketName:    common.String(sourceBucketName),
		ObjectName:    common.String(name),
	}

	return c.objectStorageClient.GetObject(context.Background(), getObjReq)
}

// PutObject in a namespace bucket
func (c *client) PutObject(namespace string, targetBucketName string,
	objectName string, objectUpload io.ReadCloser) error {
	putObjReq := objectstorage.PutObjectRequest{
		NamespaceName: common.String(namespace),
		BucketName:    common.String(targetBucketName),
		ObjectName:    common.String(objectName),
		PutObjectBody: objectUpload,
	}

	_, err := c.objectStorageClient.PutObject(context.Background(), putObjReq)

	return err
}

// DeleteObject from a bucket
func (c *client) DeleteObject(namespace, sourceBucketName, objectName string) error {
	deleteObjReq := objectstorage.DeleteObjectRequest{
		NamespaceName: common.String(namespace),
		BucketName:    common.String(sourceBucketName),
		ObjectName:    common.String(objectName),
	}
	_, err := c.objectStorageClient.DeleteObject(context.Background(), deleteObjReq)

	return err
}
