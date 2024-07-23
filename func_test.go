/*
 * Copyright (c) 2022 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl/
 */

package main

import (
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"reflect"
	"strings"
	"testing"

	"github.com/oracle/oci-go-sdk/v56/objectstorage"

	"func/oci"
)

type ociClientMock struct {
	listObjectsResponse map[string]string
}

func (c *ociClientMock) ListObjects(namespace, sourceBucketName, prefix string) ([]objectstorage.ObjectSummary, error) {
	log.Print("ListObjects Mock")
	var objects []objectstorage.ObjectSummary
	json.Unmarshal([]byte(c.listObjectsResponse[sourceBucketName]), &objects)

	return objects, nil
}

func (c *ociClientMock) GetObject(namespace, sourceBucketName, name string) (objectstorage.GetObjectResponse, error) {
	log.Print("GetObject Mock")
	r := ioutil.NopCloser(strings.NewReader(""))
	return objectstorage.GetObjectResponse{
		Content: r,
	}, nil
}

func (c *ociClientMock) PutObject(namespace string, targetBucketName string, objectName string, objectUpload io.ReadCloser) error {
	log.Print("PutObject Mock")
	return nil
}

func (c *ociClientMock) DeleteObject(namespace, sourceBucketName, objectName string) error {
	log.Print("DeleteObject Mock")
	if "customer-import/part-objecterror.csv" == objectName {
		return fmt.Errorf("error deleting object from original storage")
	}
	return nil
}

func Test_processFiles(t *testing.T) {
	ociClientMock := &ociClientMock{}
	ociClientMock.listObjectsResponse = make(map[string]string,2)
	ociClientMock.listObjectsResponse["erp-transformed"] = "[{\"name\":\"customer-import/\",\"size\":null,\"md5\":null,\"timeCreated\":null,\"etag\":null,\"timeModified\":null},{\"name\":\"customer-import/ArUpdCustomers-anderson.csv\",\"size\":null,\"md5\":null,\"timeCreated\":null,\"etag\":null,\"timeModified\":null},{\"name\":\"customer-import/part-00000-2374e489-0ffd-47d1-8eb8-9eb623d65986-c000.csv\",\"size\":null,\"md5\":null,\"timeCreated\":null,\"etag\":null,\"timeModified\":null}]"
	ociClientMock.listObjectsResponse["erp-transformed-error"] = "[{\"name\":\"customer-import/\",\"size\":null,\"md5\":null,\"timeCreated\":null,\"etag\":null,\"timeModified\":null},{\"name\":\"customer-import/ArUpdCustomers-anderson.csv\",\"size\":null,\"md5\":null,\"timeCreated\":null,\"etag\":null,\"timeModified\":null},{\"name\":\"customer-import/part-objecterror.csv\",\"size\":null,\"md5\":null,\"timeCreated\":null,\"etag\":null,\"timeModified\":null}]"

	type args struct {
		request   *Request
		ociClient oci.Client
	}
	tests := []struct {
		name string
		args args
		want *Response
	}{
		{name: "TestSuccess",
			args: args{
				request: &Request{
					Namespace:        "ateamsaas",
					SourceBucketName: "erp-transformed",
					TargetBucketName: "erp-zip",
					Prefix:           "customer-import/",
				},
				ociClient: ociClientMock,
			},
			want: &Response{
				ProcessedWithSuccess: []string{"customer-import/part-00000-2374e489-0ffd-47d1-8eb8-9eb623d65986-c000.csv"},
				ProcessedWithError:   nil,
			},
		},
		{name: "TestError",
			args: args{
				request: &Request{
					Namespace:        "ateamsaas",
					SourceBucketName: "erp-transformed-error",
					TargetBucketName: "erp-zip",
					Prefix:           "customer-import/",
				},
				ociClient: ociClientMock,
			},
			want: &Response{
				ProcessedWithSuccess: nil,//[]string{"customer-import/part-00000-2374e489-0ffd-47d1-8eb8-9eb623d65986-c000.csv"},
				ProcessedWithError: []Errors{{
					Name:  "customer-import/part-objecterror.csv",
					Error: "error deleting file error deleting object from original storage",
				}},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := processFiles(tt.args.request, tt.args.ociClient); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("processFiles() = %v, want %v", got, tt.want)
			}
		})
	}
}
