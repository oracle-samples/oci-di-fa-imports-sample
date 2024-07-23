/*
 * Copyright (c) 2022 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl/
 */

package main

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"strings"

	"github.com/fnproject/fdk-go"

	"func/oci"
)

type Request struct {
	Namespace        string `json:"namespace"`
	SourceBucketName string `json:"sourceBucketName"`
	TargetBucketName string `json:"targetBucketName"`
	Prefix           string `json:"prefix"`
}

type Response struct {
	ProcessedWithSuccess []string
	ProcessedWithError   []Errors
}

type Errors struct {
	Name  string
	Error string
}

func main() {
	fdk.Handle(fdk.HandlerFunc(myHandler))
}

func myHandler(ctx context.Context, in io.Reader, out io.Writer) {
	//creates the new client structure that contains the authentication
	ociClient, err := oci.CreateNewClient()
	if err != nil {
		log.Panicf("error creating oci client %v", err)
	}

	var request Request

	err = json.NewDecoder(in).Decode(&request)
	if err != nil {
		log.Panicf("error parsing request %v", err)
	}

	log.Printf("Request %v", request)

	response := processFiles(&request, ociClient)

	err = json.NewEncoder(out).Encode(response)
	if err != nil {
		log.Printf("error parsing response %v", err)
	}
}
// processFiles will list the objects from the source bucket, zip the files and upload to the target bucket
func processFiles(request *Request, ociClient oci.Client) *Response {
	log.Print("Starting ZipFiles Function")

	response := &Response{}
	listObjects, err := ociClient.ListObjects(request.Namespace, request.SourceBucketName, request.Prefix)

	if err != nil {
		log.Printf("error listobjects %v", err)
		addErrorToOutput(response, "oci bucket", err)
		return response
	}

	for _, object := range listObjects {
		if !strings.Contains(*object.Name, "part") {
			continue
		}

		getObjRes, err := ociClient.GetObject(request.Namespace, request.SourceBucketName, *object.Name)
		if err != nil {
			log.Printf("error get object %v", err)
			addErrorToOutput(response, *object.Name, err)

			continue
		}

		fileName := request.Prefix + "_" + strings.ReplaceAll(*object.Name, request.Prefix+"/", "")
		// zip file
		log.Printf("zip file %s", fileName)

		buf := new(bytes.Buffer)
		zipWriter := zip.NewWriter(buf)
		w1, err := zipWriter.Create(fileName)

		if err != nil {
			log.Printf("error creating archive %v", err)
			addErrorToOutput(response, *object.Name, err)

			continue
		}

		if _, err := io.Copy(w1, getObjRes.Content); err != nil {
			log.Printf("error creating archive %v", err)
			addErrorToOutput(response, *object.Name, err)

			continue
		}

		// closing zip archive
		zipWriter.Close()

		// upload zip to another bucket
		err = ociClient.PutObject(request.Namespace, request.TargetBucketName, *object.Name+".zip", ioutil.NopCloser(buf))
		if err != nil {
			log.Printf("error uploading archive %v", err)
			addErrorToOutput(response, *object.Name, err)

			continue
		}

		log.Print("Upload finished.")

		// delete original file
		err = ociClient.DeleteObject(request.Namespace, request.SourceBucketName, *object.Name)
		if err != nil {
			log.Printf("error deleting file %v", err)
			addErrorToOutput(response, *object.Name, err)

			continue
		}

		log.Print("Original file deleted.")

		response.ProcessedWithSuccess = append(response.ProcessedWithSuccess, *object.Name)
	}

	log.Print("Finished to process files.")

	return response
}

func addErrorToOutput(response *Response, objectName string, err error) {
	response.ProcessedWithError = append(response.ProcessedWithError, Errors{
		Name:  objectName,
		Error: fmt.Sprintf("error deleting file %v", err),
	})
}