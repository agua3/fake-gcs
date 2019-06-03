package fakestorage

import (
	"encoding/json"
	"net/http"

	"github.com/gorilla/mux"
)

// ObjectACL represents the object access control
// that is stored within the fake server.
type ObjectACL struct {
	BucketName string `json:"bucket"`
	ObjectName string `json:"object"`
}

func (o *ObjectACL) id() string {
	return o.BucketName + "/" + o.ObjectName
}

// ListObjectACLs List ACLs of an object
func (s *Server) ListObjectACLs(
	bucketName, objectName string,
) ([]ObjectACL, error) {
	obj, err := s.backend.GetObject(bucketName, objectName)
	if err != nil {
		return nil, err
	}

	objACL := ObjectACL{
		BucketName: obj.BucketName,
		ObjectName: obj.Name,
	}

	return []ObjectACL{objACL}, nil
}

func (s *Server) listObjectACLs(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	bucketName := vars["bucketName"]
	objectName := vars["objectName"]

	encoder := json.NewEncoder(w)

	objACLs, err := s.ListObjectACLs(bucketName, objectName)
	if err != nil {
		w.WriteHeader(http.StatusNotFound)
		errResp := newErrorResponse(http.StatusNotFound, "Not Found", nil)
		encoder.Encode(errResp)
		return
	}

	encoder.Encode(newListObjectACLsResponse(objACLs))
}
