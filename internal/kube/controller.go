package kube

/*
import (
	"context"
	"encoding/json"
	"fmt"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"
)

// CRD
var monboDBResource = schema.GroupVersionResource{Group: "mongodbcommunity.mongodb.com", Version: "v1", Resource: "mongodbcommunity"}

// ScaleMongoDB changes the number of members by the given proportion,
// which should be 0 =< proportion < 1.
func ScaleMongoDB(ctx context.Context, client dynamic.Interface, name string, namespace string, proportion uint) error {
	if proportion > 1 {
		return fmt.Errorf("proportion should be between 0 =< proportion < 1")
	}

	mongoDBClient := client.Resource(monboDBResource).Namespace(namespace)
	mdb, err := mongoDBClient.Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return err
	}

	members, found, err := unstructured.NestedInt64(mdb.UnstructuredContent(), "spec", "members")
	if err != nil {
		return err
	}

	if !found {
		return fmt.Errorf("members field not found on MongoDB spec")
	}

	scaled := int(members) * (1 + int(proportion))

	patch := []interface{}{
		map[string]interface{}{
			"op":    "replace",
			"path":  "/spec/members",
			"value": scaled,
		},
	}

	payload, err := json.Marshal(patch)
	if err != nil {
		return err
	}

	_, err = mongoDBClient.Patch(ctx, name, types.JSONPatchType, payload, metav1.PatchOptions{})
	if err != nil {
		return err
	}

	return nil
}

*/
