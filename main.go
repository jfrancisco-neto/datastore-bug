package main

import (
	"context"
	"flag"
	"fmt"
	"time"

	"cloud.google.com/go/datastore"
	"github.com/google/uuid"
)

type Entity interface {
	GetId() *datastore.Key
}

type ItemModel struct {
	Id           *datastore.Key `datastore:"__key__"`
	CreatedAt    time.Time      `datastore:"created_at"`
	Email        string         `datastore:"email,noindex"`
	UuidValue    string         `datastore:"uuid_value,noindex"`
	BooleanValue bool           `datastore:"boolean_value,noindex"`
}

func (m *ItemModel) GetId() *datastore.Key {
	return m.Id
}

func create_item(id string, email string, kind string, namespace string) *ItemModel {
	return &ItemModel{
		Id: &datastore.Key{
			Kind:      kind,
			Name:      id,
			Namespace: namespace,
		},
		CreatedAt:    time.Now().AddDate(0, 0, -1),
		Email:        email,
		UuidValue:    uuid.NewString(),
		BooleanValue: true,
	}
}

func create_entities(count int, kind, namespace string) []Entity {
	list := make([]Entity, 0, count*2)

	for i := 1; i <= count; i++ {
		id := fmt.Sprintf("id%v", i)
		email := fmt.Sprintf("%s@email.com", id)
		list = append(list, create_item(id, email, kind, namespace))
	}

	return list
}

func fillEnities(ctx context.Context, count int, client *datastore.Client, kind, namepsace string) {
	limit := 500

	entities := create_entities(count, kind, namepsace)

	keys := func(entities []Entity) []*datastore.Key {
		keys := make([]*datastore.Key, 0, len(entities))
		for _, i := range entities {
			keys = append(keys, i.GetId())
		}

		return keys
	}(entities)

	if len(keys) != len(entities) {
		panic("invalid size")
	}

	for index := 0; index < len(keys); index += limit {
		index2 := index + limit
		if index2 > len(keys) {
			index2 = len(keys)
		}
		if _, err := client.PutMulti(ctx, keys[index:index2], entities[index:index2]); err != nil {
			panic(err)
		}
	}
}

func queryCount(ctx context.Context, dsClient *datastore.Client, kind, namespace string) {
	query := datastore.NewQuery(kind).Namespace(namespace).KeysOnly().NewAggregationQuery().WithCount("count")

	result, err := dsClient.RunAggregationQuery(ctx, query)
	if err != nil {
		panic(err)
	}

	fmt.Println("Count", result["count"])
}

func main() {
	var projectName string
	var namespace string
	var kind string

	flag.StringVar(&projectName, "project_name", "my_project", "")
	flag.StringVar(&namespace, "namespace", "my_namespace", "")
	flag.StringVar(&kind, "kind", "item_kind", "")

	flag.Parse()

	if projectName == "" {
		panic("projectName is empty")
	}

	if namespace == "" {
		panic("namespace is empty")
	}

	if kind == "" {
		panic("kind is empty")
	}

	ctx := context.Background()
	dsClient, err := datastore.NewClient(ctx, projectName)
	if err != nil {
		panic(err)
	}

	fillEnities(ctx, 10, dsClient, kind, namespace)
	queryCount(ctx, dsClient, kind, namespace)
}
