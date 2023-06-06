package main

import (
	"context"
	"fmt"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type KeyValueStore struct {
	collection *mongo.Collection
	lock       sync.Mutex
}

type KeyValuePair struct {
	Key   string `bson:"key"`
	Value string `bson:"value"`
}

func NewKeyValueStore() (*KeyValueStore, error) {
	client, err := mongo.NewClient(options.Client().ApplyURI("mongodb://localhost:27017"))
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err = client.Connect(ctx)
	if err != nil {
		return nil, err
	}

	collection := client.Database("distributed_kv_store").Collection("key_value_pairs")

	return &KeyValueStore{
		collection: collection,
		lock:       sync.Mutex{},
	}, nil
}

func (kvs *KeyValueStore) Put(key, value string) error {
	kvs.lock.Lock()
	defer kvs.lock.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	filter := bson.M{"key": key}
	update := bson.M{"$set": bson.M{"value": value}}
	updateOpts := options.FindOneAndUpdate().SetUpsert(true)

	var result KeyValuePair
	err := kvs.collection.FindOneAndUpdate(ctx, filter, update, updateOpts).Decode(&result)
	if err != nil && err != mongo.ErrNoDocuments {
		return fmt.Errorf("failed to put key-value pair: %s", err)
	}

	return nil
}

func (kvs *KeyValueStore) Get(key string) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	filter := bson.M{"key": key}
	var result KeyValuePair

	err := kvs.collection.FindOne(ctx, filter).Decode(&result)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return "", fmt.Errorf("key '%s' not found", key)
		}
		return "", fmt.Errorf("failed to get value for key '%s': %s", key, err)
	}

	return result.Value, nil
}

func (kvs *KeyValueStore) Delete(key string) error {
	kvs.lock.Lock()
	defer kvs.lock.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	filter := bson.M{"key": key}
	result, err := kvs.collection.DeleteOne(ctx, filter)
	if err != nil {
		return fmt.Errorf("failed to delete key-value pair: %s", err)
	}

	if result.DeletedCount == 0 {
		return fmt.Errorf("key not found")
	}

	return nil
}

func (kvs *KeyValueStore) Replicate(newNode *KeyValueStore) error {
	// Create a cursor to iterate over all documents in the current node's collection
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	cursor, err := kvs.collection.Find(ctx, bson.M{})
	if err != nil {
		return fmt.Errorf("failed to retrieve key-value pairs: %s", err)
	}
	defer cursor.Close(ctx)

	// Iterate over the cursor and insert documents into the new node's collection
	for cursor.Next(ctx) {
		var kv KeyValuePair
		if err := cursor.Decode(&kv); err != nil {
			return fmt.Errorf("failed to decode key-value pair: %s", err)
		}

		// Append "new" to the original key for the replicated key-value pair
		newKey := kv.Key + "_new"

		// Insert the key-value pair with the new key into the new node's collection
		if err := newNode.Put(newKey, kv.Value); err != nil {
			return fmt.Errorf("failed to replicate key-value pair: %s", err)
		}
	}

	if err := cursor.Err(); err != nil {
		return fmt.Errorf("cursor error while retrieving key-value pairs: %s", err)
	}

	return nil
}
