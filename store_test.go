package main

import (
	"testing"
)

func TestPutAndGet(t *testing.T) {
	kvs, err := NewKeyValueStore()
	if err != nil {
		t.Fatalf("Failed to create KeyValueStore: %s", err)
	}

	// Test Put
	err = kvs.Put("key1", "value1")
	if err != nil {
		t.Errorf("Failed to put key-value pair: %s", err)
	}

	// Test Get
	value, err := kvs.Get("key1")
	if err != nil {
		t.Errorf("Failed to get value for key: %s", err)
	} else if value != "value1" {
		t.Errorf("Expected value 'value1', got '%s'", value)
	}
}

func TestDelete(t *testing.T) {
	kvs, err := NewKeyValueStore()
	if err != nil {
		t.Fatalf("Failed to create KeyValueStore: %s", err)
	}

	// Put a key-value pair
	err = kvs.Put("key1", "value1")
	if err != nil {
		t.Fatalf("Failed to put key-value pair: %s", err)
	}

	// Test Delete
	err = kvs.Delete("key1")
	if err != nil {
		t.Errorf("Failed to delete key-value pair: %s", err)
	}

	// Ensure the key is deleted
	_, err = kvs.Get("key1")
	if err == nil {
		t.Errorf("Expected key 'key1' to be deleted, but found value")
	}
}

func TestReplicate(t *testing.T) {
	// Create the original node
	originalNode, err := NewKeyValueStore()
	if err != nil {
		t.Fatalf("Failed to create original KeyValueStore: %s", err)
	}

	// Put some key-value pairs in the original node
	err = originalNode.Put("key1", "value1")
	if err != nil {
		t.Fatalf("Failed to put key-value pair in original node: %s", err)
	}

	err = originalNode.Put("key2", "value2")
	if err != nil {
		t.Fatalf("Failed to put key-value pair in original node: %s", err)
	}

	// Create the new node
	newNode, err := NewKeyValueStore()
	if err != nil {
		t.Fatalf("Failed to create new KeyValueStore: %s", err)
	}

	// Replicate the data from the original node to the new node
	err = originalNode.Replicate(newNode)
	if err != nil {
		t.Fatalf("Failed to replicate key-value pairs: %s", err)
	}

	// Verify that the new node has the replicated key-value pairs
	value, err := newNode.Get("key1")
	if err != nil {
		t.Errorf("Failed to get replicated value for key 'key1': %s", err)
	} else if value != "value1" {
		t.Errorf("Expected replicated value 'value1' for key 'key1', got '%s'", value)
	}

	value, err = newNode.Get("key2")
	if err != nil {
		t.Errorf("Failed to get replicated value for key 'key2': %s", err)
	} else if value != "value2" {
		t.Errorf("Expected replicated value 'value2' for key 'key2', got '%s'", value)
	}
}

