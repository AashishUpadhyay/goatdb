package api

import (
	"encoding/json"
	"io"
	"log"
	"net/http"

	"github.com/AashishUpadhyay/goatdb/src/db"
	"github.com/gorilla/mux"
)

type KVController struct {
	Logger *log.Logger
	Db     db.DB
}

type KV struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

func (kvc KVController) RegisterRoutes(r *mux.Router) {
	r.HandleFunc("/v1/kv/{key-name}", kvc.Get)
	r.HandleFunc("/v1/kv", kvc.Post)
}

func (kvc KVController) Post(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, http.StatusText(http.StatusBadRequest), http.StatusBadRequest)
		return
	}

	kv := &KV{}
	err = json.Unmarshal(body, &kv)

	if err != nil {
		http.Error(w, http.StatusText(http.StatusBadRequest), http.StatusBadRequest)
		return
	}

	err = kvc.Db.Put(db.Entry{
		Key:   kv.Key,
		Value: []byte(kv.Value),
	})

	if err != nil {
		kvc.Logger.Printf("Failed to create the KV with key %s. error : %v", kv.Key, err)
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}

	kvc.Logger.Printf("Successfully created the KV with key %s.", kv.Key)
	w.WriteHeader(http.StatusCreated)
}

func (kvc KVController) Get(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	keyName := vars["key-name"]

	retrievedEntry, err := kvc.Db.Get(keyName)

	// Test for errors in retrieving the entry
	if err != nil {
		kvc.Logger.Printf("Failed to get the key %s. error : %v", keyName, err)
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}

	kv := &KV{
		Key:   retrievedEntry.Key,
		Value: string(retrievedEntry.Value),
	}

	kvjson, err := json.MarshalIndent(kv, "", "\t")
	if err != nil {
		kvc.Logger.Printf("Failed to serialize response!")
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}

	kvc.Logger.Printf("Found key %s!", kv.Key)
	w.Header().Set("Content-Type", "application/json")
	w.Write(kvjson)
}
