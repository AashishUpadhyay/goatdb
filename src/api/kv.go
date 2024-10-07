package api

import (
	"log"
	"net/http"

	"github.com/gorilla/mux"
)

type KV struct {
	Logger *log.Logger
}

func (kv KV) RegisterRoutes(r mux.Router) {
	r.HandleFunc("/v1/kv", kv.handle)
}

func (kv KV) handle(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodPost {
		kv.post(w, r)
	}

	if r.Method == http.MethodGet {
		kv.get(w, r)
	}

	kv.Logger.Printf("Method is : " + r.Method)
	http.Error(w, http.StatusText(http.StatusMethodNotAllowed), http.StatusMethodNotAllowed)
}

func (kv KV) post(w http.ResponseWriter, r *http.Request) {

}

func (kv KV) get(w http.ResponseWriter, r *http.Request) {

}
