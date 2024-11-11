package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/AashishUpadhyay/goatdb/src/db"
	"github.com/gorilla/mux"
	"github.com/stretchr/testify/mock"
)

func TestKVController(t *testing.T) {
	t.Run("test_post_valid_kv", func(t *testing.T) {
		mockDb := new(MockDB)
		mockDb.On("Put", mock.Anything).Return(nil)
		logger := log.New(os.Stdout, "", log.Ldate|log.Ltime)
		kvc := KVController{Logger: logger, Db: mockDb}

		url := "v1/kv"
		reqBody := strings.NewReader("{\"key\":\"asdf\", \"value\":\"asdf\"}")

		w := httptest.NewRecorder()
		r, _ := http.NewRequest(http.MethodPost, url, reqBody)

		kvc.Post(w, r)
		if w.Code != http.StatusCreated {
			t.Errorf("expected status code %d, got %d", http.StatusCreated, w.Code)
		}
	})

	t.Run("test_post_invalid_json", func(t *testing.T) {
		mockDb := new(MockDB)
		mockDb.On("Put", mock.Anything).Return(nil)
		logger := log.New(os.Stdout, "", log.Ldate|log.Ltime)
		kvc := KVController{Logger: logger, Db: mockDb}

		url := "v1/kv"
		reqBody := strings.NewReader("{\"key\":\"asdf\", \"value\":\"asdf\"")

		w := httptest.NewRecorder()
		r, _ := http.NewRequest(http.MethodPost, url, reqBody)

		kvc.Post(w, r)
		if w.Code != http.StatusBadRequest {
			t.Errorf("expected status code %d, got %d", http.StatusBadRequest, w.Code)
		}
	})

	t.Run("test_post_empty_body", func(t *testing.T) {
		mockDb := new(MockDB)
		mockDb.On("Put", mock.Anything).Return(nil)
		logger := log.New(os.Stdout, "", log.Ldate|log.Ltime)
		kvc := KVController{Logger: logger, Db: mockDb}

		url := "v1/kv"
		reqBody := strings.NewReader("")

		w := httptest.NewRecorder()
		r, _ := http.NewRequest(http.MethodPost, url, reqBody)

		kvc.Post(w, r)
		if w.Code != http.StatusBadRequest {
			t.Errorf("expected status code %d, got %d", http.StatusBadRequest, w.Code)
		}
	})

	t.Run("test_post_DB_error", func(t *testing.T) {
		mockDb := new(MockDB)
		mockDb.On("Put", mock.Anything).Return(errors.New("failed to save!"))

		logger := log.New(os.Stdout, "", log.Ldate|log.Ltime)

		kvc := KVController{Logger: logger, Db: mockDb}

		url := "v1/kv"
		reqBody := strings.NewReader("{\"key\":\"asdf\", \"value\":\"asdf\"}")

		w := httptest.NewRecorder()
		r, _ := http.NewRequest(http.MethodPost, url, reqBody)

		kvc.Post(w, r)
		if w.Code != http.StatusInternalServerError {
			t.Errorf("expected status code %d, got %d", http.StatusInternalServerError, w.Code)
		}
	})

	t.Run("test_get_returns_kv", func(t *testing.T) {
		key := "asdf"
		mockDb := new(MockDB)
		mockDb.On("Get", mock.Anything).Return(db.Entry{
			Key:   "asdf",
			Value: []byte("asdf"),
		})
		logger := log.New(os.Stdout, "", log.Ldate|log.Ltime)
		kvc := KVController{Logger: logger, Db: mockDb}
		url := fmt.Sprintf("v1/kv/%s", key)
		r, _ := http.NewRequest(http.MethodGet, url, nil)
		vars := map[string]string{
			"key-name": key,
		}
		r = mux.SetURLVars(r, vars)

		w := httptest.NewRecorder()
		kvc.Get(w, r)
		if w.Code != http.StatusOK {
			t.Errorf("expected status code %d, got %d", http.StatusOK, w.Code)
		}

		kvWanted := KV{
			Key:   key,
			Value: key,
		}
		responseWanted, _ := json.MarshalIndent(kvWanted, "", "\t")
		responseJsonWanted := string(responseWanted)

		if w.Body.String() != responseJsonWanted {
			t.Errorf("expected body %q, got %q", responseJsonWanted, w.Body.String())
		}
	})

	t.Run("test_get_returns_error_when_failed_to_fetch_value", func(t *testing.T) {
		key := "asdf"
		mockDb := new(MockDB)
		mockDb.On("Get", mock.Anything).Return(errors.New("An error occurred when trying to get the value"))
		logger := log.New(os.Stdout, "", log.Ldate|log.Ltime)
		kvc := KVController{Logger: logger, Db: mockDb}
		url := fmt.Sprintf("v1/kv/%s", key)
		r, _ := http.NewRequest(http.MethodGet, url, nil)
		vars := map[string]string{
			"key-name": key,
		}
		r = mux.SetURLVars(r, vars)

		w := httptest.NewRecorder()
		kvc.Get(w, r)
		if w.Code != http.StatusInternalServerError {
			t.Errorf("expected status code %d, got %d", http.StatusInternalServerError, w.Code)
		}
	})
}

func TestFail(t *testing.T) {
	t.Errorf("This is a test failure")
}

type MockDB struct {
	mock.Mock
}

func (mdb *MockDB) Get(key string) (db.Entry, error) {
	args := mdb.Called()
	if kvRetrieved, ok := args.Get(0).(db.Entry); ok {
		return db.Entry{
			Key:   kvRetrieved.Key,
			Value: []byte(kvRetrieved.Value),
		}, nil
	}

	if args.Error(0) != nil {
		return db.Entry{}, args.Error(0)
	}

	return db.Entry{}, nil
}

func (mdb *MockDB) Put(entry db.Entry) error {
	args := mdb.Called(entry)
	if args.Error(0) != nil {
		return args.Error(0)
	}
	return nil
}
