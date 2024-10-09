package api

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/AashishUpadhyay/goatdb/src/db"
	"github.com/gorilla/mux"
)

const version = "1.0.0"

type config struct {
	port int
	env  string
}

var cfg config

func Index() {

	flag.IntVar(&cfg.port, "port", 9999, "API Server Port")
	flag.StringVar(&cfg.env, "env", "dev", "Environment")
	flag.Parse()

	logger := log.New(os.Stdout, "", log.Ldate|log.Ltime)
	addr := fmt.Sprintf(":%d", cfg.port)

	router := mux.NewRouter()
	router.HandleFunc("/v1/hc", healthcheck)

	kvc := &KVController{
		Logger: logger,
		Db: db.NewDb(db.Options{
			MemtableThreshold: 100000,
			SstableMgr: db.SSTableFileSystemManager{
				DataDir: "/Users/aashishupadhyay/Code/goatdb/.tmp/sstables/",
				Logger:  logger,
			},
			Logger: logger,
		}),
	}

	kvc.RegisterRoutes(router)

	srv := &http.Server{
		Addr:         addr,
		Handler:      router,
		IdleTimeout:  time.Minute,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
	}

	logger.Printf("starting %s server on %s", cfg.env, addr)
	err := srv.ListenAndServe()
	if err != nil {
		logger.Fatal(err)
	}
}

func healthcheck(w http.ResponseWriter, r *http.Request) {
	logger := log.New(os.Stdout, "", log.Ldate|log.Ltime)
	logger.Printf("healthcheck called!")

	if r.Method != http.MethodGet {
		http.Error(w, http.StatusText(http.StatusMethodNotAllowed), http.StatusMethodNotAllowed)
		return
	}

	returnVal := map[string]string{
		"status":      "available",
		"environment": cfg.env,
		"version":     version,
	}

	returnValJson, err := json.MarshalIndent(returnVal, "", "\t")
	if err != nil {
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}

	returnValJson = append(returnValJson, '\n')
	w.Header().Set("Content-Type", "application/json")
	w.Write(returnValJson)
	logger.Printf("request successful!")
}
