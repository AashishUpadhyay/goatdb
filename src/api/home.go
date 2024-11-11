package api

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/AashishUpadhyay/goatdb/src/db"
	"github.com/gorilla/mux"
)

const version = "1.0.0"

type config struct {
	port              int
	env               string
	memtableThreshold int
	dataDir           string
}

var cfg config

func Index() {
	// Set default from env var or fallback
	defaultEnv := os.Getenv("ENV")
	env := "dev" // default value
	if defaultEnv != "" {
		defaultEnv = env
	}

	defaultDataDir := os.Getenv("DATA_DIR")
	if defaultDataDir == "" {
		defaultDataDir = "app/sstables/"
	}

	defaultMemtableThreshold := os.Getenv("MEMTABLE_THRESHOLD")
	if defaultMemtableThreshold == "" {
		defaultMemtableThreshold = "100"
	}

	defaultPort := os.Getenv("PORT")
	if defaultPort == "" {
		defaultPort = "9999"
	}

	flag.StringVar(&cfg.env, "env", defaultEnv, "Environment")
	flag.StringVar(&cfg.dataDir, "data-dir", defaultDataDir, "Data directory for SSTable storage")

	memThreshold, _ := strconv.Atoi(defaultMemtableThreshold)
	flag.IntVar(&cfg.memtableThreshold, "memtable-threshold", memThreshold, "Memtable threshold")

	portNum, _ := strconv.Atoi(defaultPort)
	flag.IntVar(&cfg.port, "port", portNum, "API Server Port")
	flag.Parse()

	logger := log.New(os.Stdout, "", log.Ldate|log.Ltime)
	addr := fmt.Sprintf(":%d", cfg.port)

	router := mux.NewRouter()
	router.HandleFunc("/v1/hc", healthcheck)
	router.HandleFunc("/", serveIndex)

	// Add this line to serve static files
	router.PathPrefix("/static/").Handler(http.StripPrefix("/static/", http.FileServer(http.Dir("static"))))

	kvc := &KVController{
		Logger: logger,
		Db: db.NewDb(db.Options{
			MemtableThreshold: cfg.memtableThreshold,
			SstableMgr: db.SSTableFileSystemManager{
				DataDir: cfg.dataDir,
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

func serveIndex(w http.ResponseWriter, r *http.Request) {
	http.ServeFile(w, r, "./static/index.html")
}
