package main

import (
	"log"
	"net/http"
	"os"
	"path/filepath"
)

func main() {
	root := env("STATIC_ROOT", "/usr/share/nginx/html")
	addr := env("HTTP_ADDR", ":80")
	files := http.FileServer(http.Dir(root))

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		path := filepath.Join(root, filepath.Clean(r.URL.Path))
		if info, err := os.Stat(path); err == nil && !info.IsDir() {
			files.ServeHTTP(w, r)
			return
		}
		http.ServeFile(w, r, filepath.Join(root, "index.html"))
	})

	log.Printf("static frontend listening on %s, root=%s", addr, root)
	log.Fatal(http.ListenAndServe(addr, nil))
}

func env(key string, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}
