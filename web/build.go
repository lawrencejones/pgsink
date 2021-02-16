package web

import (
	"embed"
	"io/fs"
	"net/http"
	"os"
	"path"
)

//go:embed build/*
var Assets embed.FS

// Pre-flight check. If we haven't built the assets, or they are missing for some reason,
// then we want to break the build. This prevents accidentally shipping a binary that has
// no website.
//
// Only run this when the GORELEASER environment variable is set, so we only break release
// builds.
func init() {
	if os.Getenv("GORELEASER") != "" {
		_, err := Assets.Open("build/index.html")
		if err != nil {
			panic("failed to open build/index.html: have we built assets with 'yarn build'?")
		}
	}
}

type fsFunc func(name string) (fs.File, error)

func (f fsFunc) Open(name string) (fs.File, error) {
	return f(name)
}

// AssetHandler returns an http.Handler that will serve files from the Assets embed.FS.
// When locating a file, it will strip the given prefix from the request and prepend the
// root to the filesystem lookup: typical prefix might be /web/, and root would be build.
func AssetHandler(prefix, root string) http.Handler {
	handler := fsFunc(func(name string) (fs.File, error) {
		assetPath := path.Join(root, name)

		// If we can't find the asset, return the default index.html content
		f, err := Assets.Open(assetPath)
		if os.IsNotExist(err) {
			return Assets.Open("build/index.html")
		}

		// Otherwise assume this is a legitimate request routed correctly
		return f, err
	})

	return http.StripPrefix(prefix, http.FileServer(http.FS(handler)))
}
