package protobuf

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"

	"github.com/jhump/protoreflect/desc/protoparse"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/dynamicpb"
)

// RegistriesFromMap attempts to parse a map of filenames (relative to import
// directories) and their contents out into a registry of protobuf files and
// protobuf types. These registries can then be used as a mechanism for
// dynamically (un)marshalling the definitions within.
func RegistriesFromMap(filesMap map[string]string) (*protoregistry.Files, *protoregistry.Types, error) {
	var parser protoparse.Parser
	parser.Accessor = protoparse.FileContentsFromMap(filesMap)

	names := make([]string, 0, len(filesMap))
	for k := range filesMap {
		names = append(names, k)
	}

	fds, err := parser.ParseFiles(names...)
	if err != nil {
		return nil, nil, err
	}

	files, types := &protoregistry.Files{}, &protoregistry.Types{}
	for _, v := range fds {
		if err := files.RegisterFile(v.UnwrapFile()); err != nil {
			return nil, nil, fmt.Errorf("failed to register file '%v': %w", v.GetName(), err)
		}
		for _, t := range v.GetMessageTypes() {
			if err := types.RegisterMessage(dynamicpb.NewMessageType(t.UnwrapMessage())); err != nil {
				return nil, nil, fmt.Errorf("failed to register type '%v': %w", t.GetName(), err)
			}
			for _, nt := range t.GetNestedMessageTypes() {
				if err := types.RegisterMessage(dynamicpb.NewMessageType(nt.UnwrapMessage())); err != nil {
					return nil, nil, fmt.Errorf("failed to register type '%v': %w", nt.GetFullyQualifiedName(), err)
				}
			}
		}
	}
	return files, types, nil
}

// loadDescriptors parses every .proto file under importPaths.
func loadDescriptors(f fs.FS, importPaths []string) (*protoregistry.Files, *protoregistry.Types, error) {
	files := map[string]string{}
	for _, importPath := range importPaths {
		if err := fs.WalkDir(f, importPath, func(path string, info fs.DirEntry, ferr error) error {
			if ferr != nil || info.IsDir() {
				return ferr
			}
			if filepath.Ext(info.Name()) == ".proto" {
				rPath, ferr := filepath.Rel(importPath, path)
				if ferr != nil {
					return fmt.Errorf("failed to get relative path: %v", ferr)
				}
				content, ferr := os.ReadFile(path)
				if ferr != nil {
					return fmt.Errorf("failed to read import %v: %v", path, ferr)
				}
				files[rPath] = string(content)
			}
			return nil
		}); err != nil {
			return nil, nil, err
		}
	}
	return RegistriesFromMap(files)
}
