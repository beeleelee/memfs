package kvdbfs

import (
	"os"
	"path/filepath"
	"time"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	"golang.org/x/net/context"
)

// Init the directory with the required properties for the directory.
func NewDir(name string, mode os.FileMode, parent *Node, memfs *FileSystem) *Node {
	// Make sure the mode is a directory, then init the node.
	mode = os.ModeDir | mode
	d := NewNode(name, mode, parent, memfs)
	// Make the children mapping
	d.Children = make(map[string]*Node)
	d.Ents = make(map[string]*fuse.Dirent)
	return d
}

//===========================================================================
// Dir fuse.Node* Interface
//===========================================================================

// Create creates and opens a new file in the receiver, which must be a Dir.
// NOTE: the interface docmentation says create a directory, but the docs
// for fuse.CreateRequest say create and open a file (not a directory).
//
// https://godoc.org/bazil.org/fuse/fs#NodeCreater
func (d *Node) Create(ctx context.Context, req *fuse.CreateRequest, resp *fuse.CreateResponse) (fs.Node, fs.Handle, error) {
	if d.fs.readonly {
		return nil, nil, fuse.EPERM
	}

	d.fs.Lock()
	defer d.fs.Unlock()

	// Update the directory Atime
	d.Attrs.Atime = time.Now()

	// Create the file
	f := NewFile(req.Name, req.Mode, d, d.fs)

	// Set the file's UID and GID to that of the caller
	f.Attrs.Uid = req.Header.Uid
	f.Attrs.Gid = req.Header.Gid

	// Add the file to the directory
	d.Children[f.Name] = f
	d.Ents[f.Name] = &fuse.Dirent{
		Inode: f.Attrs.Inode,
		Name:  f.Name,
		Type:  f.FuseType(),
	}

	// Update the directory Mtime
	d.Attrs.Mtime = time.Now()

	// Update the file system state
	d.fs.nfiles++
	d.sync()
	d.fs.sync()
	// Log the file creation and return the file, which is both node and handle.
	logger.Infof("create %q in %q, mode %v", f.Name, d.Path(), req.Mode)
	return f, f, nil
}

// Link creates a new directory entry in the receiver based on an
// existing Node. Receiver must be a directory.
//
// A LinkRequest is a request to create a hard link and contains the old node
// ID and the NewName (a string), the old node is supplied to the server.
//
// https://godoc.org/bazil.org/fuse/fs#NodeLinker
// TODO: Implement
// func (d *Dir) Link(ctx context.Context, req *fuse.LinkRequest, old Node) (fs.Node, error) {
// 	return nil, nil
// }

// Mkdir creates (but not opens) a directory in the given directory.
//
// https://godoc.org/bazil.org/fuse/fs#NodeMkdirer
func (d *Node) Mkdir(ctx context.Context, req *fuse.MkdirRequest) (fs.Node, error) {
	if d.fs.readonly {
		return nil, fuse.EPERM
	}

	d.fs.Lock()
	defer d.fs.Unlock()

	// Update the directory Atime
	d.Attrs.Atime = time.Now()

	// TODO: Allow for the creation of archive directories

	// Create the child directory
	c := NewDir(req.Name, req.Mode, d, d.fs)

	// Set the directory's UID and GID to that of the caller
	c.Attrs.Uid = req.Header.Uid
	c.Attrs.Gid = req.Header.Gid

	// Add the directory to the directory
	d.Children[c.Name] = c
	d.Ents[c.Name] = &fuse.Dirent{
		Inode: c.Attrs.Inode,
		Name:  c.Name,
		Type:  c.FuseType(),
	}

	// Update the directory Mtime
	d.Attrs.Mtime = time.Now()

	// Update the file system state
	d.fs.ndirs++
	d.sync()
	d.fs.sync()
	// Log the directory creation and return the dir node
	logger.Infof("mkdir %q in %q, mode %v", c.Name, d.Path(), req.Mode)
	return c, nil
}

// Mknode I assume creates but not opens a node and returns it.
//
// https://godoc.org/bazil.org/fuse/fs#NodeMknoder
// TODO: Implement
// func (d *Dir) Mknod(ctx context.Context, req *fuse.MknodRequest) (fs.Node, error) {
//     return nil, nil
// }

// Remove removes the entry with the given name from the receiver, which must
// be a directory.  The entry to be removed may correspond to a file (unlink)
// or to a directory (rmdir).
//
// https://godoc.org/bazil.org/fuse/fs#NodeRemover
func (d *Node) Remove(ctx context.Context, req *fuse.RemoveRequest) error {
	if d.fs.readonly {
		return fuse.EPERM
	}

	d.fs.Lock()
	defer d.fs.Unlock()

	// Update the directory Atime
	d.Attrs.Atime = time.Now()

	var ent *Node
	var ok bool

	// Get the node from the directory by name.
	if ent, ok = d.Children[req.Name]; !ok {
		logger.Debugf("(error) could not find node to remove named %q in %q", req.Name, d.Path())
		return fuse.EEXIST
	}

	// Do not remove a directory that contains files.
	if ent.IsDir() && len(ent.Children) > 0 {
		logger.Debugf("(error) will not remove non-empty directory %q in %q", req.Name, d.Path())
		return fuse.EIO
	}

	// Delete the entry from the directory Children
	delete(d.Children, req.Name)
	delete(d.Ents, req.Name)

	// Update the directory Mtime
	d.Attrs.Mtime = time.Now()

	// Update the file system state
	if ent.IsDir() {
		d.fs.ndirs--
	} else {
		d.fs.nfiles--
	}
	d.sync()
	d.fs.sync()
	// Log the directory removal and return no error
	logger.Infof("removed %q from %q", req.Name, d.Path())
	return nil
}

// Rename a file in a directory. NOTE: There is no documentation on this.
// Implemented to move the entry by name from the dir to the newDir.
//
// https://godoc.org/bazil.org/fuse/fs#NodeRenamer
func (d *Node) Rename(ctx context.Context, req *fuse.RenameRequest, newDir fs.Node) error {
	if d.fs.readonly {
		return fuse.EPERM
	}

	d.fs.Lock()
	defer d.fs.Unlock()

	// Update the directory Atime
	d.Attrs.Atime = time.Now()

	var dst *Node
	var ok bool
	var node *Node

	// Convert newDir to an actual Dir object
	if dst, ok = newDir.(*Node); !ok || !dst.IsDir() {
		logger.Debugf("(error) could not convert %q to a dir", newDir)
		return fuse.EEXIST
	}

	// Update the dst directory Atime
	dst.Attrs.Atime = time.Now()

	// Get the child entity from the directory
	if node, ok = d.Children[req.OldName]; !ok {
		logger.Debugf("(error) could not find %q in %q to move", req.OldName, d.Path())
		return fuse.EEXIST
	}

	// Get the node from the entity and update attrs.
	node.Name = req.NewName
	node.Attrs.Mtime = time.Now()

	dst.Children[req.NewName] = node // Add the entity to the new directory
	dst.Ents[req.NewName] = &fuse.Dirent{
		Inode: node.Attrs.Inode,
		Name:  node.Name,
		Type:  node.FuseType(),
	}
	dst.Attrs.Mtime = time.Now()

	delete(dst.Children, req.OldName) // Delete the entity from the old directory
	delete(dst.Ents, req.OldName)
	d.Attrs.Mtime = time.Now()
	d.sync()
	node.sync()
	d.fs.sync()
	logger.Infof("moved %q from %q to %q", req.OldName, d.Path(), node.Path())
	return nil
}

// Lookup looks up a specific entry in the receiver,
// which must be a directory.  Lookup should return a Node
// corresponding to the entry.  If the name does not exist in
// the directory, Lookup should return ENOENT.
//
// Lookup need not to handle the names "." and "..".
//
// https://godoc.org/bazil.org/fuse/fs#NodeStringLookuper
// NOTE: implemented NodeStringLookuper rather than NodeRequestLookuper
// https://godoc.org/bazil.org/fuse/fs#NodeRequestLookuper
func (d *Node) Lookup(ctx context.Context, name string) (fs.Node, error) {

	d.fs.Lock()
	defer d.fs.Unlock()

	// Update the directory Atime
	d.Attrs.Atime = time.Now()

	if ent, ok := d.Children[name]; ok {
		logger.Infof("lookup %s in %s", name, d.Path())
		return ent, nil
	}

	if ent, ok := d.Ents[name]; ok {
		if ndata, err := d.fs.kv.Get(filepath.Join(d.Path(), ent.Name)); err == nil {
			if n, err := NodeFromBytes(ndata, d, d.fs); err == nil {
				d.Children[ent.Name] = n
				return n, nil
			}
		}
		if ent.Type == fuse.DT_Dir {
			n := NewDir(ent.Name, 0755, d, d.fs)
			d.Children[ent.Name] = n
			return n, nil
		}
		n := NewFile(ent.Name, 0644, d, d.fs)
		d.Children[ent.Name] = n
		return n, nil
	}

	logger.Debugf("(error) couldn't lookup %s in %s", name, d.Path())
	return nil, fuse.ENOENT
}

// Symlink creates a new symbolic link in the receiver, which must be a directory.
// TODO is the above true about directories?
//
// https://godoc.org/bazil.org/fuse/fs#NodeSymlinker
// TODO: Implement
// func (d *Dir) Symlink(ctx context.Context, req *fuse.SymlinkRequest) (Node, error) {
//     return nil, fuse.EEXIST
// }

//===========================================================================
// Dir fuse.Handle* Interface
//===========================================================================

// ReadDirAll reads the entire directory contents and returns a list of fuse
// Dirent objects - which specify the internal contents of the directory.
//
// https://godoc.org/bazil.org/fuse/fs#HandleReadDirAller
func (d *Node) ReadDirAll(ctx context.Context) ([]fuse.Dirent, error) {
	contents := make([]fuse.Dirent, 0, len(d.Children))

	d.fs.Lock()
	defer d.fs.Unlock()

	// Set the access time
	d.Attrs.Atime = time.Now()

	// Create the Dirent response
	// for _, node := range d.Children {
	// 	dirent := fuse.Dirent{
	// 		Inode: node.Attrs.Inode,
	// 		Type:  node.FuseType(),
	// 		Name:  node.Name,
	// 	}

	// 	contents = append(contents, dirent)
	// }

	for _, ent := range d.Ents {
		contents = append(contents, *ent)
	}

	logger.Infof("read all for directory %s, ents: %v", d.Path(), contents)
	return contents, nil
}
