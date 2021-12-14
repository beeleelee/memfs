// Package kvdbfs implements an file system with Bazil FUSE
// kvdbfs use kvdb as its base storage
package kvdbfs

import (
	"os"
	"sync"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	"github.com/bbengfort/sequence"
	logging "github.com/ipfs/go-log/v2"
)

var logger = logging.Logger("kvdbfs")

//===========================================================================
// New MemFS File System
//===========================================================================

// New MemFS file system created from a mount path and a configuration. This
// is the entry point for creating and launching all in-memory file systems.
func New(mount string, config *Config) *FileSystem {
	// Set the Log Level
	if config.LogLevel != "" {
		logging.SetLogLevelRegex("^kvdbfs", config.LogLevel)
	}

	// Create the file system
	fs := new(FileSystem)
	fs.MountPoint = mount
	fs.Config = config
	fs.Sequence, _ = sequence.New()

	// Set the UID and GID of the file system
	fs.uid = uint32(os.Geteuid())
	fs.gid = uint32(os.Getegid())

	// Set other system flags from the configuration
	fs.readonly = fs.Config.ReadOnly

	// Create the root directory
	fs.root = NewDir("/", 0755, nil, fs)

	// Return the file system
	return fs
}

//===========================================================================
// File System Struct
//===========================================================================

// FileSystem implements the fuse.FS* interfaces as well as providing a
// lockable interaction structure to ensure concurrent accesses succeed.
type FileSystem struct {
	sync.Mutex                    // FileSystem can be locked and unlocked
	MountPoint string             // Path to the mount location on disk
	Config     *Config            // Configuration of the FileSystem
	Conn       *fuse.Conn         // Hook to the FUSE connection object
	Sequence   *sequence.Sequence // Monotonically increasing counter for inodes
	root       *Node              // The root of the file system
	uid        uint32             // The user id of the process running the file system
	gid        uint32             // The group id of the process running the file system
	nfiles     uint64             // The number of files in the file system
	ndirs      uint64             // The number of directories in the file system
	nbytes     uint64             // The amount of data in the file system
	readonly   bool               // If the file system is readonly or not
}

// Run the FileSystem, mounting the MountPoint and connecting to FUSE
func (mfs *FileSystem) Run() error {
	var err error

	// Unmount the FS in case it was mounted with errors.
	fuse.Unmount(mfs.MountPoint)

	// Create the mount options to pass to Mount.
	opts := []fuse.MountOption{
		fuse.VolumeName("KVDBFS"),
		fuse.FSName("kvdbfs"),
		fuse.Subtype("kvdbfs"),
	}

	// If we're in readonly mode - pass to the mount options
	if mfs.readonly {
		opts = append(opts, fuse.ReadOnly())
	}

	// Mount the FS with the specified options
	if mfs.Conn, err = fuse.Mount(mfs.MountPoint, opts...); err != nil {
		return err
	}

	// Ensure that the file system is shutdown
	defer mfs.Conn.Close()
	logger.Infof("mounted memfs:// on %s", mfs.MountPoint)

	// Serve the file system
	if err = fs.Serve(mfs.Conn, mfs); err != nil {
		return err
	}

	logger.Info("post serve")

	// Check if the mount process has an error to report
	<-mfs.Conn.Ready
	if mfs.Conn.MountError != nil {
		return mfs.Conn.MountError
	}

	return nil
}

// Shutdown the FileSystem unmounting the MountPoint and disconnecting FUSE.
func (mfs *FileSystem) Shutdown() error {
	logger.Info("shutting the file system down gracefully")

	if mfs.Conn == nil {
		return nil
	}

	if err := fuse.Unmount(mfs.MountPoint); err != nil {
		return err
	}

	return nil
}

//===========================================================================
// Implement fuse.FS* Methods
//===========================================================================

// Root is called to obtain the Node for the file system root. Implements the
// fuse.FS interface required of all file systems.
func (mfs *FileSystem) Root() (fs.Node, error) {
	return mfs.root, nil
}

// Destroy is called when the file system is shutting down. Implements the
// fuse.FSDestroyer interface.
//
// Linux only sends this request for block device backed (fuseblk)
// filesystems, to allow them to flush writes to disk before the
// unmount completes.
func (mfs *FileSystem) Destroy() {
	logger.Info("file system is being destroyed")
}

// GenerateInode is called to pick a dynamic inode number when it
// would otherwise be 0. Implements the fuse.FSInodeGenerator interface.
//
// Not all filesystems bother tracking inodes, but FUSE requires
// the inode to be set, and fewer duplicates in general makes UNIX
// tools work better.
//
// Operations where the nodes may return 0 inodes include Getattr,
// Setattr and ReadDir.
//
// If FS does not implement FSInodeGenerator, GenerateDynamicInode
// is used.
//
// Implementing this is useful to e.g. constrain the range of
// inode values used for dynamic inodes.
func (mfs *FileSystem) GenerateInode(parentInode uint64, name string) uint64 {
	return fs.GenerateDynamicInode(parentInode, name)
}
