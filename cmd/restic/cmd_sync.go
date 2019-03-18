package main

import (
	"os"
	"sync"
	"time"

	"github.com/restic/restic/internal/archiver"
	"github.com/restic/restic/internal/debug"
	"github.com/restic/restic/internal/errors"
	"github.com/restic/restic/internal/filter"
	"github.com/restic/restic/internal/fs"
	"github.com/restic/restic/internal/restic"
	"github.com/restic/restic/internal/restorer"
	"github.com/restic/restic/internal/ui"
	"github.com/restic/restic/internal/ui/termstatus"
	tomb "gopkg.in/tomb.v2"

	"github.com/spf13/cobra"
)

var cmdSync = &cobra.Command{
	Use:   "sync [flags] snapshotID",
	Short: "Syncronize the target directory with the data from a snapshot",
	Long: `
The "sync" command syncronizes the data from a snapshot from the repository to
a directory.

WARNING: Files in the target directory that are not in the snapshot will be deleted.
`,
	DisableAutoGenTag: true,
	RunE: func(cmd *cobra.Command, args []string) error {
		var t tomb.Tomb
		term := termstatus.New(globalOptions.stdout, globalOptions.stderr, globalOptions.Quiet)
		t.Go(func() error { term.Run(t.Context(globalOptions.ctx)); return nil })

		err := runSync(syncOptions, globalOptions, term, args)
		if err != nil {
			return err
		}
		t.Kill(nil)
		return t.Wait()

	},
}

// SyncOptions collects all options for the restore command.
type SyncOptions struct {
	Include  []string
	Target   string
	Host     string
	Paths    []string
	Tags     restic.TagLists
	Verify   bool
	Progress bool
}

var syncOptions SyncOptions

func init() {
	cmdRoot.AddCommand(cmdSync)

	flags := cmdSync.Flags()
	flags.StringVarP(&syncOptions.Target, "target", "t", "", "directory to extract data to")

	flags.StringVarP(&syncOptions.Host, "host", "H", "", `only consider snapshots for this host when the snapshot ID is "latest"`)
	flags.Var(&syncOptions.Tags, "tag", "only consider snapshots which include this `taglist` for snapshot ID \"latest\"")
	flags.StringArrayVar(&syncOptions.Paths, "path", nil, "only consider snapshots which include this (absolute) `path` for snapshot ID \"latest\"")
	flags.BoolVar(&syncOptions.Verify, "verify", false, "verify restored files content")
	flags.BoolVar(&syncOptions.Progress, "progress", false, "report progress while doing restore.")
}

type FileTracker struct {
	sync.Mutex
	FilesToGet    []string
	FilesToDelete []string
}

func (ft *FileTracker) CompleteItem(item string, previous, current *restic.Node, s archiver.ItemStats, d time.Duration) {
	if current == nil {
		return
	}
	Printf("Item: %s Current: %+v Prevous: %+v\n", item, current, previous)
	if current.Type == "file" {
		ft.Lock()
		if previous == nil {
			Printf("Deleting: %s\n", item)
			// No previous file on record, this is a new file we should delete
			ft.FilesToDelete = append(ft.FilesToDelete, item)
		} else if !previous.Equals(*current) {
			Printf("Getting: %s\n", item)
			// We are different, we should restore this file.
			ft.FilesToGet = append(ft.FilesToGet, item)
		}
		ft.Unlock()
	}
}

func runSync(opts SyncOptions, gopts GlobalOptions, term *termstatus.Terminal, args []string) error {
	ctx := gopts.ctx

	fileTracker := FileTracker{}

	switch {
	case len(args) == 0:
		return errors.Fatal("no snapshot ID specified")
	case len(args) > 1:
		return errors.Fatalf("more than one snapshot ID specified: %v", args)
	}

	if opts.Target == "" {
		return errors.Fatal("please specify a directory to restore to (--target)")
	}

	targets, err := filterExisting([]string{opts.Target})
	if err != nil {
		return errors.Fatalf("error filtering targets: %v", err)
	}

	snapshotIDString := args[0]

	var t tomb.Tomb

	debug.Log("restore %v to %v", snapshotIDString, opts.Target)

	repo, err := OpenRepository(gopts)
	if err != nil {
		return err
	}

	p := ui.NewBackup(term, gopts.verbosity)

	// use the terminal for stdout/stderr
	prevStdout, prevStderr := gopts.stdout, gopts.stderr
	defer func() {
		gopts.stdout, gopts.stderr = prevStdout, prevStderr
	}()
	gopts.stdout, gopts.stderr = p.Stdout(), p.Stderr()
	t.Go(func() error { return p.Run(t.Context(gopts.ctx)) })

	if !gopts.NoLock {
		lock, err := lockRepo(repo)
		defer unlockRepo(lock)
		if err != nil {
			return err
		}
	}

	err = repo.LoadIndex(ctx)
	if err != nil {
		return err
	}

	var id restic.ID

	if snapshotIDString == "latest" {
		id, err = restic.FindLatestSnapshot(ctx, repo, opts.Paths, opts.Tags, opts.Host)
		if err != nil {
			Exitf(1, "latest snapshot for criteria not found: %v Paths:%v Host:%v", err, opts.Paths, opts.Host)
		}
	} else {
		id, err = restic.FindSnapshot(repo, snapshotIDString)
		if err != nil {
			Exitf(1, "invalid id %q: %v", snapshotIDString, err)
		}
	}

	selectByNameFilter := func(item string) bool {
		return true
	}

	selectFilter := func(item string, fi os.FileInfo) bool {
		return true
	}

	scanError := func(item string, fi os.FileInfo, err error) error {
		Printf("Item: %s FI: %+v Err: %v\n", item, fi, err)
		return err
	}
	scanResult := func(item string, s archiver.ScanStats) {
		Printf("Item: %s Stats: %+v\n", item, s)

	}

	var targetFS fs.FS = fs.Local{}
	sc := archiver.NewScanner(targetFS)
	sc.SelectByName = selectByNameFilter
	sc.Select = selectFilter
	sc.Error = scanError
	sc.Result = scanResult

	Verbosef("start scan on %v\n", targets)
	t.Go(func() error { return sc.Scan(t.Context(gopts.ctx), targets) })

	arch := archiver.New(repo, targetFS, archiver.Options{})
	arch.SelectByName = selectByNameFilter
	arch.Select = selectFilter
	arch.WithAtime = false
	arch.Error = p.Error
	arch.CompleteItem = fileTracker.CompleteItem
	arch.StartFile = p.StartFile
	arch.CompleteBlob = p.CompleteBlob
	arch.NopUpload = true

	snapshotOpts := archiver.SnapshotOptions{
		Time:           time.Now(),
		Hostname:       opts.Host,
		ParentSnapshot: id,
	}

	if !gopts.JSON {
		p.V("start backup on %v", targets)
	}
	_, _, err = arch.Snapshot(gopts.ctx, targets, snapshotOpts)
	if err != nil {
		return errors.Fatalf("unable to save snapshot: %v", err)
	}
	p.V("Waiting to finish")

	// The following code is verbatim the code from cmd_restore.

	res, err := restorer.NewRestorer(repo, id)
	if err != nil {
		Exitf(2, "creating restorer failed: %v\n", err)
	}

	totalErrors := 0
	res.Error = func(location string, err error) error {
		Warnf("ignoring error for %s: %s\n", location, err)
		totalErrors++
		return nil
	}

	// No files modified, and none to delete.  try to get everything.
	if len(fileTracker.FilesToGet) == 0 && len(fileTracker.FilesToDelete) == 0 {
		res.SelectFilter = func(item string, dstpath string, node *restic.Node) (selectedForRestore bool, childMayBeSelected bool) {
			return true, true
		}
	} else {
		res.SelectFilter = func(item string, dstpath string, node *restic.Node) (selectedForRestore bool, childMayBeSelected bool) {
			matched, childMayMatch, err := filter.List(fileTracker.FilesToGet, item)
			if err != nil {
				Warnf("error for include pattern: %v", err)
			}

			selectedForRestore = matched
			childMayBeSelected = (childMayMatch) && node.Type == "dir"
			Printf("Restoring: %s %s %v\n", item, dstpath, matched)

			return selectedForRestore, childMayBeSelected
		}
	}

	Verbosef("restoring %s to %s\n", res.Snapshot(), opts.Target)

	if opts.Progress {
		res.Progress = func(totalBytes, bytesSoFar uint64) {
			PrintProgress("PROGRESS: %f %d %d", float64(bytesSoFar)/float64(totalBytes), bytesSoFar, totalBytes)
		}
	}

	err = res.RestoreTo(ctx, opts.Target)
	if err == nil && opts.Verify {
		Verbosef("verifying files in %s\n", opts.Target)
		var count int
		count, err = res.VerifyFiles(ctx, opts.Target)
		Verbosef("finished verifying %d files in %s\n", count, opts.Target)
	}
	if totalErrors > 0 {
		Printf("There were %d errors\n", totalErrors)
	}
	p.Finish(id)
	p.V("Files To Get: %v", fileTracker.FilesToGet)
	p.V("Files To Delete: %v", fileTracker.FilesToDelete)

	return err
}
