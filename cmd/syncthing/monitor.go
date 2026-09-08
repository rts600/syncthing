// Copyright (C) 2014 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

package main

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// monitorMain is retained for compatibility but immediately delegates to syncthingMain
// without spawning any child/monitor processes.
func (c *serveCmd) monitorMain() {
	c.syncthingMain()
}

// rotatedFile keeps a set of rotating logs. There will be the base file plus up
// to maxFiles rotated ones, each ~ maxSize bytes large.
type rotatedFile struct {
	name        string
	create      createFn
	maxSize     int64 // bytes
	maxFiles    int
	currentFile io.WriteCloser
	currentSize int64
}

type createFn func(name string) (io.WriteCloser, error)

func newRotatedFile(name string, create createFn, maxSize int64, maxFiles int) (*rotatedFile, error) {
	var size int64
	if info, err := os.Lstat(name); err != nil {
		if !os.IsNotExist(err) {
			return nil, err
		}
		size = 0
	} else {
		size = info.Size()
	}
	writer, err := create(name)
	if err != nil {
		return nil, err
	}
	return &rotatedFile{
		name:        name,
		create:      create,
		maxSize:     maxSize,
		maxFiles:    maxFiles,
		currentFile: writer,
		currentSize: size,
	}, nil
}

func (r *rotatedFile) Write(bs []byte) (int, error) {
	// Check if we're about to exceed the max size, and if so close this
	// file so we'll start on a new one.
	if r.currentSize+int64(len(bs)) > r.maxSize {
		r.currentFile.Close()
		r.currentSize = 0
		r.rotate()
		f, err := r.create(r.name)
		if err != nil {
			return 0, err
		}
		r.currentFile = f
	}

	n, err := r.currentFile.Write(bs)
	r.currentSize += int64(n)
	return n, err
}

func (r *rotatedFile) rotate() {
	// The files are named "name", "name.0", "name.1", ...
	// "name.(r.maxFiles-1)". Increase the numbers on the
	// suffixed ones.
	for i := r.maxFiles - 1; i > 0; i-- {
		from := numberedFile(r.name, i-1)
		to := numberedFile(r.name, i)
		err := os.Rename(from, to)
		if err != nil && !os.IsNotExist(err) {
			fmt.Println("LOG: Rotating logs:", err)
		}
	}

	// Rename the base to base.0
	err := os.Rename(r.name, numberedFile(r.name, 0))
	if err != nil && !os.IsNotExist(err) {
		fmt.Println("LOG: Rotating logs:", err)
	}
}

// numberedFile adds the number between the file name and the extension.
func numberedFile(name string, num int) string {
	ext := filepath.Ext(name) // contains the dot
	withoutExt := name[:len(name)-len(ext)]
	return fmt.Sprintf("%s.%d%s", withoutExt, num, ext)
}

// An autoclosedFile is an io.WriteCloser that opens itself for appending on
// Write() and closes itself after an interval of no writes (closeDelay) or
// when the file has been open for too long (maxOpenTime). A call to Write()
// will return any error that happens on the resulting Open() call too. Errors
// on automatic Close() calls are silently swallowed...
type autoclosedFile struct {
	name        string        // path to write to
	closeDelay  time.Duration // close after this long inactivity
	maxOpenTime time.Duration // or this long after opening

	fd         io.WriteCloser // underlying WriteCloser
	opened     time.Time      // timestamp when the file was last opened
	closed     chan struct{}  // closed on Close(), stops the closerLoop
	closeTimer *time.Timer    // fires closeDelay after a write

	mut sync.Mutex
}

func newAutoclosedFile(name string, closeDelay, maxOpenTime time.Duration) (*autoclosedFile, error) {
	f := &autoclosedFile{
		name:        name,
		closeDelay:  closeDelay,
		maxOpenTime: maxOpenTime,
		closed:      make(chan struct{}),
		closeTimer:  time.NewTimer(time.Minute),
	}
	f.mut.Lock()
	defer f.mut.Unlock()
	if err := f.ensureOpenLocked(); err != nil {
		return nil, err
	}
	go f.closerLoop()
	return f, nil
}

func (f *autoclosedFile) Write(bs []byte) (int, error) {
	f.mut.Lock()
	defer f.mut.Unlock()

	// Make sure the file is open for appending
	if err := f.ensureOpenLocked(); err != nil {
		return 0, err
	}

	// If we haven't run into the maxOpenTime, postpone close for another
	// closeDelay
	if time.Since(f.opened) < f.maxOpenTime {
		f.closeTimer.Reset(f.closeDelay)
	}

	return f.fd.Write(bs)
}

func (f *autoclosedFile) Close() error {
	f.mut.Lock()
	defer f.mut.Unlock()

	// Stop the timer and closerLoop() routine
	f.closeTimer.Stop()
	close(f.closed)

	// Close the file, if it's open
	if f.fd != nil {
		return f.fd.Close()
	}

	return nil
}

// Must be called with f.mut held!
func (f *autoclosedFile) ensureOpenLocked() error {
	if f.fd != nil {
		// File is already open
		return nil
	}

	// We open the file for write only, and create it if it doesn't exist.
	flags := os.O_WRONLY | os.O_CREATE | os.O_APPEND

	fd, err := os.OpenFile(f.name, flags, 0o666)
	if err != nil {
		return err
	}

	f.fd = fd
	f.opened = time.Now()
	return nil
}

func (f *autoclosedFile) closerLoop() {
	for {
		select {
		case <-f.closeTimer.C:
			// Close the file when the timer expires.
			f.mut.Lock()
			if f.fd != nil {
				f.fd.Close() // errors, schmerrors
				f.fd = nil
			}
			f.mut.Unlock()

		case <-f.closed:
			return
		}
	}
}
