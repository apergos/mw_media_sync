NOTE: placeholder with all stubs until it gets written.
NO USABLE CODE HERE YET.

Intro
=====

What is this? Keep a local copy of media on Wikimedia projects
in sync on your local server, deleting stuff that's no longer
on the remote and downloading new stuff.

This script passes over all active projects; you can't tell
it to just do your favorites right now.

This script won't check to see if files on Commons are more
recent than the local copies you have; this means you might
have old versions. To be fixed in the future.

Setup
=====

Copy the file sync.conf.sample into sync.conf and edit with
the appropriate values.

Make sure you have LOTS of storage available for storing media.

Run the script with --help to get usage instructions.

Start with a low value for downloads, to test that it works the
way you expect.

Please be extra polite with this script; let's not have piles
of downloaders all trying to grab all the media. Better is to
have one mirror grab and make available to everyone else (and
that's indeed the plan).

