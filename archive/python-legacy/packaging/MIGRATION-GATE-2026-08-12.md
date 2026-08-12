# Python Legacy Split Gate - 2026-08-12

Status: external gate.

Why the tree stays here:

- No external archive repository or destination URL is configured.
- The refocus task forbids replacing the source tree with a large in-repo zip or
  bundle copy.
- Recovery must stay possible from the current working tree without inventing a
  remote target.

Prepared locally:

- `manifest-2026-08-12.tsv` lists every file under `archive/python-legacy/`
  except `packaging/`, with byte length and SHA-256.

Verified on 2026-08-12:

- Manifest rows: 366
- Covered source files outside `packaging/`: 366
- Covered source bytes outside `packaging/`: 6,861,100
- Files inside `packaging/`: 2
- Total files now present under `archive/python-legacy/`: 368

Replay check:

- Import the manifest as TSV with tab delimiters.
- Recompute each covered file's size and SHA-256 from the live tree.
- Refuse any split/export if counts, paths, sizes, or hashes differ.

Required before R6 can complete:

1. Create or designate the external archive repository.
2. Export `archive/python-legacy/` from the live tree into that destination.
3. Verify the exported file set against `manifest-2026-08-12.tsv`.
4. Only after that verification, replace the live-tree copy with a minimal
   pointer README in a follow-up change.
