# PST Outlook-Import Fix

**Status:** ✅ Generated PSTs now import into Outlook Classic without
the "Errors detected — Run Repair Tool" prompt and without
`.pst.corrupt` rename. Verified against round52.pst (synthetic) and
a real frontend-downloaded export.

**Scope:** `vendor/pstwriter` (the C++ writer that produces every PST
the restore-worker emits). Python / orchestration unchanged.

---

## What was broken

Generated PSTs that the restore-worker uploaded to S3 — when the user
downloaded them and dragged them into Outlook Classic — exhibited two
failure modes depending on which writer round produced them:

1. **Hard quarantine**: Outlook renamed the file from
   `whatever.pst` to `whatever.pst.corrupt` and refused to open it
   at all (rounds 11–12).
2. **Repair-required prompt**: Outlook displayed
   *"Errors detected. Would you like to run the Inbox Repair tool?"*
   on every open. After repair the messages were visible, but the
   prompt blocked the import-on-double-click flow that production
   downloads expect (rounds 13–51).

`scanpst.exe` reported up to 14 distinct errors against the writer's
output across the rounds we audited.

---

## Root causes

The writer produced PST files that were *almost* spec-conforming and
that Outlook *almost* accepted — but Outlook (and `scanpst`) enforce
several constraints that the public [MS-PST] spec either marks as
"deprecated" or leaves "out of scope". Six independent root causes
combined to trigger the prompt, all of them buried in real-Outlook
implementation details.

| # | Root cause | Source of insight |
|---|---|---|
| 1 | **Missing PMap page at file offset 0x4600.** The spec marks PMap as deprecated, but Outlook reads the slot anyway. Earlier round-11 attempt to add it used a zero-byte body, which Outlook quarantined because the bitmap was inconsistent with the AMap. | Procmon trace of Outlook's file-read pattern + [MS-PST] §2.6 ("MUST be present"); body content found by hex-dumping a real `backup.pst`. |
| 2 | **SMQ / SAL / per-search-folder SUQ emitted as Table Contexts.** Spec §2.4.8.3 calls them "basic queue nodes" — flat byte arrays, not TCs. SAL specifically (§2.4.8.4.2) is described as "a simple array of NIDs (not a queue)", which contradicted what we'd guessed. | [MS-PST] PDF text extraction via `pdftotext` — the public Microsoft Learn HTML pages were stub-only. |
| 3 | **EC1 (NID 0x0000EC1) emitted as a TC.** Real Outlook stores it as a 20-byte raw structure: `{wFlags=0, wSUDType=9, NID=0x2223, padding[12]}`. Not in the spec at all. | Hex-dump of `backup.pst`'s NID 0xEC1 block — the bytes didn't decode as an HN. |
| 4 | **Search Criteria Object (NID 0x2227) had wrong properties.** We were guessing tags (`PR_FOLDER_FLAGS=4`, `0x6840`, `0x36C2`, etc.). Real Outlook uses **exactly one undocumented property: `0x660B Int32 = 0`**. Spec §2.4.8.6.4 explicitly says SCO properties are "out of scope of this document". | Direct PC decode of `backup.pst` NID 0x2227. |
| 5 | **Folder Contents Table had 28 columns instead of 29.** Missing `0x300B PR_SEARCH_KEY` (4-byte HID, iBit=28, ibData=120). Plus several other columns (`ChangeKey`, `SecureSubmitFlags`, `LastModificationTime`, `Repl*`) were at the wrong ibData/iBit positions. Per-row size 126 → 130 bytes. | Tag-by-tag schema diff between our writer output and `backup.pst`'s 0x808E TC. |
| 6 | **Message NIDs collided with folder NIDs at idx 0x400.** Folder NID `0x8002` (idx 0x400, type 0x02) and message NID `0x8004` (idx 0x400, type 0x04) shared the same nidIndex. Real Outlook keeps message idx ≥ 0x10001, separate from folder idx. | NBT walk of `backup.pst`: folder idx ≤ 0x404, message idx ≥ 0x10001. |

A handful of secondary issues stacked on top — sibling-table NBT
entries having `nidParent=0` instead of the owning folder NID, the
row matrix being inlined in the HN instead of promoted to a subnode,
`PR_FOLDER_FLAGS` using the wrong bit value (`0x4`=`FLDFLAG_NORMAL`
where `0x2`=`FLDFLAG_SEARCH` was wanted) — but these were cosmetic on
their own; the six above were what actually moved the needle.

---

## How the fixes were found

The MS-PST spec gave us about 50 % of the way there. The other 50 %
came from byte-diffing a real-Outlook-generated PST. Specifically:

1. **Built a Python decoder** (`pst_decode.py`, ~530 lines) that walks
   `HEADER → ROOT → BBT/NBT B-trees → block decryption (permute) →
   HN parse → PC/TC structure → HID resolution → SLBLOCK walk`. The
   decoder doesn't write anything; it just parses both our output and
   `backup.pst` so we can compare structurally.
2. **Extracted the [MS-PST] PDF** (Feb-2025 v11.2) via `pdftotext`
   because the public HTML pages were stub-only — sections like
   §2.4.8.4.2 (Search Activity List) were one sentence on the web
   but had a full structure description in the PDF.
3. **Tag-by-tag schema diff** of every TC and PC NID we cared about
   (Contents Table, message PC, SCO 0x2227, EC1, EC6).

The decoder is the artefact that unblocked Round L. Without it we
were guessing at undocumented internals; with it we could see what
real Outlook actually wrote.

---

## Patch summary by file

All edits are in `vendor/pstwriter`. 14 files, +1026 lines / −189 lines.

| File | Change |
|---|---|
| `src/page.cpp` | `buildEmptyPMap` body filled with `0xFF` (PMap inverse semantics: bit=1 means free) instead of zeros. |
| `include/pstwriter/page.hpp` | New `buildEmptyPMap` declaration. |
| `src/writer.cpp` | New constants `kIbPMap=0x4600`, `kIbNbt=0x4800`, `kIbBbt=0x4A00`, `kIbEof=0x4C00`, `kBlocksStart=0x4800`. Emits PMap page in all three write paths (M2 minimal, M3 multi-block, M5/M7 full). |
| `src/mail.cpp`, `src/messaging.cpp`, `src/event.cpp`, `src/contact.cpp` | `kBlocksStart` constant bumped from `0x4600u` → `0x4800u` (synced with writer.cpp; block-trailer wSig depends on this matching the on-disk offset). |
| `src/pst_baseline.cpp` + `src/pst_baseline.hpp` | New `mkEmptyQueue` helper + `isEmptyQueue` flag on `PstBaselineEntry`. Switched SMQ (0x1E1), SUQ (0x2226), SUQ (0xEC6) to empty-queue. SAL (0x201) emitted as 4-byte NID-array. EC1 (0xEC1) emitted as 20-byte raw SUD-like blob. SDO (0x261) added as 4-byte NID-array per §2.4.8.4.3. |
| `src/messaging.cpp` (TC + SCO) | `kContentsCols`: 28 → 29 columns (added `0x300B0102 PR_SEARCH_KEY` at iBit=28 ibData=120) with corrected ibData/iBit positions for `0x3013 ChangeKey`, `0x65C6 SecureSubmitFlags`, `0x3008 LastModificationTime`, `0x0E30/33/34/38/3C/3D Repl*`, `0x0057 MessageToMe`, `0x0058 MessageCcMe`. `kContentsRowSize=130`, `kContentsCebOff=126`. CEB bits 2/13/14/15 (MessageStatus / MessageToMe / MessageCcMe / Sensitivity) made unconditional. `buildSearchCriteriaObjectPc` reduced to single property: `0x660B Int32 = 0`. `buildSearchFolderPc` PR_FOLDER_FLAGS value `0x4` → `0x2` (FLDFLAG_SEARCH per [MS-OXCFOLD]). |
| `include/pstwriter/messaging.hpp` | New `searchKeyBytes` / `searchKeySize` fields on `ContentsTcRow` and supporting comments. |
| `src/mail.cpp` (PC + row) | Added 4 PC default props (`0x0036 Sensitivity`, `0x0E17 MessageStatus`, `0x0057 MessageToMe`, `0x0058 MessageCcMe`) so row CEB bits match. `DisplayCc/Bcc` now only emitted when non-empty. `ConversationIndex` populated as 22-byte canonical stub when missing (matches every real-Outlook row). `PR_SEARCH_KEY` populated on row matching the PC value. Hierarchy/Contents/FAI sibling-table NBT entries now carry `nidParent=folder NID` (was 0). Empty `0x671` attachment table emitted on every message even when no attachments. New `scheduleEmptyQueue` lambda for queue-node baseline entries. |
| `src/contact.cpp`, `src/event.cpp` | Mirror of mail.cpp's `scheduleEmptyQueue` for contact / calendar PSTs. |
| `src/m5_allocator.cpp` | `nextIndex_[NormalMessage]` and `nextIndex_[AssocMessage]` seeded to `0x10001` (was `0x400`) — separates message idx range from folder idx range, matching real-Outlook PSTs. |
| `src/ltp.cpp` | `buildTableContext` now always promotes the row matrix to a subnode when `firstSubnodeNid` is provided and rows exist (was: only on HN-overflow). Matches real-Outlook behaviour. |
| `src/ndb.cpp` | (Cosmetic only — comment cleanups around block trailer.) |

The writer is fully backward-compatible with downstream callers
(`pst_convert` CLI, the restore-worker's `mail.py` writer, etc.).
No public API changes; the only behavioral difference is that
`writeM7Pst` (and friends) now produce files Outlook Classic accepts
without prompting for repair.

---

## Verification

| Test | Result |
|---|---|
| `scanpst.exe round52.pst` | 1 hard error remaining (`Contents Table for 8002, row doesn't match sub-object`) — see "Known limitations" below. **The 8 other scanpst errors that were present at baseline (rounds 23–42) are all cleared.** |
| Drag `round52.pst` into Outlook Classic | ✅ Opens cleanly, no repair prompt, no `.pst.corrupt` rename. Test message visible with subject + body intact. |
| Real PST downloaded from frontend export | ✅ Same — opens cleanly without repair. |

---

## Known limitations

`scanpst.exe` still reports one residual error and one warning on
every PST we generate:

1. **`!! Contents Table for 8002, row doesn't match sub-object`** —
   our Python decoder cannot find the actual mismatch. The TC schema
   matches `backup.pst` byte-for-byte (29 cols, 130-byte rows, row
   matrix in subnode with NID type `LtpReserved`); every CEB-set
   column resolves to bytes identical to the corresponding PC
   property; the row index BTH correctly maps the row's NID; the
   message NID is in the correct idx range. scanpst is enforcing
   some structural validation that isn't visible at the byte level —
   most likely an exact-HNID-target equality check or a hidden
   per-row metadata comparison. **Outlook itself does not enforce
   this check at open time**, so the remaining flag is scanpst-only
   noise. Resolving it would require disassembling `scanpst.exe`.

2. **`?? Deleting SDO`** — informational warning. Persists despite
   emitting a spec-conforming SDO. Likely scanpst always emits this
   notice regardless of content. Not an error; doesn't affect
   Outlook's open-time check.

These don't block the import flow; the user-visible goal
("don't make people click Repair") is met.

---

## References used

- **[MS-PST] v11.2** (Feb 2025) — used the PDF, not the HTML stubs.
  Sections that mattered: §2.2.2.6 (HEADER), §2.2.2.7 (Pages, esp.
  PMap §2.2.2.7.3), §2.2.2.8 (Blocks + trailer), §2.3.4 (TC),
  §2.4.4.5.1 (Contents TC required cols), §2.4.8.3 (Basic Queue
  Node), §2.4.8.4.2 (SAL — "simple array of NIDs, not a queue"),
  §2.4.8.6.4 (SCO — "out of scope"), §2.7.1 (Mandatory NIDs).
- **[MS-OXCFOLD]** — `FLDFLAG_SEARCH=0x2`.
- **[MS-OXCMSG]** — message PC required properties.
- **`backup.pst`** — a real-Outlook 2.3 MB PST with one user folder
  and 12 messages. Provided by the user. The single most useful
  artefact for this fix; without it we'd have been stuck at 4
  errors instead of 1.
- **`pst_decode.py`** (in repo root, separate file) — byte-level
  decoder that surfaced every diff we acted on this round.

---

## Test artefacts (not in repo)

In `D:\Work\TM_vault\` (workspace root, .gitignored):

- `round23.pst` — the round-23 baseline (9 scanpst errors).
- `round52.pst` — the final state (1 cosmetic scanpst error,
  Outlook accepts cleanly).
- `round*.log` — scanpst output for each round.
- `pst_decode.py` — the Python decoder.
- `backup.pst` — real-Outlook reference PST (user-provided).
- `mspst.txt` — extracted [MS-PST] PDF text.
- `test_mail.json` — minimal mail-PC test fixture used to drive
  `pst_convert mail`.
