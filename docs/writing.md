# Writing documents for this repository

Optimize for how fast a reader understands on the first pass, not for
completeness. The first draft of the 2.0 design was cut to about a fifth of its
length and carried more information afterwards; what went was dilution.

## Format

Markdown is pleasant to write and an agent writes these documents now, so ease
of writing is not the constraint. Ease of *reading* is. HTML carries diagrams,
tables that hold their shape, and cross-links; Markdown flattens all three.

**HTML** for the detail level: every subsystem design under `docs/design`.

**Markdown** where it earns its place:

- `AGENTS.md` and the files it routes to at the top level, because they are the
  first thing read and have to stay small
- anything a build step parses — `docs/design/capabilities.md` is read by
  `sbt checkCapabilityIndex`, so its tables have to stay machine-readable
- anything appended to and reviewed by diff, such as the decision log in
  section 6 of `docs/design/README.md`

**Never both.** A document exists in one form. An HTML twin of a Markdown file
is two copies of one fact, and the two will disagree.

Mechanics for the HTML files:

- Start with `<meta charset="utf-8">`. Without it the file renders as mojibake
  when opened from disk, which is how most people will read it.
- Self-contained: no external scripts, stylesheets or fonts. A design document
  has to open from a checkout with no network.
- Diagrams are hand-authored inline SVG. No ASCII art, no image files, no
  diagramming library. The diagram rules below apply unchanged.
- Style it for reading: one column, a readable measure, real `<table>` markup
  for anything tabular, and anchors on the section headings so other documents
  can link to a specific part.

## Structure

1. **Essence first.** The first screen states the thing in one sentence. The
   rest of the document is that sentence expanded. Not background, then
   solution, then detail.
2. **Each section opens with its claim.** The mechanism and the details are the
   evidence for it. Never derive from mechanism to conclusion and leave the
   claim in the last paragraph for the reader to extract.
3. **Select ruthlessly.** Only what a reader needs to understand the design.
   Background is one sentence. Post-mortems, validation matrices, future work
   and "confirmed over several rounds with X" are process, not deliverable; give
   them their own file if they are needed at all.
4. **Main line first.** Walk the path that works, then name the exceptions and
   account for their cost in one place. Do not interleave counter-evidence into
   a forward argument. Anything deferred must be picked back up explicitly.

## Sentences

- **Numbers lead.** "Network cost is 27.36% of the AWS bill, 190.5k USD a
  month", not "network cost is high, roughly 190k".
- **Blockers and gaps are stated outright.** "Tencent Cloud has no test
  environment, so QA cannot run end to end and this is not delivered" beats "in
  progress".
- **An intuition the data contradicts is the most valuable thing you can
  write** — as a statement, not as a story about being surprised.
- **Explain to the depth the reader can act on.** An existing algorithm is a
  black box: say a deterministic algorithm exists and how the new field enters
  it. Do not unfold its internals.
- **Concrete examples beat abstract mechanism.** Walk one real address, one real
  row. Before-and-after beats a single diagram of the after.

## Diagrams

Use vector graphics, never ASCII art. A diagram carries structure; the prose
carries only the causality a picture cannot show, and never repeats the picture.

- Lines are semantic. One encoding per diagram, colour and dash meaning one
  thing each. If the lines speak for themselves, delete the legend.
- Colour encodes grouping, not component type. Identity goes in the box text.
  Row and tier labels are plain text; a box reads as a component.
- Colour only what this diagram argues about and grey out the rest. Delete
  anything not on that argument, even if it is real.
- Prefer no line to a line that detours or crosses. Hierarchy is not an arrow;
  arrows mean traffic or calls.
- Draw real instances, not one representative. Two availability zones means two
  drawn.
- A diagram embedded in a document must not fill a screen. Turn a tall chain
  sideways.

## Prohibited

- **Jargon without a definition.** Any abbreviation or domain term gets one
  sentence the first time it appears.
- **Metaphors**, including ones that sound precise. Plain language is not a
  metaphor. Use definitions, not analogies. Official terms keep their original
  names; components keep their formal names.
- **AI register**: reversal constructions ("not X but Y", "more than just"),
  posture headings ("from X to Y"), explicit labels like "key insight", opening
  three-part summaries, and packaging labels invented on the spot ("two
  corollaries:", "three points to close:"). Write the content or delete it.
- **Guide phrases** — "first let us establish two facts", "below we will see".
  The structure leads; it does not need a narrator.
- **Repetition.** What a diagram shows is not restated in a table. Two phrasings
  of the same point keep one. A cross-reference points once. Summary sections at
  the end are cut entirely.
- **Provenance lines** under a heading. Put the citation inline where the thing
  is first mentioned.

## Deliverable documents

Tables are action lists, not inventories. Sort by what needs discussion and what
needs doing, most important first. Drop rows that exist and need nothing.
Pending questions fold into a column of the row they belong to rather than
forming a list of their own. Keep only measured numbers plus one line of
conclusion; delete whole sections that were never measured rather than leaving
an empty table.
