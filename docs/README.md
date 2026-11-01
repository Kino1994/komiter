# Design assets

Two generated images, each with the page it is rendered from. Nothing here is hand-edited —
change the source and re-render.

## Banner — `img/banner.png`

The image at the top of the README: the wordmark with a crumb trail spilling out of the "o".

```bash
google-chrome --headless=new --disable-gpu --hide-scrollbars \
  --force-device-scale-factor=2 --window-size=1200,350 --virtual-time-budget=6000 \
  --screenshot=docs/img/banner.png docs/banner.html
```

The trail is generated at render time from the *measured* bounding box of the "o", so it stays
attached to the glyph if the type size or the font changes; a seeded PRNG keeps it identical
between runs.

It carries its own dark background on purpose: GitHub renders READMEs in both light and dark
themes, and a transparent image with white text disappears in one of them.

## Flow diagram — `img/flow.png`

`flow.html` renders it with mermaid itself, so it keeps the familiar mermaid look rather than
an imitation of it. The crop follows the diagram's own bounding box, so there is no fixed size
to keep in sync:

```bash
google-chrome --headless=new --disable-gpu --hide-scrollbars \
  --force-device-scale-factor=3 --window-size=1600,1400 --virtual-time-budget=8000 \
  --screenshot=docs/img/flow.png docs/flow.html
```

It is a static image rather than a fenced mermaid block on purpose: GitHub wraps every mermaid
block in an interactive pan-and-zoom viewer whose controls sit on top of the diagram, and there
is no way to opt out.

Edge labels are kept short — long ones push the ranks apart and the layout sprawls. The prose
under the diagram in the README carries the detail instead.

## Screenshots

`img/kafka-ui-*.png` are captured from a real run of [`../demo/`](../demo/) against a live
broker — nothing is mocked up. See the demo README for how to reproduce the run.
