# Construction sequence animations (shop drawing portal)

Step-by-step animations of how a Docksteader detail gets built. They live under the shop drawing portal, in
section **09 Construction Sequences** of `public/shop-drawings/index.html`, and each one has its own page.

| Animation | Page | Added | Drawn from |
|---|---|---|---|
| Grid H bulkhead, Z2 Admin / Z3 Vehicle Bay slab step (12 steps) | `/shop-drawings/sequences/grid-h-bulkhead/` | 2026-11-15 | 5/S702, 11/S006, 3/S008, 7/S008, A151 PC-022, AGF 4/R29, S201 slab notes, spec 03 30 00 |

## Adding one

1. **Page.** Put it at `public/shop-drawings/sequences/<slug>/index.html`, one self-contained HTML file with its CSS and
   script inline, no build step. Live link: `https://gridlineai.web.app/shop-drawings/sequences/<slug>/`.
   Hosting serves the folder's `index.html` before the `**` rewrite, the same way it serves `stair-a-overlay/`.
2. **Back link.** First thing in the header: `<a class="back" href="/shop-drawings/#sequences">← Docksteader shop drawing portal</a>`.
3. **Portal card.** Add a `drawing-card` to the `#sequences` article in `public/shop-drawings/index.html`:

   ```html
   <a class="drawing-card" href="./sequences/<slug>/"><div class="drawing-copy"><span>Animation · <sheets used></span><h3><Title></h3><p><Mon D, YYYY> · <N> steps · <one-line summary></p></div><div class="drawing-action"><span class="status status-sequence">Animation</span><b>Play animation →</b></div></a>
   ```

   Then update the count in the section heading (`<p>N animations</p>`), the hero **Animations** number, and the dated
   note in the footer.
4. **Conventions the existing page follows.** Light and dark themes from CSS tokens; a phone view that zooms the drawing
   on the work area; Play / Prev / Next plus a numbered step bar; every step has a caption and key numbers; open questions
   for the consultants in a fold-out list; sources and "schematic, not a substitute for the engineer's detail" in the footer.
5. **Ship it.** Branch `feat/<slug>`, open a pull request to `main`, merge. The "Deploy to Firebase Hosting on merge"
   workflow runs the functions tests and deploys Hosting about a minute later.
