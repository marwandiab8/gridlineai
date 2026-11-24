# Construction sequence animations (shop drawing portal)

Step-by-step animations of how a Docksteader detail gets built. They live under the shop drawing portal, in
section **09 Construction Sequences** of `public/shop-drawings/index.html`, and each one has its own page.

| Animation | Page | Added | Drawn from |
|---|---|---|---|
| Grid H bulkhead, Z2 Admin / Z3 Vehicle Bay slab step (12 steps) | `/shop-drawings/sequences/grid-h-bulkhead/` | 2026-11-15 | 5/S702, 11/S006, 3/S008, 7/S008, A151 PC-022, AGF 4/R29, S201 slab notes, spec 03 30 00 |
| Roof Flow Lab, Level 2 west roof water-flow simulator (7 built-in scenarios) | `/shop-drawings/sequences/roof-flow-lab/` | 2026-11-16 | F2C as-built top-of-deck survey (221 readings, surveyed Nov 5 2026, revised Nov 11); S203B for the design-deck comparison |
| Solatube tubular skylights, one unit from deck opening to diffuser (8 steps) | `/shop-drawings/sequences/solatube-install/` | 2026-11-18 | Solatube shop dwg M74 DS-O-C-DP-FC-A Rev 2 (R0), metal curb dwg Rev A + DSA-requested markup, M74 DS install instructions, spec 08 63 00, Triumph 18/A210, RFI 251, MCL sketches SK-SOL-01/02 |
| Walk-in cooler #10, Norbec box from the slab depression to start-up (12 steps, 3D WebGL) | `/shop-drawings/sequences/walk-in-cooler-install/` | 2026-11-19 | Norbec stamped shop dwg S-294496 rev 02 (sheets 1/11–11/11), MCL transmittal Aug 25 2025 (job 2025-048), Bohn PT0078MBNAMCA data sheet, IM4 operating instructions, Kason 1236 sheets |
| Car Wash 117, Istobal Flex 5 XL rollover gantry from the trench drains to the test wash (11 steps, 3D WebGL) | `/shop-drawings/sequences/car-wash-install/` | 2026-11-19 | Istobal layout drawings for National Wash, sheets 1/7–7/7 (title blocks rev 4.0, May 7 2025; MCL transmittal Aug 20 2025), spec 11 11 26, National Wash scope review minutes (Apr 8 2025), RFI 080, M202, A105, S506 |
| Vehicle bay slab on grade, south of H and west of 6.1, from the underground plumbing to the topping (12 steps, 3D WebGL) | `/shop-drawings/sequences/vehicle-bay-slab/` | 2026-11-19 | M200 underground plumbing (PC-056 / MNC-15), M903, A151 slab edge plan (PC-022), S201 SOG schedule + notes, AGF R49 trench drain reinforcement (reviewed as noted Feb 5 2026), SK-01, RFI 260 / 315 / 326 R1, F2C grade sheet (Oct 18 2026), Kolostat / Uponor Level 1 radiant layout R2, spec 03 30 00 |
| Elevator, Schindler 3300 MRL from the ready hoistway to acceptance (13 steps, 3D WebGL) | `/shop-drawings/sequences/elevator-install/` | 2026-11-21 | Schindler shop dwgs WBS 11944614 sub 002 sheets 1/12–12/12 (submittal 14 21 23-1 R3, reviewed as noted by DSA Oct 22 2026), RFI 293, PC-082, RFI 325 R2 / PC-083, Alvin Hernandez critical items email (Nov 20 2026) |
| Elevator temporary top landing platform at Level 2, from design sign-off to removal (12 steps, 3D WebGL) | `/shop-drawings/sequences/top-landing-platform/` | 2026-11-21 | Schindler shop dwg 3/12 (platform plan, notes 1–6, Section A-A), 1/12 note 35 and item 30, 4/12 barrier detail, O. Reg. 213/91 ss. 26.1, 26.3, 130, 134, 135 (consolidated Jan 1 2026) |
| CB-4 lead tie-in for the fuelling-station roof leader, Road-Ex's June 22–23 2026 work under CD-007 from the buried lead to the backfill (10 steps, 3D WebGL) | `/shop-drawings/sequences/cb4-lead-tie-in/` | 2026-11-24 | Stantec GEN-1 rev 32 (Jun 25 2026) and SPC-008, DSA PC-054, DSA CD-007 (Jun 22 2026), Introba M-100 rev 72 detail 2 (MNC-14 / PC-049), MCL progress markup (Jul 6 2026), field grading sketch, Road-Ex tickets 2570 and 2571 and invoice 62223, O. Reg. 213/91 Part III. Companion sketch SK-RWL-01 (11×17 PDF, not in the repo). Pit size, depth and stub height are derived from the drawings until confirmed in the field |

## 3D sequences

The walk-in cooler, car wash, vehicle bay slab, elevator, top landing platform and CB-4 lead tie-in pages draw their models with WebGL 2 straight from the page script
(boxes and cylinders, shadows, cutaway walls), with labels, dimensions and captions in an HTML overlay. The slab page
adds heightfield meshes for the ground and slab layers (built in the page from the A151 falls), line meshes for the
rebar mats and radiant tubing, and a clip plane that sweeps each layer in as it is placed. They still need no library
and no build step. Opening one with `window.__VIDEO = true` set before the script runs gives a full-frame 16:9 stage, and
`window.__seek(seconds)` draws any moment; that is how their MP4s were filmed.

## Simulators

The Roof Flow Lab is a working model rather than a step sequence, so it has storm, hose, drain, sump and build-up tools
in place of Play / Prev / Next. It is still one self-contained page: the survey grid, the flow engine and the interface
are inline in its `index.html`, with no build step. Scenarios saved on the page stay in the browser that saved them
(`localStorage`); the seven built-in scenarios are in the page itself. Its portal card uses the same `status-sequence`
chip with the label **Simulator**, and the section heading counts it separately (`1 animation · 1 simulator`).

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
