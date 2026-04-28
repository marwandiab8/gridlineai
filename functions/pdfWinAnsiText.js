/**
 * StandardFonts (Helvetica) use WinAnsi — many Unicode code points throw at draw/measure time.
 * Normalize for pdf-lib StandardFonts before any widthOfTextAtSize or drawText.
 */
function sanitizePdfText(input) {
  let s = String(input ?? "");
  const swaps = {
    "â€”": "--",
    "â€“": "-",
    "â€˜": "'",
    "â€™": "'",
    "â€œ": '"',
    "â€": '"',
    "â€¦": "...",
    "Â³": "3",
    "Â²": "2",
    "\u2022": "-",
    "\u2023": "-",
    "\u2043": "-",
    "\u2192": "->",
    "\u2190": "<-",
    "\u2194": "<->",
    "\u21D2": "=>",
    "\u21D4": "<=>",
    "\u21D0": "<=",
    "\u2013": "-",
    "\u2014": "--",
    "\u2015": "--",
    "\u2011": "-",
    "\u2212": "-",
    "\u2018": "'",
    "\u2019": "'",
    "\u201c": '"',
    "\u201d": '"',
    "\u2026": "...",
    "\u00a0": " ",
    "\u00b2": "2",
    "\u00b3": "3",
  };
  for (const [k, v] of Object.entries(swaps)) {
    s = s.split(k).join(v);
  }
  let out = "";
  for (const ch of s) {
    const cp = ch.codePointAt(0);
    out += cp <= 0xff ? ch : "?";
  }
  return out;
}

module.exports = { sanitizePdfText };
