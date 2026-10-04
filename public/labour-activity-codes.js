// Browser copy of functions/labourActivityCodes.js (labels only; matching rules stay on the server).
// functions/labourActivityCodes.test.js fails if the two lists differ.
export const LABOUR_CATEGORIES = [
  {
    "id": "winter-heat",
    "label": "Winter Heat",
    "chargeable": true,
    "note": "Extra - charged to the owner"
  },
  {
    "id": "general-conditions",
    "label": "General Conditions",
    "chargeable": false,
    "note": "In contract"
  },
  {
    "id": "other",
    "label": "Other Work",
    "chargeable": false,
    "note": "Not extra"
  }
];

export const LABOUR_ACTIVITIES = [
  {
    "code": "WH-HOARD-PREP",
    "category": "winter-heat",
    "keyword": "hoarding prep",
    "label": "Hoarding - prepare lumber and tarps",
    "description": "Measure curtain wall and window openings; cut and assemble dimensional lumber for hoarding frames; cut tarps to size; stage material at the openings."
  },
  {
    "code": "WH-HOARD-INSTALL",
    "category": "winter-heat",
    "keyword": "hoarding install",
    "label": "Hoarding - install from floor",
    "description": "Install hoarding frames at curtain wall and window openings working from the floor slab; fasten frames to the structure; fit and secure tarps and seal the edges to retain heat."
  },
  {
    "code": "WH-HOARD-LIFT",
    "category": "winter-heat",
    "keyword": "hoarding lift",
    "label": "Hoarding - install from scissor lift",
    "description": "Install hoarding frames at curtain wall and window openings using a 47 ft scissor lift; fasten frames to the structure; fit and secure tarps and seal the edges to retain heat."
  },
  {
    "code": "WH-HOARD-REPAIR",
    "category": "winter-heat",
    "keyword": "hoarding repair",
    "label": "Hoarding - inspect and repair",
    "description": "Inspect hoarding after wind and weather; re-secure loose frames, replace torn tarps and re-seal gaps to maintain the heated enclosure."
  },
  {
    "code": "WH-HOARD-REMOVE",
    "category": "winter-heat",
    "keyword": "hoarding remove",
    "label": "Hoarding - remove",
    "description": "Remove hoarding frames and tarps to release openings for curtain wall and window installation; salvage reusable lumber and tarps, stockpile them and clear the debris."
  },
  {
    "code": "WH-TARP",
    "category": "winter-heat",
    "keyword": "tarps",
    "label": "Tarps - install / remove for heat",
    "description": "Install and remove tarps at slab edges and openings to enclose heated work areas and retain heat during heating operations."
  },
  {
    "code": "WH-FUEL",
    "category": "winter-heat",
    "keyword": "heater fuel",
    "label": "Heaters - refuel",
    "description": "Refuel temporary heaters and exchange fuel tanks; check that each heater is running after refuelling."
  },
  {
    "code": "WH-HEATER",
    "category": "winter-heat",
    "keyword": "heater move",
    "label": "Heaters - set up / relocate",
    "description": "Set up and relocate temporary heaters and heat ducting to keep heated work areas at the required temperature; check heater operation."
  },
  {
    "code": "WH-BLANKET",
    "category": "winter-heat",
    "keyword": "blankets",
    "label": "Insulated blankets - place / remove",
    "description": "Place and remove insulated blankets to protect concrete, footings and ground from freezing."
  },
  {
    "code": "WH-SNOW",
    "category": "winter-heat",
    "keyword": "snow",
    "label": "Snow removal",
    "description": "Shovel and clear snow from work areas, slab, access routes and stairs so work can proceed safely."
  },
  {
    "code": "WH-ICE",
    "category": "winter-heat",
    "keyword": "ice",
    "label": "Ice removal / salting",
    "description": "Break up and remove ice; apply salt and ice melt on walkways, ramps, stairs and work areas."
  },
  {
    "code": "WH-PUMP",
    "category": "winter-heat",
    "keyword": "snow pump",
    "label": "Pump melted snow / ice",
    "description": "Set up pumps and discharge hoses to remove water from melted snow and ice; monitor pumps and clear blockages."
  },
  {
    "code": "WH-OTHER",
    "category": "winter-heat",
    "keyword": "winter other",
    "label": "Other winter heat work",
    "description": "Other winter heat work, as noted."
  },
  {
    "code": "GC-HOUSE",
    "category": "general-conditions",
    "keyword": "housekeeping",
    "label": "Site housekeeping",
    "description": "General site housekeeping: collect and remove debris, sweep floors and stairs, empty bins and keep access routes clear to maintain a safe work environment."
  },
  {
    "code": "GC-SIGN",
    "category": "general-conditions",
    "keyword": "signage",
    "label": "Barricades / signage",
    "description": "Install and maintain barricades, safety signage and caution tape at hazards and restricted areas."
  },
  {
    "code": "GC-RAIL-INSTALL",
    "category": "general-conditions",
    "keyword": "railing install",
    "label": "Safety railing - install",
    "description": "Install perimeter and opening safety railing at slab edges, floor openings and stairs."
  },
  {
    "code": "GC-RAIL-REINSTATE",
    "category": "general-conditions",
    "keyword": "railing reinstate",
    "label": "Safety railing - remove / reinstate",
    "description": "Remove safety railing for trade access and reinstate it after; check and re-secure railing."
  },
  {
    "code": "GC-RAIL-CARP",
    "category": "general-conditions",
    "keyword": "railing carpentry",
    "label": "Rough carpentry - safety railing",
    "description": "Cut and prepare lumber for temporary wood safety railing and guards; build the railing sections."
  },
  {
    "code": "GC-PROTECT",
    "category": "general-conditions",
    "keyword": "protection",
    "label": "Temporary protection - stairs / floors",
    "description": "Install and maintain temporary protection on stairs, floors and finished work."
  },
  {
    "code": "GC-SAFETY",
    "category": "general-conditions",
    "keyword": "safety",
    "label": "Site safety",
    "description": "Site safety walk with the supervisor; inspect and correct hazards at railings, openings and access routes."
  },
  {
    "code": "GC-OTHER",
    "category": "general-conditions",
    "keyword": "gc other",
    "label": "Other general conditions",
    "description": "Other general conditions work, as noted."
  },
  {
    "code": "OT-GEN",
    "category": "other",
    "keyword": "general",
    "label": "General labour",
    "description": "General labour assisting trades as directed by the site supervisor."
  },
  {
    "code": "OT-MAT",
    "category": "other",
    "keyword": "unloading",
    "label": "Unload / move materials",
    "description": "Unload deliveries and move materials to the work areas."
  },
  {
    "code": "OT-CARP",
    "category": "other",
    "keyword": "carpentry other",
    "label": "Rough carpentry - other",
    "description": "Rough carpentry not for winter heat or safety, as noted."
  },
  {
    "code": "OT-OTHER",
    "category": "other",
    "keyword": "other",
    "label": "Other work",
    "description": "Other work, as noted."
  }
];
