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
    "code": "WH-HOARD",
    "category": "winter-heat",
    "label": "Hoarding / winter protection",
    "description": "Build and install hoarding (lumber and tarps) at curtain wall openings and windows, including the rough carpentry for it."
  },
  {
    "code": "WH-TARP",
    "category": "winter-heat",
    "label": "Install / remove tarps (heat)",
    "description": "Install tarps to retain heat."
  },
  {
    "code": "WH-FUEL",
    "category": "winter-heat",
    "label": "Refuel heaters",
    "description": "Fuel handling and refueling."
  },
  {
    "code": "WH-HEATER",
    "category": "winter-heat",
    "label": "Set up / move heaters",
    "description": "Set up, move and check heaters and heat ducting."
  },
  {
    "code": "WH-SNOW",
    "category": "winter-heat",
    "label": "Snow / ice removal",
    "description": "Shovel and remove snow and ice."
  },
  {
    "code": "WH-PUMP",
    "category": "winter-heat",
    "label": "Pump melted snow / ice",
    "description": "Set up pumps to remove melted snow and ice."
  },
  {
    "code": "WH-OTHER",
    "category": "winter-heat",
    "label": "Other winter heat work",
    "description": "Other winter heat work (see note)."
  },
  {
    "code": "GC-HOUSE",
    "category": "general-conditions",
    "label": "Site housekeeping",
    "description": "Maintain safe work environment."
  },
  {
    "code": "GC-SIGN",
    "category": "general-conditions",
    "label": "Barricades / signage",
    "description": "Maintain barricades and signage."
  },
  {
    "code": "GC-RAIL",
    "category": "general-conditions",
    "label": "Safety railing - install / reinstate",
    "description": "Install, remove and reinstate safety railing."
  },
  {
    "code": "GC-CARP",
    "category": "general-conditions",
    "label": "Rough carpentry - railing / safety",
    "description": "Rough carpentry for safety railing and other safety work."
  },
  {
    "code": "GC-SAFETY",
    "category": "general-conditions",
    "label": "Site safety",
    "description": "Site safety checks and upkeep."
  },
  {
    "code": "GC-OTHER",
    "category": "general-conditions",
    "label": "Other general conditions",
    "description": "Other general conditions work (see note)."
  },
  {
    "code": "OT-GEN",
    "category": "other",
    "label": "General labour",
    "description": "General labour."
  },
  {
    "code": "OT-MAT",
    "category": "other",
    "label": "Unload / move materials",
    "description": "Unload, move and lift materials."
  },
  {
    "code": "OT-CARP",
    "category": "other",
    "label": "Rough carpentry - other",
    "description": "Rough carpentry not for winter heat or safety."
  },
  {
    "code": "OT-OTHER",
    "category": "other",
    "label": "Other work",
    "description": "Other work (see note)."
  }
];
