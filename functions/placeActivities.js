// What you do at a known place - "Visit", "Grocery shopping", "Fueling up", "Work" - recognized the same
// way the place itself is. A place's activity is suggested from its name when it is first saved
// (`activitySource: "suggested"`) and can be changed on the Known Places page (`activitySource: "set"`);
// a change applies to visits from then on. Every arrival and departure at the place carries it, so Time
// Left To Live shows "Grocery shopping at Costco Guelph" and counts the stay as that activity.

const MAX_ACTIVITY_LENGTH = 40;
const DEFAULT_ACTIVITY = "Visit";

// The choices offered on the Known Places page; any other short text works too.
const PLACE_ACTIVITY_CHOICES = [
  "Visit",
  "Home",
  "Work",
  "Gym",
  "Grocery shopping",
  "Shopping",
  "Fueling up",
  "Car service",
  "Eating out",
  "Appointment",
  "Errand",
  "Worship",
];

// First match wins, so the specific comes before the general: "Costco Gas" is fueling up before Costco is
// groceries, "Coffee shop" is eating out and "Barber shop" an errand before "shop" is shopping, and "Home
// Depot" is shopping before "home" is a visit. Only a place named just "Home" is Home.
const SUGGESTION_RULES = [
  ["Home", /^(?:my\s+)?(?:home|house)$/i],
  ["Fueling up", /\b(?:gas|fuel|petro|esso|shell|pioneer|ultramar|husky|mobil|chevron|sunoco|irving|circle k|gas bar)\b/i],
  ["Car service", /\b(?:oil change|mechanic|automotive|auto service|car wash|garage|jiffy|lube|midas|dealership|collision|body shop)\b|(?<!canadian )\btires?\b/i],
  ["Grocery shopping", /\b(?:costco|grocery|groceries|supermarket|no ?frills|food ?basics|freshco|loblaws|zehrs|metro|sobeys|superstore|farm ?boy|longos?|fortinos|t&t|adonis|food ?land|butcher|bakery)\b/i],
  ["Gym", /\b(?:goodlife|gym|fitness|crossfit|ymca|bells of steel|anytime|planet fitness|dojo|yoga)\b/i],
  ["Work", /\b(?:work|office|head ?office|headoffice|site|docksteader|matheson|job ?site)\b/i],
  ["Eating out", /\b(?:restaurant|grill|bar|pub|cafe|café|coffee|tim hortons|starbucks|pizza|pizzeria|burger|sushi|shawarma|diner|bistro|kitchen|steakhouse|mcdonald'?s|wendy'?s|a&w|subway|popeyes|harvey'?s|swiss chalet|kelsey'?s|montana'?s|chipotle|bbq|taco|noodle|pho|ramen)\b/i],
  ["Appointment", /\b(?:clinic|hospital|doctor|dr\.?|dentist|dental|physio|physiotherapy|chiro|chiropractor|optometrist|medical|lifelabs|vet)\b/i],
  ["Errand", /\b(?:bank|post office|canada post|pharmacy|shoppers|rexall|library|dry clean|cleaners|barber|salon|service ?ontario|city hall)\b/i],
  ["Worship", /\b(?:church|mosque|masjid|temple|cathedral|synagogue|parish)\b/i],
  ["Shopping", /\b(?:mall|walmart|canadian tire|home depot|lowe'?s|rona|ikea|best ?buy|winners|homesense|marshalls|giant tiger|dollarama|dollar|store|shop|outlet|plaza|staples)\b/i],
  ["Visit", /\b(?:house|home|place|apartment|apt|condo|cottage|farm)\b/i],
];

/** A tidy activity name ("" when there is none): single spaces, at most 40 characters. */
function normalizePlaceActivity(value) {
  return String(value == null ? "" : value).replace(/\s+/g, " ").trim().slice(0, MAX_ACTIVITY_LENGTH);
}

/** The activity a place's name suggests; "Visit" when nothing in the name says otherwise. */
function suggestPlaceActivity(name) {
  const text = normalizePlaceActivity(name) ? String(name).replace(/\s+/g, " ").trim() : "";
  for (const [activity, pattern] of SUGGESTION_RULES) {
    if (pattern.test(text)) return activity;
  }
  return DEFAULT_ACTIVITY;
}

/** The activity for a visit to `place`: the one saved on it, or the one its name suggests. */
function placeActivity(place) {
  return normalizePlaceActivity(place && place.activity) || suggestPlaceActivity(place && place.name);
}

/** Fields to save on a newly named place: its suggested activity. */
function suggestedActivityFields(name) {
  return { activity: suggestPlaceActivity(name), activitySource: "suggested" };
}

module.exports = {
  DEFAULT_ACTIVITY,
  MAX_ACTIVITY_LENGTH,
  PLACE_ACTIVITY_CHOICES,
  normalizePlaceActivity,
  placeActivity,
  suggestPlaceActivity,
  suggestedActivityFields,
};
