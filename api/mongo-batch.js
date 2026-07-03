// ─────────────────────────────────────────────────────────────────
//  api/mongo-batch.js  —  Vercel Serverless Function
//  No auth secret — called from Google Apps Script only
// ─────────────────────────────────────────────────────────────────

const { MongoClient } = require("mongodb");

const MONGO_URI = process.env.MONGO_URI;
const DB_NAME   = "sar-in-air-sales";
const USD_TO_INR = 94; // Exchange rate used to convert USD targets → INR (same base as GP values)

const COLLECTIONS = {
  "Sea Export":        "jobs_sea_export",
  "Sea Import":        "jobs_sea_import",
  "Air Export":        "jobs_air_export",
  "Air Import":        "jobs_air_import",
  "ISO Tank - Export": "jobs_isotank_export",
  "ISO Tank - Import": "jobs_isotank_import",
  "General":           "jobs_general",
  "Road":              "jobs_road",
  "Clearance - Export": "jobs_clearance_export",
  "Clearance - Import": "jobs_clearance_import",
};

const MAPPING_COLLECTIONS = new Set([
  "mapping_sales_targets",
  "mapping_zone_targets",
]);

let cachedClient = null;

async function getDB() {
  if (cachedClient) return cachedClient.db(DB_NAME);
  cachedClient = new MongoClient(MONGO_URI, {
    connectTimeoutMS: 15000,
    serverSelectionTimeoutMS: 15000,
  });
  await cachedClient.connect();
  return cachedClient.db(DB_NAME);
}

async function batchInsertJobs(db, records, clearFirst = true, fy = null) {
  const summary = {};
  const errors  = [];

  for (const [tabName, rows] of Object.entries(records)) {
    const collectionName = COLLECTIONS[tabName];
    if (!collectionName) {
      errors.push(`Unknown tab: "${tabName}" — skipped.`);
      continue;
    }
    if (!Array.isArray(rows) || rows.length === 0) {
      summary[tabName] = 0;
      continue;
    }
    try {
      const col = db.collection(collectionName);
      // Only wipe rows belonging to THIS fiscal year's previous push — never
      // wipe the whole collection, since FY26 and FY27 data now coexist in
      // the same collections. If no fy tag is given, fall back to wiping the
      // whole collection (legacy single-FY behavior) for backward compatibility.
      if (clearFirst) {
        if (fy) await col.deleteMany({ _fy: fy });
        else await col.deleteMany({});
      }
      const stamped = rows.map((row) => ({ ...row, _tab: tabName, _fy: fy || null, _insertedAt: new Date() }));
      const result  = await col.insertMany(stamped, { ordered: false });
      summary[tabName] = result.insertedCount;
    } catch (err) {
      errors.push(`${tabName}: ${err.message}`);
      summary[tabName] = 0;
    }
  }
  return { summary, errors };
}

async function batchInsertMapping(db, collectionName, records, fy = null) {
  if (!MAPPING_COLLECTIONS.has(collectionName)) {
    return { error: `Unknown mapping collection: "${collectionName}"` };
  }
  if (!Array.isArray(records) || records.length === 0) return { inserted: 0 };
  const col = db.collection(collectionName);
  // Only wipe THIS fiscal year's previously-pushed mapping rows — FY26 and
  // FY27 mapping data now coexist in the same collection. If no fy tag is
  // given, fall back to wiping the whole collection (legacy behavior).
  if (fy) await col.deleteMany({ _fy: fy });
  else await col.deleteMany({});
  const stamped = records.map((row) => ({ ...row, _fy: fy || null, _insertedAt: new Date() }));
  const result  = await col.insertMany(stamped, { ordered: false });
  return { inserted: result.insertedCount };
}

async function batchUpsertUsers(db, users) {
  if (!Array.isArray(users) || users.length === 0) {
    return { inserted: 0, updated: 0, errors: ["No users provided"] };
  }
  const col     = db.collection("users");
  const results = { inserted: 0, updated: 0, errors: [] };
  for (const u of users) {
    const email = u.email?.toLowerCase().trim();
    if (!email || !u.name) { results.errors.push(`Skipped: ${JSON.stringify(u)}`); continue; }
    try {
      const result = await col.updateOne(
        { email },
        {
          $set: { name: u.name, email, role: u.role || "user", isActive: u.isActive !== undefined ? u.isActive : true, updatedAt: new Date() },
          $setOnInsert: { createdAt: new Date(), lastLogin: null, photoUrl: "" },
        },
        { upsert: true }
      );
      if (result.upsertedCount > 0) results.inserted++;
      else if (result.modifiedCount > 0) results.updated++;
    } catch (err) { results.errors.push(`${email}: ${err.message}`); }
  }
  return results;
}

// ── ACTION: org (read-only org chart) ─────────────────────────────
async function getOrgChart(db) {
  const users = await db.collection("users").find({}).toArray();

  var people = users.map(function(u){
    return {
      email:     u.email || "",
      name:      u.name  || "",
      role:      u.role  || "user",
      reportsTo: (u.reportsTo || "").toLowerCase().trim(),
      isActive:  u.isActive !== false,
    };
  });

  // Roles considered part of the management hierarchy chain (vertical org chart)
  var HIERARCHY_ROLES = new Set(["regional manager", "zonal manager", "manager"]);
  // Roles shown as flat "support" cards on the side, not in the vertical chain
  var SUPPORT_ROLES = new Set(["admin", "hr"]);

  var byEmail = {};
  people.forEach(function(p){ byEmail[p.email.toLowerCase()] = p; });

  function isHierarchyRole(role){
    return HIERARCHY_ROLES.has(String(role||"").toLowerCase().trim());
  }
  function isSupportRole(role){
    return SUPPORT_ROLES.has(String(role||"").toLowerCase().trim());
  }

  var hierarchy = people.filter(function(p){ return isHierarchyRole(p.role); });
  var directReports = people.filter(function(p){ return !isHierarchyRole(p.role) && !isSupportRole(p.role); });
  var support = people.filter(function(p){ return isSupportRole(p.role); });

  // Count direct reports for each hierarchy person (by reportsTo email match)
  hierarchy.forEach(function(h){
    h.reportCount = people.filter(function(p){
      return p.reportsTo && byEmail[p.reportsTo] && byEmail[p.reportsTo].email.toLowerCase() === h.email.toLowerCase();
    }).length;
  });

  return {
    success: true,
    totalUsers: people.length,
    hierarchy: hierarchy,       // vertical chain: Regional/Zonal Managers
    directReports: directReports, // flat row under hierarchy: Sales Reps etc.
    support: support,           // side panel: Admin/HR
    pushedAt: new Date().toISOString(),
  };
}

async function updateUserFields(db, email, fields) {
  if (!email) return { error: "email is required" };
  var allowed = {};
  if (fields.role !== undefined)      allowed.role      = fields.role;
  if (fields.reportsTo !== undefined) allowed.reportsTo = String(fields.reportsTo || "").toLowerCase().trim();
  if (Object.keys(allowed).length === 0) return { error: "No valid fields to update" };
  allowed.updatedAt = new Date();

  var result = await db.collection("users").updateOne(
    { email: email.toLowerCase().trim() },
    { $set: allowed }
  );
  if (result.matchedCount === 0) return { error: "User not found" };
  return { updated: true };
}


const JOB_COLLECTIONS = [
  "jobs_sea_export", "jobs_sea_import", "jobs_air_export", "jobs_air_import",
  "jobs_isotank_export", "jobs_isotank_import", "jobs_general", "jobs_road",
  "jobs_clearance_export", "jobs_clearance_import",
];

// ISO Tank rows are identified by which sheet tab they came from (the _tab
// stamp added at insert time), since ISO Tank has its own dedicated tabs —
// this is reliable regardless of what the LOB column says on those rows.
const ISOTANK_COLLECTIONS = new Set(["jobs_isotank_export", "jobs_isotank_import"]);

// Everything else is classified by the row's actual "LOB" column value,
// NOT by which MongoDB collection it landed in. This protects against rows
// being misfiled into the wrong sheet tab/collection (e.g. an ISO Tank row
// sitting inside the Sea Export tab) — the LOB text on the row is the
// source of truth for Export vs Import vs Air vs Sea classification.
function classifyRow(job, collName) {
  if (ISOTANK_COLLECTIONS.has(collName)) {
    const isExport = collName === "jobs_isotank_export";
    return { kind: "ISOTANK", direction: isExport ? "EXPORT" : "IMPORT" };
  }

  const lob = String(job["LOB"] || "").toUpperCase().trim();

  if (lob === "SEA EXPORT")      return { kind: "SEA",       direction: "EXPORT" };
  if (lob === "SEA IMPORT")      return { kind: "SEA",       direction: "IMPORT" };
  if (lob === "AIR EXPORT")      return { kind: "AIR",       direction: "EXPORT" };
  if (lob === "AIR IMPORT")      return { kind: "AIR",       direction: "IMPORT" };
  if (lob === "CLEARANCE EXPORT") return { kind: "CLEARANCE", direction: "EXPORT" };
  if (lob === "CLEARANCE IMPORT") return { kind: "CLEARANCE", direction: "IMPORT" };
  if (lob === "GENERAL")         return { kind: "GENERAL",   direction: null };
  if (lob === "ROAD")            return { kind: "ROAD",       direction: null };

  // Unknown/blank LOB — fall back to the collection it was pushed from,
  // so the row still gets *some* reasonable treatment instead of being dropped.
  if (collName.includes("sea"))       return { kind: "SEA",       direction: collName.includes("import") ? "IMPORT" : "EXPORT" };
  if (collName.includes("air"))       return { kind: "AIR",       direction: collName.includes("import") ? "IMPORT" : "EXPORT" };
  if (collName.includes("clearance")) return { kind: "CLEARANCE", direction: collName.includes("import") ? "IMPORT" : "EXPORT" };
  if (collName.includes("general"))   return { kind: "GENERAL",   direction: null };
  if (collName.includes("road"))      return { kind: "ROAD",       direction: null };
  return { kind: "UNKNOWN", direction: null };
}

// Tons metric is calculated only for AIR rows (Export or Import)
function isAirRow(cls) { return cls.kind === "AIR"; }

// Pick the correct GP field based on lock status
// Air (Import/Export): if "Financial Lock" is empty → Provisional; else Actual
// All others:          if "Operation Lock" is empty → Provisional; else Actual
function pickGP(job, cls) {
  const isAir = cls.kind === "AIR";
  const lockField = isAir ? "Financial Lock" : "Operation Lock";
  const isLocked  = job[lockField] !== undefined && job[lockField] !== null && String(job[lockField]).trim() !== "";
  const actual      = parseFloat(job["Actual Profit (J=C-G)"]     || 0) || 0;
  const provisional = parseFloat(job["Provisional Profit (I=A-E)"] || 0) || 0;
  return { gp: isLocked ? actual : provisional, isProvisional: !isLocked };
}

// TEU and LCL(CBM) metrics are calculated only for SEA and ISOTANK rows
function hasTeuLclRow(cls) { return cls.kind === "SEA" || cls.kind === "ISOTANK"; }

// Decides which physical metric a row's Cargo Type maps to, independent of
// LOB kind. This is the source of truth for TEU vs LCL within Sea/ISO Tank
// rows — a Sea Export row can be FCL (→ Container TEU) or LCL (→ Volume)
// depending purely on what Cargo Type says, not on its LOB classification.
function cargoMetricFor(job) {
  const cargoType = String(job["Cargo Type"] || "").toUpperCase().trim();
  if (cargoType === "FCL" || cargoType === "LIQUID (CONT)") return "TEU";
  if (cargoType === "LCL") return "LCL";
  return null; // unrecognized Cargo Type — contributes to neither TEU nor LCL
}

// Date column: Export-direction rows use "ETD Loading Port", Import-direction
// rows use "ETA Discharge". GENERAL/ROAD (no direction) use "Job Date" only.
// All fall back to "Job Date" if the primary column is blank.
function getDateColumnFor(cls) {
  if (cls.direction === "EXPORT") return "ETD Loading Port";
  if (cls.direction === "IMPORT") return "ETA Discharge";
  return "Job Date";
}

// Indian Fiscal Years — FY26 runs Apr 2025 → Mar 2026, FY27 runs Apr 2026 → Mar 2027.
// Both are listed here since FY26 and FY27 job data now coexist in the same
// MongoDB collections (appended, not replaced) and need to be recognized
// together for month-bucketing across the combined dataset.
const FY_MONTHS = [
  "Apr-25","May-25","Jun-25","Jul-25","Aug-25","Sep-25",
  "Oct-25","Nov-25","Dec-25","Jan-26","Feb-26","Mar-26",
  "Apr-26","May-26","Jun-26","Jul-26","Aug-26","Sep-26",
  "Oct-26","Nov-26","Dec-26","Jan-27","Feb-27","Mar-27"
];
const MONTH_NAMES  = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"];

function zoneHue(zone) {
  let h = 0;
  for (let i = 0; i < zone.length; i++) h = (h * 31 + zone.charCodeAt(i)) & 0xffff;
  return h % 360;
}

// Normalizes a sales person name for matching: strips "| CODE" suffixes,
// extra whitespace, and lowercases. "Pankaj Kumar | INZ03" → "pankaj kumar"
function normalizeName(name) {
  return String(name || "")
    .split("|")[0]
    .toLowerCase()
    .replace(/\s+/g, " ")
    .trim();
}

// In-memory cache — survives across warm Lambda invocations (same container)
let salesCache = null;
let salesCacheTime = 0;
const SALES_CACHE_TTL_MS = 30 * 60 * 1000; // 30 minutes — reduces cold fetches significantly

// ── DRILL-DOWN: real job rows behind a clicked table cell ──────────
// entity: "Grand Total" | zone name | rep display name
// metric: "Shipments" | "GP" | "Tons (Air)" | "TEUs (Ocean)" | "LCL (Ocean in CBM)"
// month: e.g. "Apr-25", or "FY Total" for the whole year
// ── Drill-down row cache ─────────────────────────────────────────────────────
// Strategy: preload the full classified job dataset for a date-window once,
// then serve every subsequent drill click (different entity / metric / same
// month window) from memory in milliseconds.
//
// Key = the "month" parameter string (e.g. "Jan-26", "RANGE:Jan-26:Jun-26").
// Value = { rows: [...], mappingData: {...}, fetchedAt: timestamp }
// TTL = 20 minutes (same order as salesCache).
//
// The first click in a session warms the cache; all subsequent clicks for the
// same date window are served sub-millisecond from the cached rows without any
// MongoDB round-trip.
const DRILL_CACHE_TTL_MS = 20 * 60 * 1000;
const drillCache = new Map(); // key → { allRows, mappingData, fetchedAt }

// Evict expired entries (called before every cache write to keep memory clean)
function evictDrillCache() {
  const now = Date.now();
  for (const [k, v] of drillCache) {
    if (now - v.fetchedAt > DRILL_CACHE_TTL_MS) drillCache.delete(k);
  }
}

// Normalise a month param to a canonical cache key — e.g. "Jan-26", "RANGE:…",
// "WEEK:2026-W14", "FY Total". Different entity/metric combinations sharing the
// same date window reuse the same cached row set.
function drillCacheKey(month) {
  return month || "FY Total";
}

async function getDrillRows(db, entity, metric, month) {
  const CROSS_SALES_ZONE = "Cross Sales";
  const cacheKey = drillCacheKey(month);

  // ── Level 1: Check the drill row cache ──────────────────────────────────
  // If we've already fetched and classified all job rows for this date window,
  // every drill click (different entity/metric, same month) costs zero MongoDB
  // round-trips — just in-memory filtering on the cached classified rows.
  evictDrillCache();
  let cached = drillCache.get(cacheKey);

  if (!cached) {
    // ── Cache MISS: fetch mapping + all job rows for this date window ──────
    // This is the expensive path. It runs once per date window per serverless
    // instance lifetime, then every subsequent click is served from cache.

    // 1a. Load mapping (small collection, fast)
    const mappingRows = await db.collection("mapping_sales_targets").find({}).toArray();
    const repLookupByFY      = { FY26: {}, FY27: {} };
    const repsByZoneByFY     = { FY26: {}, FY27: {} };
    const normByDisplayByFY  = { FY26: {}, FY27: {} };

    for (const row of mappingRows) {
      const rawName = row["Sales Rep Name"];
      const norm = normalizeName(rawName);
      if (!norm) continue;
      const fy = (row._fy === "FY27") ? "FY27" : "FY26";
      const zone = String(row["Zone"] || "Unassigned").trim();
      const displayName = String(row["Display Name"] || rawName || "").trim();
      repLookupByFY[fy][norm] = { displayName, zone,
        lob: String(row["LOB"] || "").trim(),
        monthlyTarget: (parseFloat(row["Monhtly Target (USD)"] || row["Monthly Target (USD)"] || 0) || 0) * USD_TO_INR,
        email: String(row["Email ID"] || "").toLowerCase().trim(),
      };
      if (!repsByZoneByFY[fy][zone]) repsByZoneByFY[fy][zone] = [];
      if (!repsByZoneByFY[fy][zone].includes(norm)) repsByZoneByFY[fy][zone].push(norm);
      normByDisplayByFY[fy][displayName] = norm;
    }

    // 1b. Build date filter string bounds (dates stored as strings in MongoDB)
    const isFYTotal_    = month === "FY Total";
    const isYearGroup_  = month && month.startsWith("YEAR:");
    const isRange_      = month && month.startsWith("RANGE:");
    const isWeek_       = month && month.startsWith("WEEK:");
    const isDateRange_  = month && month.startsWith("DATERANGE:");

    let dateStrStart = null, dateStrEnd = null;

    if (isFYTotal_) {
      dateStrStart = "2025-04-01"; dateStrEnd = "2027-03-31";
    } else if (isYearGroup_) {
      const y = 2000 + parseInt(month.split(":")[1], 10);
      dateStrStart = `${y}-04-01`; dateStrEnd = `${y+1}-03-31`;
    } else if (isWeek_) {
      // Compute Mon-Sun bounds for this ISO week
      const isoKey = month.slice(5);
      const [iyStr, wnStr] = isoKey.split("-W");
      const iy = parseInt(iyStr, 10), wn = parseInt(wnStr, 10);
      const jan4 = new Date(Date.UTC(iy, 0, 4));
      const jan4DN = (jan4.getUTCDay() + 6) % 7;
      const w1Mon = new Date(jan4); w1Mon.setUTCDate(jan4.getUTCDate() - jan4DN);
      const mon = new Date(w1Mon); mon.setUTCDate(w1Mon.getUTCDate() + (wn-1)*7);
      const sun = new Date(mon); sun.setUTCDate(mon.getUTCDate() + 6);
      dateStrStart = mon.toISOString().slice(0,10);
      dateStrEnd   = sun.toISOString().slice(0,10);
    } else if (isDateRange_) {
      const bits = month.split(":");
      dateStrStart = bits[1]; dateStrEnd = bits[2];
    } else if (isRange_) {
      const bits = month.split(":");
      const toLabelDate = (lbl, isEnd) => {
        const [mn, yr] = lbl.split("-");
        const mi = MONTH_NAMES.indexOf(mn), y = 2000+parseInt(yr,10);
        if (isEnd) { const ld = new Date(Date.UTC(y,mi+1,0)).getUTCDate(); return `${y}-${String(mi+1).padStart(2,'0')}-${ld}`; }
        return `${y}-${String(mi+1).padStart(2,'0')}-01`;
      };
      dateStrStart = toLabelDate(bits[1], false); dateStrEnd = toLabelDate(bits[2], true);
    } else if (month) {
      const [mn, yr] = month.split("-");
      const mi = MONTH_NAMES.indexOf(mn), y = 2000+parseInt(yr,10);
      const ld = new Date(Date.UTC(y,mi+1,0)).getUTCDate();
      dateStrStart = `${y}-${String(mi+1).padStart(2,'0')}-01`;
      dateStrEnd   = `${y}-${String(mi+1).padStart(2,'0')}-${ld}`;
    }

    const buildDateCond = (col) => ({ [col]: { $type:2, $gte: dateStrStart, $lte: dateStrEnd+"\uffff" } });
    const mongoFilter = (dateStrStart && dateStrEnd) ? {
      $or: [ buildDateCond("ETD Loading Port"), buildDateCond("ETA Discharge"), buildDateCond("Job Date") ]
    } : {};

    // 1c. Fetch all collections in parallel, classify each row, store in cache
    const allCollectionResults = await Promise.all(
      JOB_COLLECTIONS.map(collName =>
        db.collection(collName).find(mongoFilter).toArray().then(rows => ({ collName, rows }))
      )
    );

    // 1d. Pre-classify every row and compute all derived fields so subsequent
    //     entity/metric filtering in Step 2 is pure in-memory logic.
    const allClassifiedRows = [];
    for (const { collName, rows } of allCollectionResults) {
      for (const job of rows) {
        const cls     = classifyRow(job, collName);
        const dateCol = getDateColumnFor(cls);
        const rawDate = job[dateCol] || job["Job Date"];
        if (!rawDate) continue;
        const d = new Date(rawDate);
        if (isNaN(d.getTime())) continue;
        const monthLabel = MONTH_NAMES[d.getMonth()] + "-" + String(d.getFullYear()).slice(2);
        if (!FY_MONTHS.includes(monthLabel)) continue;
        const salesPerson = normalizeName(job["Sales Person"]);
        if (!salesPerson) continue;
        const { gp: rowGP, isProvisional } = pickGP(job, cls);
        const provRevenue   = parseFloat(job["Provisional Revenue (A)"] || 0) || 0;
        const billedRevenue = parseFloat(job["Billed Revenue (C)"]       || 0) || 0;
        const provCost      = parseFloat(job["Provisional Cost (E)"]     || 0) || 0;
        const postedCostRaw = job["Posted Cost (G)"];
        const postedCost    = (postedCostRaw !== undefined && postedCostRaw !== null && String(postedCostRaw).trim() !== "")
          ? (parseFloat(postedCostRaw) || 0)
          : (billedRevenue - (parseFloat(job["Actual Profit (J=C-G)"] || 0) || 0));
        const unbilledRevenue = parseFloat(job["Unbilled Revenue (D=A-C)"] ?? provRevenue - billedRevenue) || 0;
        const unpostedCost    = parseFloat(job["Unposted Cost (H = E-G)"]  ?? provCost - postedCost)    || 0;
        const chargeable = parseFloat(job["Chargeable Weight"] || job["Volume"] || 0) || 0;

        allClassifiedRows.push({
          _salesPerson: salesPerson,  // normalized, for entity matching
          _monthLabel:  monthLabel,
          _date:        d,
          _collName:    collName,
          _cls:         cls,
          // All display fields
          shipmentNo: job["Shipment No"]    || "—",
          jobDate:    job["Job Date"]       || "",
          lob: cls.kind + (cls.direction ? " " + cls.direction : ""),
          masterNo: job["Master No."]       || "",
          houseNo:  job["House No."]        || "",
          consolNo: job["Consol No."]       || "",
          cargoType: job["Cargo Type"]      || "",
          carrier:   job["Carrier"] || job["Carrier Name"] || "",
          provRevenue, billedRevenue, unbilledRevenue,
          provCost, postedCost, unpostedCost,
          provisionalProfit: parseFloat(job["Provisional Profit (I=A-E)"] || 0) || 0,
          actualProfit:      parseFloat(job["Actual Profit (J=C-G)"]      || 0) || 0,
          customer:    job["Customer"]              || "",
          ataDischarge: job["ATA Discharge"]        || "",
          atdLoading:  job["ATD Loading Port"]      || "",
          location:    job["Location"]              || "",
          consignee:   job["Consignee"]             || "",
          consolType:  job["Consol Type"]           || "",
          teu: parseFloat(job["Container TEU"] || 0) || 0,
          destAgent:   job["Destination Agent"]     || "",
          etaDischarge: job["ETA Discharge"]        || "",
          etdLoading:  job["ETD Loading Port"]      || "",
          jobOwner:    job["Job Owner"]             || "",
          jobRevRecogDate: job["Job Rev Recognition Date"] || "",
          originAgent: job["Origin Agent"]          || "",
          salesPerson: job["Sales Person"]          || "",
          shipper:     job["Shipper"]               || "",
          volume:      parseFloat(job["Volume"] || 0) || 0,
          volumeUnit:  job["Volume Unit"]           || "",
          operationLock: job["Operation Lock"]      || "",
          financialLock: job["Financial Lock"]      || "",
          // Summary fields for totals
          g:  rowGP, r: billedRevenue, x: postedCost, t: chargeable,
          prov: isProvisional ? 1 : 0,
          m: (cls.kind === "AIR") ? chargeable : (parseFloat(job["Container TEU"] || 0) || 0),
        });
      }
    }

    cached = {
      allRows:     allClassifiedRows,
      mappingData: { repLookupByFY, repsByZoneByFY, normByDisplayByFY },
      fetchedAt:   Date.now(),
    };
    drillCache.set(cacheKey, cached);
    console.log(`[drill cache] MISS — loaded ${allClassifiedRows.length} rows for key="${cacheKey}"`);
  } else {
    console.log(`[drill cache] HIT — ${cached.allRows.length} rows for key="${cacheKey}"`);
  }

  // ── Level 2: Filter cached rows by entity + metric + exact date ─────────
  // This is now pure in-memory work — no MongoDB round-trips at all.
  const { allRows, mappingData: { repLookupByFY, repsByZoneByFY, normByDisplayByFY } } = cached;

  const isCrossSalesZone   = entity === CROSS_SALES_ZONE;
  const isGrandTotal       = entity === "Grand Total";
  const isKnownZone        = Object.values(repsByZoneByFY).some(d => d[entity]);
  const isKnownRep         = Object.values(normByDisplayByFY).some(d => d[entity]);
  const isCrossSalesBranch = !isCrossSalesZone && !isGrandTotal && !isKnownZone && !isKnownRep;
  const useCrossSalesPath  = isCrossSalesZone || isCrossSalesBranch;

  const isFYTotal      = month === "FY Total";
  const isYearGroup    = month && month.startsWith("YEAR:");
  const yearGroupSuffix = isYearGroup ? month.split(":")[1] : null;
  const isRange        = month && month.startsWith("RANGE:");
  const rangeParts     = isRange ? (() => { const b = month.split(":"); return {start:b[1], end:b[2]}; })() : null;
  const isWeek         = month && month.startsWith("WEEK:");
  const isDateRange    = month && month.startsWith("DATERANGE:");
  const isAirMetric    = metric === "Tons (Air)";
  const isTeuLclMetric = metric === "TEUs (Ocean)" || metric === "LCL (Ocean in CBM)";

  // Precompute week/daterange bounds for in-memory date filter
  let weekRange = null, dateRangeParts = null;
  if (isWeek) {
    const isoKey = month.slice(5);
    const [iyStr, wnStr] = isoKey.split("-W");
    const iy = parseInt(iyStr,10), wn = parseInt(wnStr,10);
    const jan4 = new Date(Date.UTC(iy,0,4));
    const jan4DN = (jan4.getUTCDay()+6)%7;
    const w1Mon = new Date(jan4); w1Mon.setUTCDate(jan4.getUTCDate()-jan4DN);
    const mon = new Date(w1Mon); mon.setUTCDate(w1Mon.getUTCDate()+(wn-1)*7);
    const sun = new Date(mon); sun.setUTCDate(mon.getUTCDate()+6); sun.setUTCHours(23,59,59,999);
    weekRange = { start: mon, end: sun };
  }
  if (isDateRange) {
    const bits = month.split(":");
    dateRangeParts = { start: new Date(bits[1]+"T00:00:00.000Z"), end: new Date(bits[2]+"T23:59:59.999Z") };
  }

  const matchedRows = [];

  for (const row of allRows) {
    // ── Metric filter (collection-level) ──
    if (isAirMetric    && !row._collName.includes("air")) continue;
    if (isTeuLclMetric && !row._collName.includes("sea") && !row._collName.includes("isotank")) continue;

    // ── Date filter (precise) ──
    const d         = row._date;
    const monthLabel = row._monthLabel;

    if (!isFYTotal && !isYearGroup && !isRange && !isWeek && !isDateRange && monthLabel !== month) continue;
    if (isYearGroup  && yearGroupSuffix && !monthLabel.endsWith('-'+yearGroupSuffix)) continue;
    if (isRange && rangeParts) {
      const fi = FY_MONTHS.indexOf(rangeParts.start), ti = FY_MONTHS.indexOf(rangeParts.end), mi = FY_MONTHS.indexOf(monthLabel);
      if (fi<0||ti<0||mi<fi||mi>ti) continue;
    }
    if (isWeek      && weekRange      && (d < weekRange.start      || d > weekRange.end))      continue;
    if (isDateRange && dateRangeParts && (d < dateRangeParts.start || d > dateRangeParts.end)) continue;

    // ── Entity filter (in-memory normalizeName matching) ──
    const salesPerson = row._salesPerson;
    const fy = (d.getFullYear() >= 2026 && d.getMonth() >= 3) || (d.getFullYear() > 2026) ? "FY27" : "FY26";
    const mapped = repLookupByFY[fy][salesPerson] || repLookupByFY["FY26"][salesPerson] || repLookupByFY["FY27"][salesPerson];

    if (isGrandTotal) {
      // all rows pass
    } else if (useCrossSalesPath) {
      if (mapped) continue; // mapped rows are not Cross Sales
      if (isCrossSalesBranch) {
        const loc = String(row.location || "").trim();
        if (loc.toLowerCase() !== entity.toLowerCase()) continue;
      }
    } else if (isKnownZone) {
      if (!mapped) continue;
      if (mapped.zone !== entity) continue;
    } else {
      // Individual rep — match by display name
      const norm = normByDisplayByFY[fy]?.[entity] || normByDisplayByFY["FY26"]?.[entity] || normByDisplayByFY["FY27"]?.[entity];
      if (!norm || salesPerson !== norm) continue;
    }

    // ── Metric value ──
    let metricVal = 0;
    if (metric === "Shipments") metricVal = 1;
    else if (isAirMetric) metricVal = row.volume || 0;
    else metricVal = row.teu || 0;

    matchedRows.push({ ...row, m: metricVal });
  }

  matchedRows.sort((a, b) => b._date - a._date);
  const totalMetric  = matchedRows.reduce((s,r)=>s+(r.m||0), 0);
  const totalGP      = matchedRows.reduce((s,r)=>s+(r.g||0), 0);
  const totalRevenue = matchedRows.reduce((s,r)=>s+(r.r||0), 0);
  const totalCost    = totalRevenue - totalGP;

  return {
    success: true, entity, metric, month,
    count: matchedRows.length,
    totalMetric, totalGP, totalRevenue, totalCost,
    rows: matchedRows.slice(0, 6000),
  };

}

async function getSalesAggregate(db, force) {
  if (!force && salesCache && (Date.now() - salesCacheTime) < SALES_CACHE_TTL_MS) {
    return { ...salesCache, cached: true };
  }


  const result = await computeSalesAggregate(db);
  salesCache = result;
  salesCacheTime = Date.now();
  return result;
}

// Given a month label like "Apr-26", returns which fiscal year it belongs
// to: "FY26" covers Apr-25→Mar-26, "FY27" covers Apr-26→Mar-27.
function fyForMonthLabel(monthLabel) {
  const fy26Months = new Set(["Apr-25","May-25","Jun-25","Jul-25","Aug-25","Sep-25","Oct-25","Nov-25","Dec-25","Jan-26","Feb-26","Mar-26"]);
  return fy26Months.has(monthLabel) ? "FY26" : "FY27";
}

async function computeSalesAggregate(db) {
  // 1. Load sales rep mapping — built as ONE lookup PER FISCAL YEAR, since
  // FY26 and FY27 mapping rows now coexist in the same collection and a
  // rep's zone/target can legitimately differ between the two years.
  const mappingRows = await db.collection("mapping_sales_targets").find({}).toArray();
  const repLookupByFY = { FY26: {}, FY27: {} };
  for (const row of mappingRows) {
    const key = normalizeName(row["Sales Rep Name"]);
    if (!key) continue;
    const fy = (row._fy === "FY27") ? "FY27" : "FY26"; // untagged/legacy rows default to FY26
    if (!repLookupByFY[fy]) repLookupByFY[fy] = {};
    repLookupByFY[fy][key] = {
      displayName:   String(row["Display Name"] || row["Sales Rep Name"] || "").trim(),
      zone:          String(row["Zone"] || "Unassigned").trim(),
      lob:           String(row["LOB"] || "").trim(),
      monthlyTarget: (parseFloat(row["Monhtly Target (USD)"] || row["Monthly Target (USD)"] || 0) || 0) * USD_TO_INR,
      email:         String(row["Email ID"] || "").toLowerCase().trim(),
    };
  }

  // 2. Load zone targets — same per-FY split
  const zoneTargetRows = await db.collection("mapping_zone_targets").find({}).toArray();
  const zoneTargetsByFY = { FY26: {}, FY27: {} };
  for (const row of zoneTargetRows) {
    const zone = String(row["Zone"] || "").trim();
    if (!zone) continue;
    const fy = (row._fy === "FY27") ? "FY27" : "FY26";
    if (!zoneTargetsByFY[fy]) zoneTargetsByFY[fy] = {};
    zoneTargetsByFY[fy][zone] = {
      yearlyTarget:  (parseFloat(row["Yearly Target (USD)"]  || 0) || 0) * USD_TO_INR,
      monthlyTarget: (parseFloat(row["Monthly Target (USD)"] || 0) || 0) * USD_TO_INR,
    };
  }

  // 3. Aggregate job data per rep per month
  const repMonthData = {};
  const repMeta      = {};
  const unmapped     = {};

  const CROSS_SALES_ZONE = "Cross Sales";
  const branchMonthData = {}; // branchName → { monthLabel → { gp, ship, tons, teu, lcl } }
  const repWeekData   = {}; // repKey → { "2026-W14" → { gp, ship, tons, teu, lcl }, ... }
  const branchWeekData= {}; // branchName → { "2026-W14" → { gp, ship, tons, teu, lcl }, ... }
  const repLobData    = {}; // repKey → { "SEA EXPORT" → { "Apr-25" → {gp,ship,tons,teu,lcl} } }
  const branchLobData = {}; // branchName → { "SEA EXPORT" → { "Apr-25" → {...} } }

  // True ISO 8601 week number (Mon-Sun), independent of calendar month —
  // a week is identified by "<isoYear>-W<NN>" and can span two months.
  // Returns { key, weekNum, isoYear, monday (Date), sunday (Date) }.
  function isoWeekInfo(date) {
    var d = new Date(Date.UTC(date.getFullYear(), date.getMonth(), date.getDate()));
    var dayNum = (d.getUTCDay() + 6) % 7; // Mon=0 .. Sun=6
    var thursday = new Date(d);
    thursday.setUTCDate(d.getUTCDate() - dayNum + 3);
    var isoYear = thursday.getUTCFullYear();
    var firstThursday = new Date(Date.UTC(isoYear, 0, 4));
    var fThDayNum = (firstThursday.getUTCDay() + 6) % 7;
    firstThursday.setUTCDate(firstThursday.getUTCDate() - fThDayNum + 3);
    var weekNum = 1 + Math.round((thursday - firstThursday) / (7 * 86400000));
    var monday = new Date(d);
    monday.setUTCDate(d.getUTCDate() - dayNum);
    var sunday = new Date(monday);
    sunday.setUTCDate(monday.getUTCDate() + 6);
    var key = isoYear + '-W' + String(weekNum).padStart(2, '0');
    return { key: key, weekNum: weekNum, isoYear: isoYear, monday: monday, sunday: sunday };
  }

  for (const collName of JOB_COLLECTIONS) {
    // Fetch every field any classification might need — we don't know which
    // branch a row falls into until we read its LOB, so project broadly.
    const jobs = await db.collection(collName).find(
      {},
      { projection: {
          "Sales Person": 1, "Shipment No": 1, "Job Date": 1, "LOB": 1, "Location": 1, "Cargo Type": 1,
          "Actual Profit (J=C-G)": 1, "Provisional Profit (I=A-E)": 1, "Financial Lock": 1, "Operation Lock": 1,
          "ETD Loading Port": 1, "ETA Discharge": 1,
          "Chargeable Weight": 1, "Chargeable Weight Unit": 1,
          "Container TEU": 1, "Volume": 1, "Volume Unit": 1,
        }
      }
    ).toArray();

    for (const job of jobs) {
      const salesPerson = normalizeName(job["Sales Person"]);
      if (!salesPerson) continue;

      // Classify this row by its own LOB column (or _tab for ISO Tank),
      // NOT by which collection it happens to be stored in — protects
      // against rows that were misfiled into the wrong sheet tab.
      const cls = classifyRow(job, collName);
      const dateCol = getDateColumnFor(cls);

      // Primary date column for this row's classification, falling back to Job Date if blank
      let monthLabel = null;
      let rowDate = null;
      const primaryDate = job[dateCol];
      const fallbackDate = job["Job Date"];
      const rawDate = primaryDate || fallbackDate;
      if (rawDate) {
        const d = new Date(rawDate);
        if (!isNaN(d.getTime())) {
          monthLabel = MONTH_NAMES[d.getMonth()] + "-" + String(d.getFullYear()).slice(2);
          rowDate = d;
        }
      }
      if (!monthLabel || !FY_MONTHS.includes(monthLabel)) continue;

      // Pick the mapping lookup matching THIS row's own fiscal year — a rep's
      // zone/target can differ between FY26 and FY27, so the right lookup
      // table depends on which year this specific job row's date falls in.
      const rowFY = fyForMonthLabel(monthLabel);
      const repLookup = repLookupByFY[rowFY] || {};
      const mapped = repLookup[salesPerson];

      const { gp, isProvisional } = pickGP(job, cls);
      const gpProv   = isProvisional ? gp : 0;
      const gpActual = isProvisional ? 0 : gp;

      // Tons — only for AIR rows, Chargeable Weight (kg) ÷ 1000
      let tons = 0;
      if (isAirRow(cls)) {
        const rawWeight = parseFloat(job["Chargeable Weight"] || 0) || 0;
        const unit = String(job["Chargeable Weight Unit"] || "").toLowerCase().trim();
        // Assume kg unless explicitly marked as already in tons/lb
        if (unit === "ton" || unit === "tons" || unit === "mt") {
          tons = rawWeight;
        } else if (unit === "lb" || unit === "lbs") {
          tons = rawWeight * 0.000453592;
        } else {
          tons = rawWeight / 1000; // default: kg → tons
        }
      }

      // TEU / LCL(CBM) — only for SEA and ISOTANK rows, but WHICH of the two
      // a row contributes to is decided by its Cargo Type, not its LOB kind:
      //   Cargo Type = FCL or "Liquid (Cont)" → Container TEU column
      //   Cargo Type = LCL                    → Volume column (assumed CBM)
      let teu = 0;
      let lcl = 0;
      if (hasTeuLclRow(cls)) {
        const cargoMetric = cargoMetricFor(job);
        if (cargoMetric === "TEU") {
          teu = parseFloat(job["Container TEU"] || 0) || 0;
        } else if (cargoMetric === "LCL") {
          const vol = parseFloat(job["Volume"] || 0) || 0;
          const volUnit = String(job["Volume Unit"] || "").toUpperCase().trim();
          // Only count as LCL(CBM) if the unit is actually CBM (or blank, assumed CBM)
          if (!volUnit || volUnit === "CBM") lcl = vol;
        }
      }

      if (!mapped) {
        // Unmapped sales person → Cross Sales, grouped by branch (Location)
        unmapped[job["Sales Person"]] = (unmapped[job["Sales Person"]] || 0) + 1;

        const branch = String(job["Location"] || "Unspecified").trim() || "Unspecified";
        if (!branchMonthData[branch]) branchMonthData[branch] = {};
        if (!branchMonthData[branch][monthLabel]) branchMonthData[branch][monthLabel] = { gp: 0, gpProv: 0, gpActual: 0, ship: 0, tons: 0, teu: 0, lcl: 0 };
        branchMonthData[branch][monthLabel].gp   += gp;
        branchMonthData[branch][monthLabel].gpProv   += gpProv;
        branchMonthData[branch][monthLabel].gpActual += gpActual;
        branchMonthData[branch][monthLabel].ship += 1;
        branchMonthData[branch][monthLabel].tons += tons;
        branchMonthData[branch][monthLabel].teu  += teu;
        branchMonthData[branch][monthLabel].lcl  += lcl;
        // Weekly accumulation — keyed by true ISO week (Mon-Sun), e.g. "2026-W14"
        if (rowDate) {
          const wk = isoWeekInfo(rowDate).key;
          if (!branchWeekData[branch]) branchWeekData[branch] = {};
          if (!branchWeekData[branch][wk]) branchWeekData[branch][wk] = { gp:0, gpProv:0, gpActual:0, ship:0, tons:0, teu:0, lcl:0 };
          branchWeekData[branch][wk].gp += gp; branchWeekData[branch][wk].gpProv += gpProv; branchWeekData[branch][wk].gpActual += gpActual; branchWeekData[branch][wk].ship += 1;
          branchWeekData[branch][wk].tons += tons; branchWeekData[branch][wk].teu += teu; branchWeekData[branch][wk].lcl += lcl;
        }
        // LOB accumulation (Sea Export/Import, ISOTANK Export/Import, Air Export/Import)
        {
          const lobKey = cls.kind + (cls.direction ? " " + cls.direction : "");
          if (!branchLobData[branch]) branchLobData[branch] = {};
          if (!branchLobData[branch][lobKey]) branchLobData[branch][lobKey] = {};
          if (!branchLobData[branch][lobKey][monthLabel]) branchLobData[branch][lobKey][monthLabel] = { gp:0, gpProv:0, gpActual:0, ship:0, tons:0, teu:0, lcl:0 };
          branchLobData[branch][lobKey][monthLabel].gp   += gp;
          branchLobData[branch][lobKey][monthLabel].gpProv   += gpProv;
          branchLobData[branch][lobKey][monthLabel].gpActual += gpActual;
          branchLobData[branch][lobKey][monthLabel].ship += 1;
          branchLobData[branch][lobKey][monthLabel].tons += tons;
          branchLobData[branch][lobKey][monthLabel].teu  += teu;
          branchLobData[branch][lobKey][monthLabel].lcl  += lcl;
        }
        continue;
      }

      const repKey = mapped.displayName + "||" + mapped.zone;

      if (!repMonthData[repKey]) repMonthData[repKey] = {};
      if (!repMonthData[repKey][monthLabel]) repMonthData[repKey][monthLabel] = { gp: 0, gpProv: 0, gpActual: 0, ship: 0, tons: 0, teu: 0, lcl: 0 };
      repMonthData[repKey][monthLabel].gp   += gp;
      repMonthData[repKey][monthLabel].gpProv   += gpProv;
      repMonthData[repKey][monthLabel].gpActual += gpActual;
      repMonthData[repKey][monthLabel].ship += 1;
      repMonthData[repKey][monthLabel].tons += tons;
      repMonthData[repKey][monthLabel].teu  += teu;
      repMonthData[repKey][monthLabel].lcl  += lcl;
      // Weekly accumulation — keyed by true ISO week (Mon-Sun), e.g. "2026-W14"
      if (rowDate) {
        const wk = isoWeekInfo(rowDate).key;
        if (!repWeekData[repKey]) repWeekData[repKey] = {};
        if (!repWeekData[repKey][wk]) repWeekData[repKey][wk] = { gp:0, gpProv:0, gpActual:0, ship:0, tons:0, teu:0, lcl:0 };
        repWeekData[repKey][wk].gp += gp; repWeekData[repKey][wk].gpProv += gpProv; repWeekData[repKey][wk].gpActual += gpActual; repWeekData[repKey][wk].ship += 1;
        repWeekData[repKey][wk].tons += tons; repWeekData[repKey][wk].teu += teu; repWeekData[repKey][wk].lcl += lcl;
      }
      // LOB accumulation
      {
        const lobKey = cls.kind + (cls.direction ? " " + cls.direction : "");
        if (!repLobData[repKey]) repLobData[repKey] = {};
        if (!repLobData[repKey][lobKey]) repLobData[repKey][lobKey] = {};
        if (!repLobData[repKey][lobKey][monthLabel]) repLobData[repKey][lobKey][monthLabel] = { gp:0, gpProv:0, gpActual:0, ship:0, tons:0, teu:0, lcl:0 };
        repLobData[repKey][lobKey][monthLabel].gp   += gp;
        repLobData[repKey][lobKey][monthLabel].gpProv   += gpProv;
        repLobData[repKey][lobKey][monthLabel].gpActual += gpActual;
        repLobData[repKey][lobKey][monthLabel].ship += 1;
        repLobData[repKey][lobKey][monthLabel].tons += tons;
        repLobData[repKey][lobKey][monthLabel].teu  += teu;
        repLobData[repKey][lobKey][monthLabel].lcl  += lcl;
      }

      if (!repMeta[repKey]) repMeta[repKey] = mapped;
    }
  }

  // 4. Shape into repsRaw
  const activeMonths = FY_MONTHS.filter(m =>
    Object.values(repMonthData).some(d => d[m]) ||
    Object.values(branchMonthData).some(d => d[m])
  );

  const repsRaw = [];
  for (const [repKey, monthData] of Object.entries(repMonthData)) {
    const meta = repMeta[repKey];
    const gp      = activeMonths.map(m => monthData[m]?.gp      || 0);
    const gpProv  = activeMonths.map(m => monthData[m]?.gpProv  || 0);
    const gpActual= activeMonths.map(m => monthData[m]?.gpActual|| 0);
    const ship = activeMonths.map(m => monthData[m]?.ship || 0);
    const tons = activeMonths.map(m => Math.round((monthData[m]?.tons || 0) * 100) / 100);
    const teu  = activeMonths.map(m => Math.round((monthData[m]?.teu  || 0) * 100) / 100);
    const lcl  = activeMonths.map(m => Math.round((monthData[m]?.lcl  || 0) * 100) / 100);

    repsRaw.push({
      name:  meta.displayName,
      zone:  meta.zone,
      lob:   meta.lob,
      email: meta.email,
      hue:   zoneHue(meta.zone),
      gp, gpProv, gpActual, ship, tons, teu, lcl,
      tank:  activeMonths.map(() => 0),
      tgt:   0,
      weekData: repWeekData[repKey] || {},
      lobData: repLobData[repKey] || {}, // { "SEA EXPORT" → { "Apr-25" → {gp,ship,tons,teu,lcl} } }
    });
  }

  // Cross Sales branches
  for (const [branchName, monthData] of Object.entries(branchMonthData)) {
    const gp      = activeMonths.map(m => monthData[m]?.gp      || 0);
    const gpProv  = activeMonths.map(m => monthData[m]?.gpProv  || 0);
    const gpActual= activeMonths.map(m => monthData[m]?.gpActual|| 0);
    const ship = activeMonths.map(m => monthData[m]?.ship || 0);
    const tons = activeMonths.map(m => Math.round((monthData[m]?.tons || 0) * 100) / 100);
    const teu  = activeMonths.map(m => Math.round((monthData[m]?.teu  || 0) * 100) / 100);
    const lcl  = activeMonths.map(m => Math.round((monthData[m]?.lcl  || 0) * 100) / 100);

    repsRaw.push({
      name:  branchName,
      zone:  CROSS_SALES_ZONE,
      lob:   "",
      email: "",
      hue:   zoneHue(CROSS_SALES_ZONE + branchName),
      gp, gpProv, gpActual, ship, tons, teu, lcl,
      tank:  activeMonths.map(() => 0),
      tgt:   0,
      isBranch: true,
      weekData: branchWeekData[branchName] || {},
      lobData: branchLobData[branchName] || {},
    });
  }

  repsRaw.sort((a, b) => {
    // Cross Sales always sorts last, after every real zone
    if (a.zone === CROSS_SALES_ZONE && b.zone !== CROSS_SALES_ZONE) return 1;
    if (b.zone === CROSS_SALES_ZONE && a.zone !== CROSS_SALES_ZONE) return -1;
    if (a.zone !== b.zone) return a.zone.localeCompare(b.zone);
    const aGP = a.gp.reduce((s, v) => s + v, 0);
    const bGP = b.gp.reduce((s, v) => s + v, 0);
    return bGP - aGP;
  });

  // 5. Build zone summaries
  // Zone targets: prefer FY27's value if present, else fall back to FY26's —
  // since a single zone-summary row spans months from both fiscal years,
  // there's no single "correct" FY to pull from; defaulting to the more
  // recent year's target is the most useful choice for an at-a-glance number.
  const zonesMap = {};
  for (const rep of repsRaw) {
    if (!zonesMap[rep.zone]) {
      const fy27Target = zoneTargetsByFY.FY27[rep.zone];
      const fy26Target = zoneTargetsByFY.FY26[rep.zone];
      const chosenTarget = fy27Target || fy26Target || {};
      zonesMap[rep.zone] = {
        zone: rep.zone,
        monthlyTarget: chosenTarget.monthlyTarget || 0,
        yearlyTarget:  chosenTarget.yearlyTarget  || 0,
        gp:   activeMonths.map(() => 0),
        ship: activeMonths.map(() => 0),
        tons: activeMonths.map(() => 0),
        teu:  activeMonths.map(() => 0),
        lcl:  activeMonths.map(() => 0),
      };
    }
    rep.gp.forEach((v, i)   => { zonesMap[rep.zone].gp[i]   += v; });
    rep.ship.forEach((v, i) => { zonesMap[rep.zone].ship[i] += v; });
    rep.tons.forEach((v, i) => { zonesMap[rep.zone].tons[i] += v; });
    rep.teu.forEach((v, i)  => { zonesMap[rep.zone].teu[i]  += v; });
    rep.lcl.forEach((v, i)  => { zonesMap[rep.zone].lcl[i]  += v; });
  }

  return {
    success:  true,
    months:   activeMonths,
    repsRaw,
    zones:    Object.values(zonesMap),
    unmapped: Object.entries(unmapped)
      .sort((a, b) => b[1] - a[1])
      .map(([name, count]) => ({ name, count })),
    pushedAt: new Date().toISOString(),
  };
}

// In-memory cache for customer aggregate
let customerCache = null;
let customerCacheTime = 0;

// Used by Customer Insights (separate from the LOB-based sales aggregation above) —
// Customer Insights is intentionally scoped to Air Export + Air Import only.
const CUSTOMER_INSIGHTS_COLLECTIONS = ["jobs_air_export", "jobs_air_import"];

async function getCustomerAggregate(db, force) {
  if (!force && customerCache && (Date.now() - customerCacheTime) < SALES_CACHE_TTL_MS) {
    return { ...customerCache, cached: true };
  }
  const result = await computeCustomerAggregate(db);
  customerCache = result;
  customerCacheTime = Date.now();
  return result;
}

async function computeCustomerAggregate(db) {
  // Scoped to Air Export + Air Import only (current business scope)
  const custMap = {}; // customer name → { shipments, revenue, gp }

  for (const collName of CUSTOMER_INSIGHTS_COLLECTIONS) {
    const jobs = await db.collection(collName).find(
      {},
      { projection: {
          "Customer": 1,
          "Billed Revenue (C)": 1,
          "Actual Profit (J=C-G)": 1, "Provisional Profit (I=A-E)": 1, "Financial Lock": 1, "Operation Lock": 1,
        }
      }
    ).toArray();

    for (const job of jobs) {
      const customer = String(job["Customer"] || "").trim();
      if (!customer) continue;

      const revenue = parseFloat(job["Billed Revenue (C)"] || 0) || 0;
      const { gp }  = pickGP(job, cls);

      if (!custMap[customer]) custMap[customer] = { shipments: 0, revenue: 0, gp: 0 };
      custMap[customer].shipments += 1;
      custMap[customer].revenue   += revenue;
      custMap[customer].gp        += gp;
    }
  }

  const customers = Object.entries(custMap).map(([name, d]) => ({
    name,
    shipments: d.shipments,
    revenue:   Math.round(d.revenue),
    gp:        Math.round(d.gp),
    gpPct:     d.revenue > 0 ? Math.round((d.gp / d.revenue) * 1000) / 10 : 0,
  }));

  function top10(arr, key) {
    return [...arr].sort((a, b) => b[key] - a[key]).slice(0, 10);
  }

  return {
    success: true,
    topByShipments: top10(customers, "shipments"),
    topByRevenue:   top10(customers, "revenue"),
    topByGP:        top10(customers, "gp"),
    topByGPPct:     top10(customers.filter(c => c.shipments >= 2), "gpPct"), // filter noise from 1-off jobs
    totalCustomers: customers.length,
    pushedAt: new Date().toISOString(),
  };
}

// In-memory cache for usage analytics
let usageCache = null;
let usageCacheTime = 0;

async function getUsageAnalytics(db, force) {
  if (!force && usageCache && (Date.now() - usageCacheTime) < SALES_CACHE_TTL_MS) {
    return { ...usageCache, cached: true };
  }
  const result = await computeUsageAnalytics(db);
  usageCache = result;
  usageCacheTime = Date.now();
  return result;
}

async function computeUsageAnalytics(db) {
  const users = await db.collection("users").find({}).toArray();

  const sevenDaysAgo = new Date(Date.now() - 7 * 24 * 60 * 60 * 1000);

  const userRows = users.map((u, i) => ({
    index:       i + 1,
    name:        u.name || "",
    email:       u.email || "",
    role:        u.role || "user",
    totalLogins: u.loginCount || 0,
    lastLogin:   u.lastLogin ? u.lastLogin.toISOString() : null,
    isActive:    u.isActive !== false,
  }));

  // Sort by total logins descending, matching the reference screenshot
  userRows.sort((a, b) => b.totalLogins - a.totalLogins);
  userRows.forEach((u, i) => { u.index = i + 1; });

  const totalUsers   = userRows.length;
  const activeUsers  = userRows.filter(u => u.isActive).length;
  const loggedIn7Day = userRows.filter(u => u.lastLogin && new Date(u.lastLogin) >= sevenDaysAgo).length;

  return {
    success: true,
    totalUsers,
    activeUsers,
    loggedIn7Day,
    users: userRows,
    pushedAt: new Date().toISOString(),
  };
}

module.exports = async function handler(req, res) {
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Access-Control-Allow-Methods", "GET, POST, OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type");

  if (req.method === "OPTIONS") return res.status(200).end();

  const action = req.query?.action || req.body?.action;
  if (!action) return res.status(400).json({ error: "action required: jobs | mapping | users | sales" });

  // Lightweight ping — warms up the serverless function + DB connection.
  // Also proactively refreshes the sales cache in the background so users
  // never hit a cold, slow full-aggregation on their first page load.
  if (action === "ping") {
    const db = await getDB(); // ensures connection is alive
    // Fire-and-forget cache warm-up — don't block the ping response on it.
    // Only refreshes if the cache is close to expiring, to avoid redundant work.
    if (!salesCache || (Date.now() - salesCacheTime) > (SALES_CACHE_TTL_MS - 20 * 60 * 1000)) {
      getSalesAggregate(db, false).catch(() => {}); // best-effort, errors ignored
    }
    return res.status(200).json({ ok: true, ts: Date.now() });
  }

  // "sales" is a read action — allow GET. Everything else requires POST.
  const READ_ONLY_ACTIONS = new Set(["sales", "meta", "debug", "customers", "usage", "org", "lobCheck", "drill", "ping"]);
  if (!READ_ONLY_ACTIONS.has(action) && req.method !== "POST") {
    return res.status(405).json({ error: "Use POST for this action." });
  }

  try {
    const db = await getDB();

    if (action === "debug") {
      const sample = {};
      sample.mapping_sales_targets = await db.collection("mapping_sales_targets").find({}).limit(5).toArray();
      sample.mapping_zone_targets  = await db.collection("mapping_zone_targets").find({}).limit(10).toArray();
      sample.jobs_air_export_count = await db.collection("jobs_air_export").countDocuments({});
      sample.jobs_air_import_count = await db.collection("jobs_air_import").countDocuments({});
      sample.jobs_air_export_sample = await db.collection("jobs_air_export").find({}).limit(2).toArray();
      sample.jobs_air_import_sample = await db.collection("jobs_air_import").find({}).limit(2).toArray();
      return res.status(200).json(sample);
    }

    if (action === "lobCheck") {
      // Shows the actual LOB value distribution inside each collection —
      // use this to verify rows are classified correctly by classifyRow().
      const result = {};
      for (const collName of JOB_COLLECTIONS) {
        const rows = await db.collection(collName).find({}, { projection: { "LOB": 1 } }).toArray();
        const counts = {};
        for (const r of rows) {
          const lob = String(r["LOB"] || "(blank)").trim();
          counts[lob] = (counts[lob] || 0) + 1;
        }
        result[collName] = { totalRows: rows.length, lobBreakdown: counts };
      }
      return res.status(200).json({ success: true, result });
    }

    if (action === "meta") {
      const meta = await db.collection("_meta").findOne({ _id: "lastDataPush" });
      return res.status(200).json({
        success: true,
        lastUpdated: meta?.updatedAt ? meta.updatedAt.toISOString() : null,
      });
    }

    const forceRefresh = req.query?.force === "1" || req.query?.force === "true";

    if (action === "sales") {
      const result = await getSalesAggregate(db, forceRefresh);
      const includeWeek = req.query?.includeWeek === "1";
      const includeLob  = req.query?.includeLob  === "1";
      // By default, strip the heavy per-rep weekData/lobData breakdowns from
      // the response — they're only needed for Weekly view and the Filters
      // panel respectively. This keeps the default page-load payload small
      // and fast; the frontend lazy-fetches these with includeWeek=1 /
      // includeLob=1 only when the user actually switches to Weekly view or
      // opens the LOB filter. The full data stays cached server-side either way.
      if (!includeWeek || !includeLob) {
        const trimmed = { ...result, repsRaw: result.repsRaw.map(r => {
          const copy = { ...r };
          if (!includeWeek) delete copy.weekData;
          if (!includeLob)  delete copy.lobData;
          return copy;
        })};
        return res.status(200).json(trimmed);
      }
      return res.status(200).json(result);
    }

    if (action === "customers") {
      const result = await getCustomerAggregate(db, forceRefresh);
      return res.status(200).json(result);
    }

    if (action === "usage") {
      const result = await getUsageAnalytics(db, forceRefresh);
      return res.status(200).json(result);
    }

    if (action === "org") {
      const result = await getOrgChart(db);
      return res.status(200).json(result);
    }

    if (action === "drill") {
      const entity = req.query?.entity || req.body?.entity;
      const metric = req.query?.metric || req.body?.metric;
      const month  = req.query?.month  || req.body?.month;
      if (!entity || !metric || !month) {
        return res.status(400).json({ error: "entity, metric, and month are required." });
      }
      const result = await getDrillRows(db, entity, metric, month);
      return res.status(200).json(result);
    }

    if (action === "updateUser") {
      const { email, role, reportsTo } = req.body || {};
      const result = await updateUserFields(db, email, { role, reportsTo });
      if (result.error) return res.status(400).json({ error: result.error });
      return res.status(200).json({ success: true, ...result });
    }

    if (action === "jobs") {
      const { records, clearFirst = true, fy = null } = req.body || {};
      if (!records) return res.status(400).json({ error: "records required" });
      const { summary, errors } = await batchInsertJobs(db, records, clearFirst, fy);
      salesCache = null; // invalidate in-memory cache — fresh data was just pushed
      await db.collection("_meta").updateOne(
        { _id: "lastDataPush" },
        { $set: { updatedAt: new Date() } },
        { upsert: true }
      );
      return res.status(200).json({
        success: true, action: "jobs", summary,
        totalInserted: Object.values(summary).reduce((s, n) => s + n, 0),
        ...(errors.length ? { errors } : {}),
      });
    }

    if (action === "mapping") {
      const { collectionName, records, fy = null } = req.body || {};
      if (!collectionName || !records) return res.status(400).json({ error: "collectionName and records required" });
      const result = await batchInsertMapping(db, collectionName, records, fy);
      if (result.error) return res.status(400).json({ error: result.error });
      return res.status(200).json({ success: true, action: "mapping", collection: collectionName, inserted: result.inserted });
    }

    if (action === "users") {
      const { users } = req.body || {};
      if (!users) return res.status(400).json({ error: "users array required" });
      const results = await batchUpsertUsers(db, users);
      return res.status(200).json({ success: true, action: "users", inserted: results.inserted, updated: results.updated, ...(results.errors.length ? { errors: results.errors } : {}) });
    }

    return res.status(400).json({ error: `Unknown action: "${action}"` });

  } catch (err) {
    console.error("❌ Batch error:", err.message);
    return res.status(500).json({ error: "Batch operation failed", detail: err.message });
  }
};
