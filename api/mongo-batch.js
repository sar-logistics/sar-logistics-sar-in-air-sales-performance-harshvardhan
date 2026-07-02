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
async function getDrillRows(db, entity, metric, month) {
  const CROSS_SALES_ZONE = "Cross Sales";

  // ── 1. Load mapping — EXACT same logic as computeSalesAggregate ─────
  const mappingRows = await db.collection("mapping_sales_targets").find({}).toArray();
  const repLookupByFY      = { FY26: {}, FY27: {} };
  const repsByZoneByFY     = { FY26: {}, FY27: {} };
  const normByDisplayByFY  = { FY26: {}, FY27: {} };
  const rawNamesByNormByFY = { FY26: {}, FY27: {} };
  const allMappedNames     = new Set();

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
    repsByZoneByFY[fy][zone].push(norm);
    normByDisplayByFY[fy][displayName] = norm;
    if (!rawNamesByNormByFY[fy][norm]) rawNamesByNormByFY[fy][norm] = [];
    rawNamesByNormByFY[fy][norm].push(String(rawName || "").trim());
    allMappedNames.add(norm);
  }

  // ── 2. Resolve entity ────────────────────────────────────────────────
  const isCrossSalesZone = entity === CROSS_SALES_ZONE;
  const isGrandTotal     = entity === "Grand Total";
  const isKnownZone = Object.values(repsByZoneByFY).some(z => z[entity]);
  const isKnownRep  = Object.values(normByDisplayByFY).some(d => d[entity]);
  const isCrossSalesBranch = !isCrossSalesZone && !isGrandTotal && !isKnownZone && !isKnownRep;
  const useCrossSalesPath  = isCrossSalesZone || isCrossSalesBranch;

  // ── 3. Build MongoDB $in filter ──────────────────────────────────────
  const allRelevantRawNames = new Set();
  if (!useCrossSalesPath) {
    ["FY26","FY27"].forEach(fy => {
      let norms = [];
      if (isGrandTotal)                         norms = Object.keys(repLookupByFY[fy]);
      else if (repsByZoneByFY[fy][entity])      norms = repsByZoneByFY[fy][entity];
      else if (normByDisplayByFY[fy][entity])   norms = [normByDisplayByFY[fy][entity]];
      norms.forEach(norm =>
        (rawNamesByNormByFY[fy][norm] || []).forEach(raw => allRelevantRawNames.add(raw))
      );
    });
  }

  const isFYTotal      = month === "FY Total";
  const isYearGroup    = month && month.startsWith("YEAR:");
  const yearGroupSuffix = isYearGroup ? month.split(":")[1] : null;
  // RANGE:Jan-26:Jun-26 → include all months between start and end inclusive
  const isRange        = month && month.startsWith("RANGE:");
  let rangeParts = null;
  if (isRange) {
    const bits = month.split(":");
    rangeParts = { start: bits[1], end: bits[2] };
  }
  const isAirMetric    = metric === "Tons (Air)";
  const isTeuLclMetric = metric === "TEUs (Ocean)" || metric === "LCL (Ocean in CBM)";

  const relevantCollections = JOB_COLLECTIONS.filter(collName => {
    if (isAirMetric)    return collName.includes("air");
    if (isTeuLclMetric) return collName.includes("sea") || collName.includes("isotank");
    return true;
  });

  const baseProjection = {
    "Shipment No":1,"Sales Person":1,"Job Date":1,"LOB":1,"Location":1,"Cargo Type":1,
    "Customer":1,"Loading Port":1,"Discharge Port":1,"Actual Profit (J=C-G)":1,
    "ETD Loading Port":1,"ETA Discharge":1,
    "Chargeable Weight":1,"Chargeable Weight Unit":1,
    "Container TEU":1,"Volume":1,"Volume Unit":1,
    "Billed Revenue (C)":1,"Actual Cost (G)":1,"Provisional Profit (I=A-E)":1,"Financial Lock":1,"Operation Lock":1,
  };

  const queryPromises = relevantCollections.map(collName => {
    let filter;
    if (isGrandTotal) {
      filter = {}; // Grand Total: fetch every row, no Sales Person filter
    } else if (useCrossSalesPath) {
      filter = {}; // Cross Sales: also fetch all, filter in-memory by mapped/unmapped
    } else if (allRelevantRawNames.size > 0) {
      filter = { "Sales Person": { $in: Array.from(allRelevantRawNames) } };
    } else {
      filter = { "Sales Person": { $in: [] } }; // empty result
    }
    return db.collection(collName).find(filter, { projection: baseProjection }).toArray()
      .then(rows => ({ collName, rows }));
  });

  const resultsByCollection = await Promise.all(queryPromises);
  const matchedRows = [];

  for (const { collName, rows } of resultsByCollection) {
    for (const job of rows) {
      const salesPerson = normalizeName(job["Sales Person"]);
      if (!salesPerson) continue;

      // ── Date — EXACT same logic as computeSalesAggregate ───────────────
      const cls     = classifyRow(job, collName);
      const dateCol = getDateColumnFor(cls);
      const rawDate = job[dateCol] || job["Job Date"];
      if (!rawDate) continue;
      const d = new Date(rawDate);
      if (isNaN(d.getTime())) continue;
      const monthLabel = MONTH_NAMES[d.getMonth()] + "-" + String(d.getFullYear()).slice(2);
      if (!FY_MONTHS.includes(monthLabel)) continue;
      if (!isFYTotal && !isYearGroup && !isRange && monthLabel !== month) continue;
      if (isYearGroup && yearGroupSuffix && !monthLabel.endsWith('-' + yearGroupSuffix)) continue;
      if (isRange && rangeParts) {
        const allM = FY_MONTHS;
        const fi = allM.indexOf(rangeParts.start);
        const ti = allM.indexOf(rangeParts.end);
        const mi = allM.indexOf(monthLabel);
        if (fi < 0 || ti < 0 || mi < fi || mi > ti) continue;
      }

      // ── FY-aware mapping — EXACT same as computeSalesAggregate ─────────
      const rowFY  = fyForMonthLabel(monthLabel);
      const repLookup = repLookupByFY[rowFY] || {};
      const mapped    = repLookup[salesPerson];

      // ── Entity membership ────────────────────────────────────────────────
      if (isGrandTotal) {
        // Grand Total = every valid job row, mapped or unmapped — no filter
      } else if (useCrossSalesPath) {
        // Cross Sales zone or branch: only unmapped reps
        if (allMappedNames.has(salesPerson)) continue;
        if (isCrossSalesBranch) {
          const branch = String(job["Location"] || "Unspecified").trim() || "Unspecified";
          if (branch !== entity) continue;
        }
      } else {
        // Named zone or rep: must be mapped AND belong to that entity
        if (!mapped) continue;
        if (isKnownZone && mapped.zone !== entity)        continue;
        if (isKnownRep  && mapped.displayName !== entity) continue;
      }

      // ── Metric filtering ────────────────────────────────────────────────
      const cargoMetric = hasTeuLclRow(cls) ? cargoMetricFor(job) : null;
      if (metric === "Tons (Air)"         && !isAirRow(cls))        continue;
      if (metric === "TEUs (Ocean)"       && cargoMetric !== "TEU") continue;
      if (metric === "LCL (Ocean in CBM)" && cargoMetric !== "LCL") continue;

      // ── Metric value — EXACT same formulas as computeSalesAggregate ────
      let metricVal = 0; let metricUnit = "";
      if (metric === "Shipments") {
        metricVal = 1; metricUnit = "job";
      } else if (metric === "GP") {
        metricVal = parseFloat(job["Actual Profit (J=C-G)"] || 0) || 0; metricUnit = "currency";
      } else if (metric === "Tons (Air)") {
        const rawWeight = parseFloat(job["Chargeable Weight"] || 0) || 0;
        const unit = String(job["Chargeable Weight Unit"] || "").toLowerCase().trim();
        metricVal = (unit==="ton"||unit==="tons"||unit==="mt") ? rawWeight
                  : (unit==="lb"||unit==="lbs") ? rawWeight*0.000453592 : rawWeight/1000;
        metricUnit = "t";
      } else if (metric === "TEUs (Ocean)") {
        metricVal = parseFloat(job["Container TEU"] || 0) || 0; metricUnit = "TEU";
      } else if (metric === "LCL (Ocean in CBM)") {
        const vol = parseFloat(job["Volume"] || 0) || 0;
        const volUnit = String(job["Volume Unit"] || "").toUpperCase().trim();
        metricVal = (!volUnit || volUnit === "CBM") ? vol : 0; metricUnit = "m³";
      }
      if (metric !== "Shipments" && metric !== "GP" && metricVal === 0) continue;

      const { gp: rowGP, isProvisional } = pickGP(job, cls);
      const rowRevenue = parseFloat(job["Billed Revenue (C)"] || 0) || 0;
      const rowCost    = rowRevenue - rowGP;

      matchedRows.push({
        s: job["Shipment No"]    || "—",      // shipmentNo
        p: job["Sales Person"]   || "",        // salesPerson
        c: job["Customer"]       || "",        // customer
        o: job["Loading Port"]   || "",        // origin
        d: job["Discharge Port"] || "",        // destination
        l: cls.kind + (cls.direction ? " " + cls.direction : ""), // lob
        dt: d.toISOString().slice(0,10),       // date (date-only, saves ~15 chars)
        g: rowGP,
        r: rowRevenue,
        prov: isProvisional ? 1 : 0, // 1=provisional, 0=actual
        x: rowCost,
        t: parseFloat(job["Container TEU"] || 0) || 0,
        m: metricVal,
      });
    }
  }

  matchedRows.sort((a, b) => new Date(b.dt) - new Date(a.dt));
  const totalMetric  = matchedRows.reduce((s, r) => s + (r.m || 0), 0);
  const totalGP      = matchedRows.reduce((s, r) => s + (r.g || 0), 0);
  const totalRevenue = matchedRows.reduce((s, r) => s + (r.r || 0), 0);
  const totalCost    = totalRevenue - totalGP;

  return {
    success: true, entity, metric, month,
    count: matchedRows.length,
    totalMetric, totalGP, totalRevenue, totalCost,
    rows: matchedRows.slice(0, 25000),
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
  const repWeekData   = {}; // repKey → { "Apr-25:W1" → { gp, ship, tons, teu, lcl }, ... }
  const branchWeekData= {}; // branchName → { "Apr-25:W1" → { gp, ship, tons, teu, lcl }, ... }
  const repLobData    = {}; // repKey → { "SEA EXPORT" → { "Apr-25" → {gp,ship,tons,teu,lcl} } }
  const branchLobData = {}; // branchName → { "SEA EXPORT" → { "Apr-25" → {...} } }

  // Week label within a month: day 1-7=W1, 8-14=W2, 15-21=W3, 22+=W4
  function weekLabel(monthLabel, day){
    var w = day <= 7 ? 'W1' : day <= 14 ? 'W2' : day <= 21 ? 'W3' : 'W4';
    return monthLabel + ':' + w;
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
      let dayOfMonth = null;
      const primaryDate = job[dateCol];
      const fallbackDate = job["Job Date"];
      const rawDate = primaryDate || fallbackDate;
      if (rawDate) {
        const d = new Date(rawDate);
        if (!isNaN(d.getTime())) {
          monthLabel = MONTH_NAMES[d.getMonth()] + "-" + String(d.getFullYear()).slice(2);
          dayOfMonth = d.getDate();
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
        // Weekly accumulation
        if (dayOfMonth) {
          const wk = weekLabel(monthLabel, dayOfMonth);
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
      // Weekly accumulation
      if (dayOfMonth) {
        const wk = weekLabel(monthLabel, dayOfMonth);
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
    if (!salesCache || (Date.now() - salesCacheTime) > (SALES_CACHE_TTL_MS - 5 * 60 * 1000)) {
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
