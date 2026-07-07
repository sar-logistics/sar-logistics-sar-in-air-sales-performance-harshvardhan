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
      zone:      u.zone  || "",
      isActive:  u.isActive !== false,
      lastLogin: u.lastLogin || null,
      loginCount: u.loginCount || 0,
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
  if (fields.role      !== undefined) allowed.role      = fields.role;
  if (fields.reportsTo !== undefined) allowed.reportsTo = String(fields.reportsTo || "").toLowerCase().trim();
  if (fields.zone      !== undefined) allowed.zone      = String(fields.zone || "").trim();
  if (fields.isActive  !== undefined) allowed.isActive  = !!fields.isActive;
  if (fields.name      !== undefined) allowed.name      = String(fields.name || "").trim();
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
let salesCacheTime = 0; // v3: Customer field added
const SALES_CACHE_TTL_MS = 120 * 60 * 1000; // 2 hours — data pushed from sheets periodically

// ── DRILL-DOWN: real job rows behind a clicked table cell ──────────
// entity: "Grand Total" | zone name | rep display name
// metric: "Shipments" | "GP" | "Tons (Air)" | "TEUs (Ocean)" | "LCL (Ocean in CBM)"
// month: e.g. "Apr-25", or "FY Total" for the whole year
// ── Drill row cache (server-side, lives in salesCache) ───────────────────────
// All pre-classified job rows are computed ONCE during computeSalesAggregate
// and stored in salesCache.drillRows. The drill endpoint then just filters
// this in-memory array — zero MongoDB queries. Since salesCache lives for
// 30 minutes and is shared across all requests to the same container, and
// since the ping endpoint pre-warms salesCache proactively, drill queries
// after the first sales load are served in <50ms.
let drillRowsCache = null; // { rows: [...], mappingData: {...}, cachedAt }
let drillRowsCacheTime = 0;

async function getDrillRows(db, entity, metric, month) {
  const CROSS_SALES_ZONE = "Cross Sales";

  // ── Use drillRowsCache (pre-classified rows from computeSalesAggregate) ──
  // If salesCache has already loaded the full dataset, drillRowsCache is
  // already populated. No MongoDB queries needed — just filter in memory.
  const now = Date.now();
  const cacheStale = (now - drillRowsCacheTime) > SALES_CACHE_TTL_MS;

  if (!drillRowsCache || cacheStale) {
    // Build drill rows cache from scratch (same DB pass as aggregate)
    const t0 = Date.now();
    const mappingRows = await db.collection("mapping_sales_targets").find({}).toArray();
    const repLookupByFY = { FY26: {}, FY27: {} };
    const repsByZoneByFY = { FY26: {}, FY27: {} };
    const normByDisplayByFY = { FY26: {}, FY27: {} };

    for (const row of mappingRows) {
      const rawName = row["Sales Rep Name"];
      const norm = normalizeName(rawName);
      if (!norm) continue;
      const fy = (row._fy === "FY27") ? "FY27" : "FY26";
      const zone = String(row["Zone"] || "Unassigned").trim();
      const displayName = String(row["Display Name"] || rawName || "").trim();
      repLookupByFY[fy][norm] = { displayName, zone };
      if (!repsByZoneByFY[fy][zone]) repsByZoneByFY[fy][zone] = [];
      if (!repsByZoneByFY[fy][zone].includes(norm)) repsByZoneByFY[fy][zone].push(norm);
      normByDisplayByFY[fy][displayName] = norm;
    }

    // Build a date string filter for the requested month window.
    // Dates are stored as strings in MongoDB, so we use string gte/lte.
    let ds = null, de = null;
    if (month === "FY Total") { ds = "2025-04-01"; de = "2027-03-31"; }
    else if (month?.startsWith("YEAR:")) { const y=2000+parseInt(month.split(":")[1],10); ds=`${y}-04-01`; de=`${y+1}-03-31`; }
    else if (month?.startsWith("WEEK:")) {
      const [iy,wn]=[parseInt(month.slice(5).split("-W")[0]),parseInt(month.slice(5).split("-W")[1])];
      const j4=new Date(Date.UTC(iy,0,4)); const j4d=(j4.getUTCDay()+6)%7;
      const w1m=new Date(j4); w1m.setUTCDate(j4.getUTCDate()-j4d);
      const mon=new Date(w1m); mon.setUTCDate(w1m.getUTCDate()+(wn-1)*7);
      const sun=new Date(mon); sun.setUTCDate(mon.getUTCDate()+6);
      ds=mon.toISOString().slice(0,10); de=sun.toISOString().slice(0,10);
    } else if (month?.startsWith("DATERANGE:")) { const b=month.split(":"); ds=b[1]; de=b[2]; }
    else if (month?.startsWith("RANGE:")) {
      const b=month.split(":"); const [mn1,yr1]=b[1].split("-"); const [mn2,yr2]=b[2].split("-");
      const mi1=MONTH_NAMES.indexOf(mn1), y1=2000+parseInt(yr1,10);
      const mi2=MONTH_NAMES.indexOf(mn2), y2=2000+parseInt(yr2,10);
      const ld=new Date(Date.UTC(y2,mi2+1,0)).getUTCDate();
      ds=`${y1}-${String(mi1+1).padStart(2,"0")}-01`; de=`${y2}-${String(mi2+1).padStart(2,"0")}-${ld}`;
    } else if (month) {
      const [mn,yr]=month.split("-"); const mi=MONTH_NAMES.indexOf(mn); const y=2000+parseInt(yr,10);
      const ld=new Date(Date.UTC(y,mi+1,0)).getUTCDate();
      ds=`${y}-${String(mi+1).padStart(2,"0")}-01`; de=`${y}-${String(mi+1).padStart(2,"0")}-${ld}`;
    }
    const mf = ds ? { $or:[
      {"ETD Loading Port":{$type:2,$gte:ds,$lte:de+"\uffff"}},
      {"ETA Discharge":   {$type:2,$gte:ds,$lte:de+"\uffff"}},
      {"Job Date":        {$type:2,$gte:ds,$lte:de+"\uffff"}},
    ]} : {};

    const allResults = await Promise.all(
      JOB_COLLECTIONS.map(c => db.collection(c).find(mf).toArray().then(r => ({ collName: c, rows: r })))
    );

    const allRows = [];
    for (const { collName, rows } of allResults) {
      for (const job of rows) {
        const cls = classifyRow(job, collName);
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
        const billedRevenue = parseFloat(job["Billed Revenue (C)"] || 0) || 0;
        const provRevenue   = parseFloat(job["Provisional Revenue (A)"] || 0) || 0;
        const provCost      = parseFloat(job["Provisional Cost (E)"] || 0) || 0;
        const postedCostRaw = job["Posted Cost (G)"];
        const postedCost    = (postedCostRaw != null && String(postedCostRaw).trim() !== "")
          ? (parseFloat(postedCostRaw) || 0)
          : (billedRevenue - (parseFloat(job["Actual Profit (J=C-G)"] || 0) || 0));
        const chargeable = parseFloat(job["Chargeable Weight"] || job["Volume"] || 0) || 0;

        allRows.push({
          _sp: salesPerson,  // normalized
          _ml: monthLabel,
          _d:  d,
          _cl: collName,
          _cls: cls,
          shipmentNo: job["Shipment No"]    || "—",
          jobDate:    job["Job Date"]       || "",
          lob: cls.kind + (cls.direction ? " " + cls.direction : ""),
          masterNo: job["Master No."]       || "",
          houseNo:  job["House No."]        || "",
          consolNo: job["Consol No."]       || "",
          cargoType: job["Cargo Type"]      || "",
          carrier:   job["Carrier"] || job["Carrier Name"] || "",
          provRevenue, billedRevenue,
          unbilledRevenue: parseFloat(job["Unbilled Revenue (D=A-C)"] ?? provRevenue - billedRevenue) || 0,
          provCost, postedCost,
          unpostedCost: parseFloat(job["Unposted Cost (H = E-G)"] ?? provCost - postedCost) || 0,
          provisionalProfit: parseFloat(job["Provisional Profit (I=A-E)"] || 0) || 0,
          actualProfit:      parseFloat(job["Actual Profit (J=C-G)"]      || 0) || 0,
          customer:     job["Customer"]            || "",
          ataDischarge: job["ATA Discharge"]       || "",
          atdLoading:   job["ATD Loading Port"]    || "",
          location:     job["Location"]            || "",
          consignee:    job["Consignee"]           || "",
          consolType:   job["Consol Type"]         || "",
          teu:  parseFloat(job["Container TEU"]    || 0) || 0,
          destAgent:    job["Destination Agent"]   || "",
          etaDischarge: job["ETA Discharge"]       || "",
          etdLoading:   job["ETD Loading Port"]    || "",
          jobOwner:     job["Job Owner"]           || "",
          jobRevRecogDate: job["Job Rev Recognition Date"] || "",
          originAgent:  job["Origin Agent"]        || "",
          salesPerson:  job["Sales Person"]        || "",
          shipper:      job["Shipper"]             || "",
          volume:  parseFloat(job["Volume"]        || 0) || 0,
          volumeUnit:   job["Volume Unit"]         || "",
          operationLock: job["Operation Lock"]     || "",
          financialLock: job["Financial Lock"]     || "",
          g: rowGP, r: billedRevenue, x: postedCost,
          t: parseFloat(job["Container TEU"] || 0) || 0,
          vol: chargeable,
          prov: isProvisional ? 1 : 0,
          customer: String(job["Customer"] || "").trim(),
        });
      }
    }

    drillRowsCache = { allRows, repLookupByFY, repsByZoneByFY, normByDisplayByFY };
    drillRowsCacheTime = now;
    console.log(`[drill] Built row cache: ${allRows.length} rows in ${Date.now()-t0}ms`);
  }

  const { allRows, repLookupByFY, repsByZoneByFY, normByDisplayByFY } = drillRowsCache;

  // ── Resolve entity ────────────────────────────────────────────────────────
  const isCrossSalesZone   = entity === CROSS_SALES_ZONE;
  const isGrandTotal       = entity === "Grand Total";
  const isKnownZone        = Object.values(repsByZoneByFY).some(d => d[entity]);
  const isKnownRep         = Object.values(normByDisplayByFY).some(d => d[entity]);
  const isCrossSalesBranch = !isCrossSalesZone && !isGrandTotal && !isKnownZone && !isKnownRep;
  const useCrossSalesPath  = isCrossSalesZone || isCrossSalesBranch;

  // ── Resolve date window ───────────────────────────────────────────────────
  const isFYTotal   = month === "FY Total";
  const isYearGroup = month?.startsWith("YEAR:");
  const yearSuffix  = isYearGroup ? month.split(":")[1] : null;
  const isRange     = month?.startsWith("RANGE:");
  const rangeParts  = isRange ? (() => { const b=month.split(":"); return {start:b[1],end:b[2]}; })() : null;
  const isWeek      = month?.startsWith("WEEK:");
  const isDateRange = month?.startsWith("DATERANGE:");
  const isAirMetric    = metric === "Tons (Air)";
  const isTeuLclMetric = metric === "TEUs (Ocean)" || metric === "LCL (Ocean in CBM)";

  let weekRange = null, drParts = null;
  if (isWeek) {
    const [iyS, wnS] = month.slice(5).split("-W");
    const iy=parseInt(iyS,10), wn=parseInt(wnS,10);
    const j4=new Date(Date.UTC(iy,0,4)); const j4d=(j4.getUTCDay()+6)%7;
    const w1m=new Date(j4); w1m.setUTCDate(j4.getUTCDate()-j4d);
    const mon=new Date(w1m); mon.setUTCDate(w1m.getUTCDate()+(wn-1)*7);
    const sun=new Date(mon); sun.setUTCDate(mon.getUTCDate()+6); sun.setUTCHours(23,59,59,999);
    weekRange={start:mon, end:sun};
  }
  if (isDateRange) {
    const b=month.split(":");
    drParts={start:new Date(b[1]+"T00:00:00Z"), end:new Date(b[2]+"T23:59:59Z")};
  }

  // ── Filter rows ───────────────────────────────────────────────────────────
  const matchedRows = [];
  for (const row of allRows) {
    if (isAirMetric    && !row._cl.includes("air")) continue;
    if (isTeuLclMetric && !row._cl.includes("sea") && !row._cl.includes("isotank")) continue;

    const d=row._d, ml=row._ml;
    if (!isFYTotal && !isYearGroup && !isRange && !isWeek && !isDateRange && ml !== month) continue;
    if (isYearGroup && yearSuffix && !ml.endsWith('-'+yearSuffix)) continue;
    if (isRange && rangeParts) {
      const fi=FY_MONTHS.indexOf(rangeParts.start), ti=FY_MONTHS.indexOf(rangeParts.end), mi=FY_MONTHS.indexOf(ml);
      if (fi<0||ti<0||mi<fi||mi>ti) continue;
    }
    if (isWeek     && weekRange && (d<weekRange.start || d>weekRange.end)) continue;
    if (isDateRange && drParts  && (d<drParts.start   || d>drParts.end))  continue;

    const sp  = row._sp;
    const fy  = fyForMonthLabel(ml);
    const mapped = repLookupByFY[fy]?.[sp] || repLookupByFY.FY26?.[sp] || repLookupByFY.FY27?.[sp];

    if (isGrandTotal) {
      // all pass
    } else if (useCrossSalesPath) {
      if (mapped) continue;
      if (isCrossSalesBranch && row.location !== entity) continue;
    } else if (isKnownZone) {
      if (!mapped || mapped.zone !== entity) continue;
    } else {
      const norm = normByDisplayByFY[fy]?.[entity] || normByDisplayByFY.FY26?.[entity] || normByDisplayByFY.FY27?.[entity];
      if (!norm || sp !== norm) continue;
    }

    let metricVal = metric === "Shipments" ? 1 : isAirMetric ? (row.vol||0) : (row.t||0);
    matchedRows.push({ ...row, m: metricVal });
  }

  matchedRows.sort((a,b) => b._d - a._d);
  const totalGP      = matchedRows.reduce((s,r)=>s+(r.g||0),0);
  const totalRevenue = matchedRows.reduce((s,r)=>s+(r.r||0),0);
  const totalMetric  = matchedRows.reduce((s,r)=>s+(r.m||0),0);

  return {
    success: true, entity, metric, month,
    count: matchedRows.length,
    totalMetric, totalGP,
    totalRevenue, totalCost: totalRevenue - totalGP,
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
  // Sort oldest→newest so newest rows overwrite old ones for the same rep+FY
  mappingRows.sort((a,b) => new Date(a._insertedAt||0) - new Date(b._insertedAt||0));
  const repLookupByFY = { FY26: {}, FY27: {} };
  for (const row of mappingRows) {
    const key = normalizeName(row["Sales Rep Name"]);
    if (!key) continue;
    const fy = (row._fy === "FY27") ? "FY27" : "FY26";
    if (!repLookupByFY[fy]) repLookupByFY[fy] = {};
    const tgtINR = parseFloat(row["Monthly Target (INR)"] || 0) || 0;
    const tgtUSD = (parseFloat(row["Monhtly Target (USD)"] || row["Monthly Target (USD)"] || 0) || 0) * USD_TO_INR;
    const monthlyTarget = tgtINR > 0 ? tgtINR : tgtUSD;
    const existing = repLookupByFY[fy][key];
    // Overwrite unless existing has a target and new one doesn't
    if (existing && existing.monthlyTarget > 0 && monthlyTarget === 0) continue;
    repLookupByFY[fy][key] = {
      displayName:   String(row["Display Name"] || row["Sales Rep Name"] || "").trim(),
      zone:          String(row["Zone"] || "Unassigned").trim(),
      lob:           String(row["LOB"] || "").trim(),
      monthlyTarget,
      email:         String(row["Email ID"] || "").toLowerCase().trim(),
    };
  }

  // 2. Load zone targets — sort oldest→newest so newest wins
  const zoneTargetRows = await db.collection("mapping_zone_targets").find({}).toArray();
  zoneTargetRows.sort((a,b) => new Date(a._insertedAt||0) - new Date(b._insertedAt||0));
  const zoneTargetsByFY = { FY26: {}, FY27: {} };
  for (const row of zoneTargetRows) {
    const zone = String(row["Zone"] || "").trim();
    if (!zone) continue;
    const fy = (row._fy === "FY27") ? "FY27" : "FY26";
    if (!zoneTargetsByFY[fy]) zoneTargetsByFY[fy] = {};
    const zTgtINR = parseFloat(row["Monthly Target (INR)"] || 0) || 0;
    const zTgtUSD = (parseFloat(row["Monthly Target (USD)"] || 0) || 0) * USD_TO_INR;
    const newMonthlyTarget = zTgtINR > 0 ? zTgtINR : zTgtUSD;
    const existing = zoneTargetsByFY[fy][zone];
    if (existing && existing.monthlyTarget > 0 && newMonthlyTarget === 0) continue;
    const yearlyINR = parseFloat(row["Yearly Target (INR)"] || 0) || 0;
    const yearlyUSD = (parseFloat(row["Yearly Target (USD)"] || 0) || 0) * USD_TO_INR;
    zoneTargetsByFY[fy][zone] = {
      yearlyTarget:  yearlyINR > 0 ? yearlyINR : yearlyUSD,
      monthlyTarget: newMonthlyTarget,
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
  // Fetch all collections IN PARALLEL with a small projection — this is the
  // biggest single performance lever for initial load time.
  const allJobResults = await Promise.all(JOB_COLLECTIONS.map(cn =>
    db.collection(cn).find({}, { projection: {
      "Sales Person":1, "Job Date":1, "LOB":1, "Location":1, "Customer":1,
      "Actual Profit (J=C-G)":1, "Provisional Profit (I=A-E)":1,
      "Financial Lock":1, "Operation Lock":1,
      "ETD Loading Port":1, "ETA Discharge":1,
      "Chargeable Weight":1, "Chargeable Weight Unit":1,
      "Container TEU":1, "Volume":1, "Volume Unit":1, "Cargo Type":1,
    }}).toArray().then(rows => ({ collName: cn, jobs: rows }))
  ));

  for (const { collName, jobs } of allJobResults) {

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

      // Pick the mapping for this row's FY — fall back to the other FY if not found
      // Many reps only have FY26 mapping even if they have FY27 job data
      const rowFY = fyForMonthLabel(monthLabel);
      const mapped = repLookupByFY[rowFY]?.[salesPerson]
                  || repLookupByFY[rowFY === 'FY27' ? 'FY26' : 'FY27']?.[salesPerson];

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
    if (!meta) continue; // skip reps in job data but missing from mapping sheet

    const gp      = activeMonths.map(m => monthData[m]?.gp      || 0);
    const gpProv  = activeMonths.map(m => monthData[m]?.gpProv  || 0);
    const gpActual= activeMonths.map(m => monthData[m]?.gpActual|| 0);
    const ship = activeMonths.map(m => monthData[m]?.ship || 0);
    const tons = activeMonths.map(m => Math.round((monthData[m]?.tons || 0) * 100) / 100);
    const teu  = activeMonths.map(m => Math.round((monthData[m]?.teu  || 0) * 100) / 100);
    const lcl  = activeMonths.map(m => Math.round((monthData[m]?.lcl  || 0) * 100) / 100);

    // Use rep's own monthly target directly from the mapping sheet
    const repTgt = meta.monthlyTarget || 0;

    repsRaw.push({
      name:  meta.displayName,
      zone:  meta.zone,
      lob:   meta.lob,
      email: meta.email,
      hue:   zoneHue(meta.zone),
      gp, gpProv, gpActual, ship, tons, teu, lcl,
      tank:  activeMonths.map(() => 0),
      tgt:   repTgt,
      weekData: repWeekData[repKey] || {},
      lobData: repLobData[repKey] || {},
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
const customerCacheMap = {}; // key → { data, time }

// Used by Customer Insights
const CUSTOMER_INSIGHTS_COLLECTIONS = ["jobs_air_export", "jobs_air_import"];

async function getCustomerAggregate(db, force, dateFrom, dateTo, cacheKey) {
  const key = cacheKey || 'all';
  const entry = customerCacheMap[key];
  if (!force && entry && (Date.now() - entry.time) < SALES_CACHE_TTL_MS) {
    return { ...entry.data, cached: true };
  }
  const result = await computeCustomerAggregate(db, dateFrom, dateTo);
  customerCacheMap[key] = { data: result, time: Date.now() };
  return result;
}

async function computeCustomerAggregate(db, dateFrom, dateTo) {
  // Parse active month labels from dateFrom/dateTo (e.g. "Jan-26" to "Jun-26")
  const FY_MONTHS_LIST = ["Apr-25","May-25","Jun-25","Jul-25","Aug-25","Sep-25",
    "Oct-25","Nov-25","Dec-25","Jan-26","Feb-26","Mar-26",
    "Apr-26","May-26","Jun-26","Jul-26","Aug-26","Sep-26",
    "Oct-26","Nov-26","Dec-26","Jan-27","Feb-27","Mar-27"];
  let activeMonthSet = null;
  if (dateFrom && dateTo) {
    const fi = FY_MONTHS_LIST.indexOf(dateFrom);
    const ti = FY_MONTHS_LIST.indexOf(dateTo);
    if (fi >= 0 && ti >= 0) {
      activeMonthSet = new Set(FY_MONTHS_LIST.slice(fi, ti + 1));
    }
  }
  // LOB groups for customer insights
  const LOB_GROUPS = {
    "Air":      ["jobs_air_export",      "jobs_air_import"],
    "Ocean":    ["jobs_sea_export",      "jobs_sea_import"],
    "ISO Tank": ["jobs_isotank_export",  "jobs_isotank_import"],
  };
  const ALL_COLLS = Object.values(LOB_GROUPS).flat();

  // custMapByLob: lob → customer → { shipments, revenue, gp, tons }
  const custMapByLob = { "Air": {}, "Ocean": {}, "ISO Tank": {} };
  const custMapAll   = {};
  // salesRepMap: customer → Set of sales rep display names
  const custSalesReps = {};
  // lobsMap: customer → Set of lob sub-labels (e.g. "Air Exp", "Sea Imp")
  const custLobLabels = {};

  const projection = {
    "Customer": 1, "Billed Revenue (C)": 1,
    "Actual Profit (J=C-G)": 1, "Provisional Profit (I=A-E)": 1,
    "Sales Person": 1,
    "Financial Lock": 1, "Operation Lock": 1,
    "Chargeable Weight": 1, "Chargeable Weight Unit": 1,
    "Container TEU": 1, "Volume": 1, "Volume Unit": 1,
    "ETD Loading Port": 1, "ETA Discharge": 1, "Job Date": 1,
  };

  await Promise.all(ALL_COLLS.map(async (collName) => {
    const lob = collName.includes("air") ? "Air"
              : collName.includes("isotank") ? "ISO Tank"
              : "Ocean";
    const isAir = lob === "Air";
    const cls = {
      kind: isAir ? "AIR" : collName.includes("isotank") ? "ISOTANK" : "SEA",
      direction: collName.includes("import") ? "IMPORT" : "EXPORT"
    };

    const jobs = await db.collection(collName).find({}, { projection }).toArray();

    const isExportColl = collName.includes("export");
    const isImportColl = collName.includes("import");
    const dateCol = isExportColl ? "ETD Loading Port" : isImportColl ? "ETA Discharge" : "Job Date";

    for (const job of jobs) {
      // Date filter — only include jobs within the selected month range
      if (activeMonthSet) {
        const rawDate = job[dateCol] || job["Job Date"];
        if (!rawDate) continue;
        const dObj = new Date(rawDate);
        if (isNaN(dObj.getTime())) continue;
        const ml = MONTH_NAMES[dObj.getMonth()] + "-" + String(dObj.getFullYear()).slice(2);
        if (!activeMonthSet.has(ml)) continue;
      }

      const customer = String(job["Customer"] || "").trim();
      if (!customer) continue;
      const revenue = parseFloat(job["Billed Revenue (C)"] || 0) || 0;
      const { gp } = pickGP(job, cls);

      // Tons for Air only
      let tons = 0;
      if (isAir) {
        const rawW = parseFloat(job["Chargeable Weight"] || 0) || 0;
        const wUnit = String(job["Chargeable Weight Unit"] || "").toLowerCase().trim();
        if (wUnit === "ton" || wUnit === "tons" || wUnit === "mt") tons = rawW;
        else if (wUnit === "lb" || wUnit === "lbs") tons = rawW * 0.000453592;
        else tons = rawW / 1000;
      }

      // TEU for Ocean/ISO Tank
      let teu = 0;
      if (!isAir) {
        teu = parseFloat(job["Container TEU"] || 0) || 0;
      }

      function addTo(map, key) {
        if (!map[key]) map[key] = { shipments:0, revenue:0, gp:0, tons:0, teu:0 };
        map[key].shipments++; map[key].revenue += revenue; map[key].gp += gp;
        map[key].tons += tons; map[key].teu += teu;
      }
      addTo(custMapByLob[lob], customer);
      addTo(custMapAll, customer);

      // Track sales reps (first pipe segment = display name)
      const rawSP = String(job["Sales Person"] || "").trim();
      const dispSP = rawSP ? rawSP.split("|")[0].trim() : "";
      if (dispSP) {
        if (!custSalesReps[customer]) custSalesReps[customer] = new Set();
        custSalesReps[customer].add(dispSP);
      }
      // Track LOB sub-labels
      const lobLabel = lob === "Air"
        ? (collName.includes("import") ? "Air Imp" : "Air Exp")
        : lob === "ISO Tank"
          ? (collName.includes("import") ? "ISO Imp" : "ISO Exp")
          : (collName.includes("import") ? "Sea Imp" : "Sea Exp");
      if (!custLobLabels[customer]) custLobLabels[customer] = new Set();
      custLobLabels[customer].add(lobLabel);
    }
  }));

  function buildStats(custMap) {
    const customers = Object.entries(custMap).map(([name, d]) => ({
      name,
      shipments: d.shipments,
      revenue:   Math.round(d.revenue),
      gp:        Math.round(d.gp),
      tons:      Math.round(d.tons * 100) / 100,
      teu:       Math.round(d.teu * 100) / 100,
      gpPct:     d.revenue > 0 ? Math.round((d.gp / d.revenue) * 1000) / 10 : 0,
      salesReps: custSalesReps[name] ? [...custSalesReps[name]] : [],
      lobs:      custLobLabels[name] ? [...custLobLabels[name]].sort() : [],
    }));
    function top10(arr, key) { return [...arr].sort((a,b)=>b[key]-a[key]).slice(0,10); }
    return {
      topByShipments: top10(customers, "shipments"),
      topByRevenue:   top10(customers, "revenue"),
      topByGP:        top10(customers, "gp"),
      topByTons:      top10(customers.filter(c=>c.tons>0), "tons"),
      topByTEU:       top10(customers.filter(c=>c.teu>0), "teu"),
      topByGPPct:     top10(customers.filter(c=>c.shipments>=2), "gpPct"),
      totalCustomers: customers.length,
      allCustomers:   [...customers].sort((a,b)=>b.gp-a.gp), // full list sorted by GP for pivot
    };
  }

  return {
    success: true,
    lobs: {
      "Air":      buildStats(custMapByLob["Air"]),
      "Ocean":    buildStats(custMapByLob["Ocean"]),
      "ISO Tank": buildStats(custMapByLob["ISO Tank"]),
    },
    // "All" = combined across all LOBs
    ...buildStats(custMapAll),
    pushedAt: new Date().toISOString(),
  };
}

// ─── Agent Insights ───────────────────────────────────────────────────────────
const agentCacheMap = {}; // key → { data, time }

async function getAgentAggregate(db, force, dateFrom, dateTo, cacheKey) {
  const key = cacheKey || 'all';
  const entry = agentCacheMap[key];
  if (!force && entry && (Date.now() - entry.time) < SALES_CACHE_TTL_MS) {
    return { ...entry.data, cached: true };
  }
  const result = await computeAgentAggregate(db, dateFrom, dateTo);
  agentCacheMap[key] = { data: result, time: Date.now() };
  return result;
}

async function computeAgentAggregate(db, dateFrom, dateTo) {
  const FY_MONTHS_LIST = ["Apr-25","May-25","Jun-25","Jul-25","Aug-25","Sep-25",
    "Oct-25","Nov-25","Dec-25","Jan-26","Feb-26","Mar-26",
    "Apr-26","May-26","Jun-26","Jul-26","Aug-26","Sep-26",
    "Oct-26","Nov-26","Dec-26","Jan-27","Feb-27","Mar-27"];
  let activeMonthSet = null;
  if (dateFrom && dateTo) {
    const fi = FY_MONTHS_LIST.indexOf(dateFrom);
    const ti = FY_MONTHS_LIST.indexOf(dateTo);
    if (fi >= 0 && ti >= 0) activeMonthSet = new Set(FY_MONTHS_LIST.slice(fi, ti + 1));
  }

  const LOB_GROUPS = {
    "Air":      ["jobs_air_export",     "jobs_air_import"],
    "Ocean":    ["jobs_sea_export",     "jobs_sea_import"],
    "ISO Tank": ["jobs_isotank_export", "jobs_isotank_import"],
  };
  const ALL_COLLS = Object.values(LOB_GROUPS).flat();

  const agentMapByLob = { "Air": {}, "Ocean": {}, "ISO Tank": {} };
  const agentMapAll   = {};
  const agentSalesReps  = {}; // agent → Set of sales rep display names
  const agentLobLabels  = {}; // agent → Set of lob sub-labels

  const projection = {
    "Destination Agent": 1, "Origin Agent": 1,
    "Billed Revenue (C)": 1,
    "Actual Profit (J=C-G)": 1, "Provisional Profit (I=A-E)": 1,
    "Financial Lock": 1, "Operation Lock": 1,
    "Sales Person": 1,
    "Chargeable Weight": 1, "Chargeable Weight Unit": 1,
    "Container TEU": 1,
    "ETD Loading Port": 1, "ETA Discharge": 1, "Job Date": 1,
  };

  await Promise.all(ALL_COLLS.map(async (collName) => {
    const lob = collName.includes("air") ? "Air"
              : collName.includes("isotank") ? "ISO Tank"
              : "Ocean";
    const isAir    = lob === "Air";
    const isExport = collName.includes("export");
    const isImport = collName.includes("import");
    const cls = {
      kind: isAir ? "AIR" : collName.includes("isotank") ? "ISOTANK" : "SEA",
      direction: isImport ? "IMPORT" : "EXPORT"
    };
    const dateCol = isExport ? "ETD Loading Port" : isImport ? "ETA Discharge" : "Job Date";
    const lobLabel = lob === "Air"
      ? (isImport ? "Air Imp" : "Air Exp")
      : lob === "ISO Tank"
        ? (isImport ? "ISO Imp" : "ISO Exp")
        : (isImport ? "Sea Imp" : "Sea Exp");

    const jobs = await db.collection(collName).find({}, { projection }).toArray();

    for (const job of jobs) {
      // Date filter
      if (activeMonthSet) {
        const rawDate = job[dateCol] || job["Job Date"];
        if (!rawDate) continue;
        const dObj = new Date(rawDate);
        if (isNaN(dObj.getTime())) continue;
        const ml = MONTH_NAMES[dObj.getMonth()] + "-" + String(dObj.getFullYear()).slice(2);
        if (!activeMonthSet.has(ml)) continue;
      }

      // Agent: Export → Destination Agent, Import → Origin Agent
      const agentRaw = isExport
        ? String(job["Destination Agent"] || "").trim()
        : String(job["Origin Agent"] || "").trim();
      if (!agentRaw) continue;

      const revenue = parseFloat(job["Billed Revenue (C)"] || 0) || 0;
      const { gp } = pickGP(job, cls);

      let tons = 0;
      if (isAir) {
        const rawW = parseFloat(job["Chargeable Weight"] || 0) || 0;
        const wUnit = String(job["Chargeable Weight Unit"] || "").toLowerCase().trim();
        if (wUnit === "ton" || wUnit === "tons" || wUnit === "mt") tons = rawW;
        else if (wUnit === "lb" || wUnit === "lbs") tons = rawW * 0.000453592;
        else tons = rawW / 1000;
      }
      let teu = 0;
      if (!isAir) teu = parseFloat(job["Container TEU"] || 0) || 0;

      function addTo(map, key) {
        if (!map[key]) map[key] = { shipments:0, revenue:0, gp:0, tons:0, teu:0 };
        map[key].shipments++; map[key].revenue += revenue; map[key].gp += gp;
        map[key].tons += tons; map[key].teu += teu;
      }
      addTo(agentMapByLob[lob], agentRaw);
      addTo(agentMapAll, agentRaw);

      // Sales rep tracking
      const rawSP = String(job["Sales Person"] || "").trim();
      const dispSP = rawSP ? rawSP.split("|")[0].trim() : "";
      if (dispSP) {
        if (!agentSalesReps[agentRaw]) agentSalesReps[agentRaw] = new Set();
        agentSalesReps[agentRaw].add(dispSP);
      }
      // LOB label tracking
      if (!agentLobLabels[agentRaw]) agentLobLabels[agentRaw] = new Set();
      agentLobLabels[agentRaw].add(lobLabel);
    }
  }));

  function buildStats(agentMap) {
    const agents = Object.entries(agentMap).map(([name, d]) => ({
      name,
      shipments: d.shipments,
      revenue:   Math.round(d.revenue),
      gp:        Math.round(d.gp),
      tons:      Math.round(d.tons * 100) / 100,
      teu:       Math.round(d.teu * 100) / 100,
      gpPct:     d.revenue > 0 ? Math.round((d.gp / d.revenue) * 1000) / 10 : 0,
      salesReps: agentSalesReps[name] ? [...agentSalesReps[name]] : [],
      lobs:      agentLobLabels[name] ? [...agentLobLabels[name]].sort() : [],
    }));
    function top10(arr, key) { return [...arr].sort((a,b)=>b[key]-a[key]).slice(0,10); }
    return {
      topByShipments: top10(agents, "shipments"),
      topByRevenue:   top10(agents, "revenue"),
      topByGP:        top10(agents, "gp"),
      topByTons:      top10(agents.filter(c=>c.tons>0), "tons"),
      topByTEU:       top10(agents.filter(c=>c.teu>0), "teu"),
      topByGPPct:     top10(agents.filter(c=>c.shipments>=2), "gpPct"),
      totalAgents:    agents.length,
      allAgents:      [...agents].sort((a,b)=>b.gp-a.gp),
    };
  }

  return {
    success: true,
    lobs: {
      "Air":      buildStats(agentMapByLob["Air"]),
      "Ocean":    buildStats(agentMapByLob["Ocean"]),
      "ISO Tank": buildStats(agentMapByLob["ISO Tank"]),
    },
    ...buildStats(agentMapAll),
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

  // Count logins from login_events for accuracy (users.loginCount can be stale)
  const loginEvents = await db.collection("login_events").find({}).toArray();
  const loginCountByEmail = {};
  loginEvents.forEach(function(e){
    var em = (e.email||'').toLowerCase().trim();
    loginCountByEmail[em] = (loginCountByEmail[em]||0) + 1;
  });

  const userRows = users.map((u, i) => ({
    index:       i + 1,
    name:        u.name || "",
    email:       u.email || "",
    role:        u.role || "user",
    totalLogins: loginCountByEmail[(u.email||'').toLowerCase().trim()] || u.loginCount || 0,
    lastLogin:   u.lastLogin ? u.lastLogin.toISOString() : null,
    isActive:    u.isActive !== false,
  }));

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

let financeCache = null;
let financeCacheTime = 0; // v3: jobOwners

let opCache = null;
let opCacheTime = 0; // v3: reset

async function getOpPendency(db, force) {
  if (!force && opCache && (Date.now() - opCacheTime) < SALES_CACHE_TTL_MS) {
    return { ...opCache, cached: true };
  }
  const result = await computePendency(db, "Operation Lock");
  opCache = result;
  opCacheTime = Date.now();
  return result;
}

async function getFinancePendency(db, force) {
  if (!force && financeCache && (Date.now() - financeCacheTime) < SALES_CACHE_TTL_MS) {
    return { ...financeCache, cached: true };
  }
  const result = await computePendency(db, "Financial Lock");
  financeCache = result;
  financeCacheTime = Date.now();
  return result;
}

async function computePendency(db, lockField) {
  // Load zone mapping from mapping_sales_targets
  const mappingRows = await db.collection("mapping_sales_targets").find(
    {}, { projection: { "Sales Rep Name": 1, "Display Name": 1, "Zone": 1, "_fy": 1 } }
  ).toArray();

  // Build two lookups from mapping:
  // 1. normalized rep name → zone
  // 2. INZ code (e.g. "INZ05") → zone name (extracted from pipe-separated Sales Rep Name fields)
  const repZoneMap = {};    // normalized name → zone
  const repDisplayMap = {}; // normalized name → display name
  const inzCodeZoneMap = {}; // "INZ05" → zone name

  for (const row of mappingRows) {
    const rawName = String(row["Sales Rep Name"] || "").trim();
    const display = String(row["Display Name"] || rawName || "").trim();
    const zone    = String(row["Zone"] || "Unassigned").trim();
    const isFY26  = row._fy !== "FY27";

    // Extract INZ code from pipe-separated Sales Rep Name: "Sachin | INZ16" → "INZ16"
    const parts = rawName.split("|").map(s => s.trim());
    for (const part of parts) {
      if (/^IN[A-Z]?\d+$/i.test(part)) {
        const code = part.toUpperCase();
        if (!inzCodeZoneMap[code] || isFY26) inzCodeZoneMap[code] = zone;
      }
    }

    // Name-based lookup (both raw and display)
    const normRaw = normalizeName(rawName);
    const normDis = normalizeName(display);
    for (const key of [normRaw, normDis]) {
      if (!key) continue;
      if (!repZoneMap[key] || isFY26) {
        repZoneMap[key]    = zone;
        repDisplayMap[key] = display;
      }
    }
  }

  // Direct lean query across all collections in parallel
  const ALL_JOB_COLLS = Object.values(COLLECTIONS);
  const repMonthMap = {}; // normalizedName → { zone, displayName, monthData: { monthLabel → { pending, done } } }
  const seenMonths = new Set();

  await Promise.all(ALL_JOB_COLLS.map(async (collName) => {
    const isExport = collName.includes("export");
    const isImport = collName.includes("import");
    const dateField = isExport ? "ETD Loading Port" : isImport ? "ETA Discharge" : "Job Date";

    const jobs = await db.collection(collName).find(
      {},
      { projection: { "Sales Person": 1, "Job Owner": 1, "Financial Lock": 1, "Operation Lock": 1, [dateField]: 1, "Job Date": 1 } }
    ).toArray();

    for (const job of jobs) {
      const rawName = String(job["Sales Person"] || "").trim();
      if (!rawName) continue;
      const norm = normalizeName(rawName);

      const rawDate = job[dateField] || job["Job Date"];
      if (!rawDate) continue;
      const d = new Date(rawDate);
      if (isNaN(d.getTime())) continue;
      const monthLabel = MONTH_NAMES[d.getMonth()] + "-" + String(d.getFullYear()).slice(2);
      if (!FY_MONTHS.includes(monthLabel)) continue;

      const flDone = job[lockField] && String(job[lockField]).trim() !== "";

      // Display name = first pipe segment of Sales Person field
      const nameParts = rawName.split("|").map(s => s.trim());
      const cleanName = nameParts[0] || rawName;

      // Zone resolution priority:
      // 1. Mapping by normalized name
      // 2. INZ code lookup built from mapping (e.g. INZ05 → INAZ05 zone)
      // 3. Extract zone code directly from Sales Person pipe segments
      let zone = repZoneMap[norm] || "";
      let displayName = repDisplayMap[norm] || cleanName;

      if (!zone) {
        // Find INZ/zone code in pipe segments and look up in inzCodeZoneMap
        for (const part of nameParts.slice(1)) {
          const trimmed = part.toUpperCase();
          if (inzCodeZoneMap[trimmed]) { zone = inzCodeZoneMap[trimmed]; break; }
          // Fallback: use the code itself as zone name if it looks like a zone
          if (/^IN[A-Z]?\d+$/.test(trimmed)) { zone = zone || trimmed; }
        }
      }
      if (!zone) zone = "Unassigned";

      const jobOwner = String(job["Job Owner"] || "").trim().split("|")[0].trim() || "";

      if (!repMonthMap[norm]) repMonthMap[norm] = { zone, displayName, monthData: {}, jobOwners: {} };
      if (!repMonthMap[norm].monthData[monthLabel]) repMonthMap[norm].monthData[monthLabel] = { pending: 0, done: 0 };
      // Track job owner breakdown
      if (jobOwner && jobOwner.toLowerCase() !== displayName.toLowerCase()) {
        if (!repMonthMap[norm].jobOwners[jobOwner]) repMonthMap[norm].jobOwners[jobOwner] = {};
        if (!repMonthMap[norm].jobOwners[jobOwner][monthLabel]) repMonthMap[norm].jobOwners[jobOwner][monthLabel] = { pending: 0, done: 0 };
      }
      if (flDone) repMonthMap[norm].monthData[monthLabel].done++;
      else        repMonthMap[norm].monthData[monthLabel].pending++;
      if (jobOwner && jobOwner.toLowerCase() !== displayName.toLowerCase() && repMonthMap[norm].jobOwners[jobOwner]) {
        if (flDone) repMonthMap[norm].jobOwners[jobOwner][monthLabel].done++;
        else        repMonthMap[norm].jobOwners[jobOwner][monthLabel].pending++;
      }
      seenMonths.add(monthLabel);
    }
  }));

  const monthsInData = FY_MONTHS.filter(m => seenMonths.has(m));

  // Build reps list with zone
  const reps = Object.values(repMonthMap).map((rep) => {
    let totalPending = 0, totalDone = 0;
    for (const m of monthsInData) {
      totalPending += (rep.monthData[m]?.pending || 0);
      totalDone    += (rep.monthData[m]?.done    || 0);
    }
    // Build job owner summaries
    const jobOwners = Object.entries(rep.jobOwners || {}).map(([ownerName, ownerMonthData]) => {
      let op = 0, od = 0;
      for (const m of monthsInData) { op += ownerMonthData[m]?.pending||0; od += ownerMonthData[m]?.done||0; }
      return { name: ownerName, monthData: ownerMonthData, totalPending: op, totalDone: od, total: op+od };
    }).filter(o => o.total > 0).sort((a,b) => b.total - a.total);
    return { name: rep.displayName, zone: rep.zone, monthData: rep.monthData, totalPending, totalDone, total: totalPending + totalDone, jobOwners };
  }).sort((a, b) => b.total - a.total);

  // Build unique zones list (ordered by zone total desc)
  const zoneMap = {};
  for (const rep of reps) {
    if (!zoneMap[rep.zone]) zoneMap[rep.zone] = { totalPending: 0, totalDone: 0, total: 0 };
    zoneMap[rep.zone].totalPending += rep.totalPending;
    zoneMap[rep.zone].totalDone    += rep.totalDone;
    zoneMap[rep.zone].total        += rep.total;
  }
  const zones = Object.entries(zoneMap)
    .sort((a, b) => b[1].total - a[1].total)
    .map(([name]) => name);

  return {
    success: true,
    months: monthsInData,
    reps,
    zones,
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
    const db = await getDB();
    const now2 = Date.now();
    // Build salesCache synchronously if cold or expiring — this keeps the cache
    // warm so that the next ?action=sales request is served instantly from memory.
    if (!salesCache || (now2 - salesCacheTime) > (SALES_CACHE_TTL_MS - 20 * 60 * 1000)) {
      await getSalesAggregate(db, false).catch(() => {});
    }
    return res.status(200).json({ ok: true, ts: now2, cached: !!salesCache });
  }

  // "sales" is a read action — allow GET. Everything else requires POST.
  const READ_ONLY_ACTIONS = new Set(["sales", "meta", "debug", "customers", "agents", "usage", "org", "lobCheck", "drill", "ping", "finance", "financeDebug", "op"]);
  if (!READ_ONLY_ACTIONS.has(action) && req.method !== "POST") {
    return res.status(405).json({ error: "Use POST for this action." });
  }

  try {
    const db = await getDB();

    if (action === "debug") {
      const sample = {};
      sample.mapping_sales_targets = await db.collection("mapping_sales_targets").find({}).toArray();
      sample.mapping_zone_targets  = await db.collection("mapping_zone_targets").find({}).toArray();
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
      const includeWeek = req.query?.includeWeek === "1";
      const includeLob  = req.query?.includeLob  === "1";

      const strip = (result) => ({ ...result, repsRaw: result.repsRaw.map(r => {
        const copy = { ...r };
        if (!includeWeek) delete copy.weekData;
        if (!includeLob)  delete copy.lobData;
        return copy;
      })});

      // Force refresh: wipe server cache first, then re-fetch from MongoDB
      if (forceRefresh) {
        salesCache = null;
        salesCacheTime = 0;
        drillRowsCache = null;
        drillRowsCacheTime = 0;
      }

      // Serve from cache unless force refresh
      if (salesCache && !forceRefresh) {
        const isStale = (Date.now() - salesCacheTime) > SALES_CACHE_TTL_MS;
        if (isStale) getSalesAggregate(db, true).catch(() => {});
        return res.status(200).json(strip(salesCache));
      }

      const result = await getSalesAggregate(db, forceRefresh);
      return res.status(200).json(strip(result));
    }

    if (action === "customers") {
      const dateFrom = req.query.dateFrom || null; // e.g. "Jan-26"
      const dateTo   = req.query.dateTo   || null; // e.g. "Jun-26"
      const cacheKey = `${dateFrom}|${dateTo}`;
      const result = await getCustomerAggregate(db, forceRefresh, dateFrom, dateTo, cacheKey);
      return res.status(200).json(result);
    }

    if (action === "agents") {
      const dateFrom = req.query.dateFrom || null;
      const dateTo   = req.query.dateTo   || null;
      const cacheKey = `${dateFrom}|${dateTo}`;
      const result = await getAgentAggregate(db, forceRefresh, dateFrom, dateTo, cacheKey);
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

    if (action === "finance") {
      const result = await getFinancePendency(db, forceRefresh);
      return res.status(200).json(result);
    }

    if (action === "op") {
      const result = await getOpPendency(db, forceRefresh);
      return res.status(200).json(result);
    }

    if (action === "financeDebug") {
      // Returns unrecognized sales person names (not in mapping)
      const mappingRows = await db.collection("mapping_sales_targets").find(
        {}, { projection: { "Sales Rep Name": 1, "Display Name": 1, "Zone": 1 } }
      ).toArray();
      const knownNorms = new Set();
      const knownDisplay = {};
      for (const row of mappingRows) {
        const n1 = normalizeName(row["Sales Rep Name"]);
        const n2 = normalizeName(row["Display Name"] || row["Sales Rep Name"]);
        if (n1) { knownNorms.add(n1); knownDisplay[n1] = { raw: row["Sales Rep Name"], display: row["Display Name"], zone: row["Zone"] }; }
        if (n2) { knownNorms.add(n2); knownDisplay[n2] = { raw: row["Sales Rep Name"], display: row["Display Name"], zone: row["Zone"] }; }
      }
      const ALL_JOB_COLLS = Object.values(COLLECTIONS);
      const unrecognized = {}; // norm → { rawNames: Set, count }
      const recognized = {};
      await Promise.all(ALL_JOB_COLLS.map(async (collName) => {
        const jobs = await db.collection(collName).find({}, { projection: { "Sales Person": 1 } }).toArray();
        for (const job of jobs) {
          const raw = String(job["Sales Person"] || "").trim();
          if (!raw) continue;
          const norm = normalizeName(raw);
          if (knownNorms.has(norm)) {
            if (!recognized[norm]) recognized[norm] = { display: knownDisplay[norm], count: 0 };
            recognized[norm].count++;
          } else {
            if (!unrecognized[norm]) unrecognized[norm] = { rawNames: [], count: 0 };
            if (!unrecognized[norm].rawNames.includes(raw)) unrecognized[norm].rawNames.push(raw);
            unrecognized[norm].count++;
          }
        }
      }));
      return res.status(200).json({
        success: true,
        unrecognized: Object.entries(unrecognized).sort((a,b) => b[1].count - a[1].count).map(([norm, v]) => ({ norm, rawNames: v.rawNames, count: v.count })),
        recognizedCount: Object.keys(recognized).length,
        mapping: mappingRows.map(r => ({ raw: r["Sales Rep Name"], display: r["Display Name"], zone: r["Zone"] }))
      });
    }

    if (action === "deleteUser") {
      const { email } = req.body || {};
      if (!email) return res.status(400).json({ error: "email required" });
      await db.collection("users").deleteOne({ email: email.toLowerCase().trim() });
      return res.status(200).json({ success: true });
    }

    if (action === "addUser") {
      const { email, name, role, zone, reportsTo } = req.body || {};
      if (!email || !name) return res.status(400).json({ error: "email and name required" });
      const newUser = {
        email: email.toLowerCase().trim(),
        name: name.trim(),
        role: role || "Sales Rep",
        zone: zone || "",
        reportsTo: (reportsTo || "").toLowerCase().trim(),
        isActive: true,
        loginCount: 0,
        createdAt: new Date(),
      };
      const existing = await db.collection("users").findOne({ email: newUser.email });
      if (existing) return res.status(400).json({ error: "User with this email already exists" });
      await db.collection("users").insertOne(newUser);
      return res.status(200).json({ success: true });
    }

    if (action === "updateUser") {
      const { email, role, reportsTo, zone, isActive, name } = req.body || {};
      const result = await updateUserFields(db, email, { role, reportsTo, zone, isActive, name });
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
