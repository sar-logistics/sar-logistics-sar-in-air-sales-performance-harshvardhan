// ─────────────────────────────────────────────────────────────────
//  api/mongo-batch.js  —  Vercel Serverless Function
//  No auth secret — called from Google Apps Script only
// ─────────────────────────────────────────────────────────────────

const { MongoClient } = require("mongodb");

const MONGO_URI = process.env.MONGO_URI;
const DB_NAME   = "sar-in-air-sales";

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

async function batchInsertJobs(db, records, clearFirst = true) {
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
      // Only wipe on first chunk — subsequent chunks append
      if (clearFirst) await col.deleteMany({});
      const stamped = rows.map((row) => ({ ...row, _tab: tabName, _insertedAt: new Date() }));
      const result  = await col.insertMany(stamped, { ordered: false });
      summary[tabName] = result.insertedCount;
    } catch (err) {
      errors.push(`${tabName}: ${err.message}`);
      summary[tabName] = 0;
    }
  }
  return { summary, errors };
}

async function batchInsertMapping(db, collectionName, records) {
  if (!MAPPING_COLLECTIONS.has(collectionName)) {
    return { error: `Unknown mapping collection: "${collectionName}"` };
  }
  if (!Array.isArray(records) || records.length === 0) return { inserted: 0 };
  const col = db.collection(collectionName);
  await col.deleteMany({});
  const stamped = records.map((row) => ({ ...row, _insertedAt: new Date() }));
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

// Tons metric is calculated only from Air Export/Import (per current scope)
const AIR_COLLECTIONS = new Set(["jobs_air_export", "jobs_air_import"]);

// TEU and LCL(CBM) metrics are calculated only from Sea + ISO Tank collections
const TEU_LCL_COLLECTIONS = new Set([
  "jobs_sea_export", "jobs_sea_import",
  "jobs_isotank_export", "jobs_isotank_import",
]);

// Export-side collections use "ETD Loading Port" as the date column;
// Import-side collections use "ETA Discharge". Both fall back to "Job Date"
// if the primary column is blank. General/Road keep using Job Date only.
const EXPORT_COLLECTIONS = new Set([
  "jobs_sea_export", "jobs_air_export", "jobs_isotank_export", "jobs_clearance_export",
]);
const IMPORT_COLLECTIONS = new Set([
  "jobs_sea_import", "jobs_air_import", "jobs_isotank_import", "jobs_clearance_import",
]);

function getDateColumnFor(collName) {
  if (EXPORT_COLLECTIONS.has(collName)) return "ETD Loading Port";
  if (IMPORT_COLLECTIONS.has(collName)) return "ETA Discharge";
  return "Job Date"; // General, Road
}

// Indian Fiscal Year 2026 runs Apr 2025 → Mar 2026
const FY_MONTHS = [
  "Apr-25","May-25","Jun-25","Jul-25","Aug-25","Sep-25",
  "Oct-25","Nov-25","Dec-25","Jan-26","Feb-26","Mar-26"
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
const SALES_CACHE_TTL_MS = 5 * 60 * 1000; // 5 minutes

async function getSalesAggregate(db, force) {
  if (!force && salesCache && (Date.now() - salesCacheTime) < SALES_CACHE_TTL_MS) {
    return { ...salesCache, cached: true };
  }

  const result = await computeSalesAggregate(db);
  salesCache = result;
  salesCacheTime = Date.now();
  return result;
}

async function computeSalesAggregate(db) {
  // 1. Load sales rep mapping
  const mappingRows = await db.collection("mapping_sales_targets").find({}).toArray();
  const repLookup = {};
  for (const row of mappingRows) {
    const key = normalizeName(row["Sales Rep Name"]);
    if (!key) continue;
    repLookup[key] = {
      displayName:   String(row["Display Name"] || row["Sales Rep Name"] || "").trim(),
      zone:          String(row["Zone"] || "Unassigned").trim(),
      lob:           String(row["LOB"] || "").trim(),
      monthlyTarget: parseFloat(row["Monhtly Target (USD)"] || row["Monthly Target (USD)"] || 0) || 0,
      email:         String(row["Email ID"] || "").toLowerCase().trim(),
    };
  }

  // 2. Load zone targets
  const zoneTargetRows = await db.collection("mapping_zone_targets").find({}).toArray();
  const zoneTargets = {};
  for (const row of zoneTargetRows) {
    const zone = String(row["Zone"] || "").trim();
    if (!zone) continue;
    zoneTargets[zone] = {
      yearlyTarget:  parseFloat(row["Yearly Target (USD)"]  || 0) || 0,
      monthlyTarget: parseFloat(row["Monthly Target (USD)"] || 0) || 0,
    };
  }

  // 3. Aggregate job data per rep per month
  const repMonthData = {};
  const repMeta      = {};
  const unmapped     = {}; // tracks sales person names with no mapping match

  for (const collName of JOB_COLLECTIONS) {
    const isAir = AIR_COLLECTIONS.has(collName);
    const hasTeuLcl = TEU_LCL_COLLECTIONS.has(collName);
    const dateCol = getDateColumnFor(collName);

    const jobs = await db.collection(collName).find(
      {},
      { projection: {
          "Sales Person": 1, "Shipment No": 1, "Job Date": 1,
          "Actual Profit (J=C-G)": 1,
          [dateCol]: 1, // ETD Loading Port / ETA Discharge / Job Date — whichever applies
          ...(isAir ? { "Chargeable Weight": 1, "Chargeable Weight Unit": 1 } : {}),
          ...(hasTeuLcl ? { "Container TEU": 1, "Volume": 1, "Volume Unit": 1 } : {}),
        }
      }
    ).toArray();

    for (const job of jobs) {
      const salesPerson = normalizeName(job["Sales Person"]);
      if (!salesPerson) continue;

      const mapped = repLookup[salesPerson];
      if (!mapped) {
        unmapped[job["Sales Person"]] = (unmapped[job["Sales Person"]] || 0) + 1;
        continue;
      }

      const repKey = mapped.displayName + "||" + mapped.zone;

      // Primary date column for this collection type, falling back to Job Date if blank
      let monthLabel = null;
      const primaryDate = job[dateCol];
      const fallbackDate = job["Job Date"];
      const rawDate = primaryDate || fallbackDate;
      if (rawDate) {
        const d = new Date(rawDate);
        if (!isNaN(d.getTime())) {
          monthLabel = MONTH_NAMES[d.getMonth()] + "-" + String(d.getFullYear()).slice(2);
        }
      }
      if (!monthLabel || !FY_MONTHS.includes(monthLabel)) continue;

      const gp = parseFloat(job["Actual Profit (J=C-G)"] || 0) || 0;

      // Tons — only from Air Export/Import, Chargeable Weight (kg) ÷ 1000
      let tons = 0;
      if (isAir) {
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

      // TEU — only from Sea Export/Import and ISO Tank Export/Import, "Container TEU" column
      let teu = 0;
      // LCL (CBM) — same collections, "Volume" column (assumed CBM)
      let lcl = 0;
      if (hasTeuLcl) {
        teu = parseFloat(job["Container TEU"] || 0) || 0;
        const vol = parseFloat(job["Volume"] || 0) || 0;
        const volUnit = String(job["Volume Unit"] || "").toUpperCase().trim();
        // Only count as LCL(CBM) if the unit is actually CBM (or blank, assumed CBM)
        if (!volUnit || volUnit === "CBM") lcl = vol;
      }

      if (!repMonthData[repKey]) repMonthData[repKey] = {};
      if (!repMonthData[repKey][monthLabel]) repMonthData[repKey][monthLabel] = { gp: 0, ship: 0, tons: 0, teu: 0, lcl: 0 };
      repMonthData[repKey][monthLabel].gp   += gp;
      repMonthData[repKey][monthLabel].ship += 1;
      repMonthData[repKey][monthLabel].tons += tons;
      repMonthData[repKey][monthLabel].teu  += teu;
      repMonthData[repKey][monthLabel].lcl  += lcl;

      if (!repMeta[repKey]) repMeta[repKey] = mapped;
    }
  }

  // 4. Shape into repsRaw
  const activeMonths = FY_MONTHS.filter(m => Object.values(repMonthData).some(d => d[m]));

  // Count reps per zone so zone target can be split evenly across them
  const repsPerZone = {};
  for (const meta of Object.values(repMeta)) {
    repsPerZone[meta.zone] = (repsPerZone[meta.zone] || 0) + 1;
  }

  const repsRaw = [];
  for (const [repKey, monthData] of Object.entries(repMonthData)) {
    const meta = repMeta[repKey];
    const gp   = activeMonths.map(m => Math.round(monthData[m]?.gp || 0));
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
      gp, ship, tons, teu, lcl,
      tank:  activeMonths.map(() => 0),
      tgt:   0, // rep-level targets ignored — Target/Achievement shown at Zone level only
    });
  }

  repsRaw.sort((a, b) => {
    if (a.zone !== b.zone) return a.zone.localeCompare(b.zone);
    const aGP = a.gp.reduce((s, v) => s + v, 0);
    const bGP = b.gp.reduce((s, v) => s + v, 0);
    return bGP - aGP;
  });

  // 5. Build zone summaries
  const zonesMap = {};
  for (const rep of repsRaw) {
    if (!zonesMap[rep.zone]) {
      zonesMap[rep.zone] = {
        zone: rep.zone,
        monthlyTarget: zoneTargets[rep.zone]?.monthlyTarget || 0,
        yearlyTarget:  zoneTargets[rep.zone]?.yearlyTarget  || 0,
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

  for (const collName of AIR_COLLECTIONS) {
    const jobs = await db.collection(collName).find(
      {},
      { projection: {
          "Customer": 1,
          "Billed Revenue (C)": 1,
          "Actual Profit (J=C-G)": 1,
        }
      }
    ).toArray();

    for (const job of jobs) {
      const customer = String(job["Customer"] || "").trim();
      if (!customer) continue;

      const revenue = parseFloat(job["Billed Revenue (C)"] || 0) || 0;
      const gp      = parseFloat(job["Actual Profit (J=C-G)"] || 0) || 0;

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

  // "sales" is a read action — allow GET. Everything else requires POST.
  const READ_ONLY_ACTIONS = new Set(["sales", "meta", "debug", "customers", "usage", "org"]);
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

    if (action === "updateUser") {
      const { email, role, reportsTo } = req.body || {};
      const result = await updateUserFields(db, email, { role, reportsTo });
      if (result.error) return res.status(400).json({ error: result.error });
      return res.status(200).json({ success: true, ...result });
    }

    if (action === "jobs") {
      const { records, clearFirst = true } = req.body || {};
      if (!records) return res.status(400).json({ error: "records required" });
      const { summary, errors } = await batchInsertJobs(db, records, clearFirst);
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
      const { collectionName, records } = req.body || {};
      if (!collectionName || !records) return res.status(400).json({ error: "collectionName and records required" });
      const result = await batchInsertMapping(db, collectionName, records);
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
