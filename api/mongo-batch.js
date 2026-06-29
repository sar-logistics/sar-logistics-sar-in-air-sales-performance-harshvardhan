// ─────────────────────────────────────────────────────────────────
//  api/mongo-batch.js  —  Vercel Serverless Function
//  Called by Apps Script only (not the browser).
//
//  Two actions via ?action= query param:
//
//  POST /api/mongo-batch?action=jobs
//  Body: { records: { "Sea Export": [...rows], "Air Import": [...] } }
//  → Clears + rebuilds each FY26 job collection from Sheet data
//
//  POST /api/mongo-batch?action=users
//  Body: { users: [ { name, email, role } ] }
//  → Upserts users (insert if new, update if exists)
//
//  All requests must include header:  x-batch-secret: <BATCH_SECRET>
// ─────────────────────────────────────────────────────────────────

const { MongoClient } = require("mongodb");

const MONGO_URI    = process.env.MONGO_URI;
const BATCH_SECRET = process.env.BATCH_SECRET;
const DB_NAME      = "sar-in-air-sales";

// One MongoDB collection per destination tab
const COLLECTIONS = {
  "Sea Export":        "jobs_sea_export",
  "Sea Import":        "jobs_sea_import",
  "Air Export":        "jobs_air_export",
  "Air Import":        "jobs_air_import",
  "ISO Tank - Export": "jobs_isotank_export",
  "ISO Tank - Import": "jobs_isotank_import",
  "General":           "jobs_general",
};

// Reuse connection across warm invocations
let cachedClient = null;

async function getDB() {
  if (cachedClient) return cachedClient.db(DB_NAME);
  cachedClient = new MongoClient(MONGO_URI, {
    connectTimeoutMS: 15000,
    serverSelectionTimeoutMS: 15000,
  });
  await cachedClient.connect();
  console.log("✅ MongoDB connected →", DB_NAME);
  return cachedClient.db(DB_NAME);
}

// ── ACTION 1: Batch insert FY26 job records ───────────────────────
async function batchInsertJobs(db, records) {
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

      // Clear existing data for this tab
      await col.deleteMany({});

      // Stamp each record with metadata
      const stamped = rows.map((row) => ({
        ...row,
        _tab:        tabName,
        _insertedAt: new Date(),
      }));

      const result = await col.insertMany(stamped, { ordered: false });
      summary[tabName] = result.insertedCount;
      console.log(`✅ ${tabName}: inserted ${result.insertedCount} records`);

    } catch (err) {
      console.error(`❌ ${tabName} insert failed:`, err.message);
      errors.push(`${tabName}: ${err.message}`);
      summary[tabName] = 0;
    }
  }

  return { summary, errors };
}

// ── ACTION 2: Batch upsert users ──────────────────────────────────
async function batchUpsertUsers(db, users) {
  if (!Array.isArray(users) || users.length === 0) {
    return { inserted: 0, updated: 0, errors: ["No users provided"] };
  }

  const col     = db.collection("users");
  const results = { inserted: 0, updated: 0, errors: [] };

  for (const u of users) {
    const email = u.email?.toLowerCase().trim();
    if (!email || !u.name) {
      results.errors.push(`Skipped: missing name or email → ${JSON.stringify(u)}`);
      continue;
    }

    try {
      const result = await col.updateOne(
        { email },
        {
          $set: {
            name:      u.name,
            email,
            role:      u.role     || "user",
            isActive:  u.isActive !== undefined ? u.isActive : true,
            updatedAt: new Date(),
          },
          $setOnInsert: {
            createdAt: new Date(),
            lastLogin: null,
            photoUrl:  "",
          },
        },
        { upsert: true }
      );

      if (result.upsertedCount > 0) {
        results.inserted++;
        console.log("✅ Inserted user:", email);
      } else if (result.modifiedCount > 0) {
        results.updated++;
        console.log("✅ Updated user:", email);
      }

    } catch (err) {
      console.error("❌ User upsert failed:", email, err.message);
      results.errors.push(`${email}: ${err.message}`);
    }
  }

  return results;
}

// ── Main Vercel handler ───────────────────────────────────────────
module.exports = async function handler(req, res) {
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Access-Control-Allow-Methods", "POST, OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type, x-batch-secret");

  if (req.method === "OPTIONS") return res.status(200).end();
  if (req.method !== "POST") {
    return res.status(405).json({ error: "Method not allowed. Use POST." });
  }

  // Verify BATCH_SECRET header
  const incomingSecret = req.headers["x-batch-secret"];
  if (!BATCH_SECRET || incomingSecret !== BATCH_SECRET) {
    console.warn("❌ Unauthorized batch request");
    return res.status(401).json({ error: "Unauthorized. Invalid or missing batch secret." });
  }

  const action = req.query?.action || req.body?.action;
  if (!action) {
    return res.status(400).json({
      error: "action is required. Use ?action=jobs or ?action=users",
    });
  }

  try {
    const db = await getDB();

    // ── jobs ──────────────────────────────────────────────────────
    if (action === "jobs") {
      const { records } = req.body || {};
      if (!records || typeof records !== "object") {
        return res.status(400).json({
          error: 'records object required. Format: { "Sea Export": [...rows] }',
        });
      }

      const totalRows = Object.values(records).reduce((s, r) => s + (r?.length || 0), 0);
      console.log(`📦 Batch jobs: ${Object.keys(records).length} tabs, ${totalRows} rows`);

      const { summary, errors } = await batchInsertJobs(db, records);
      return res.status(200).json({
        success:      true,
        action:       "jobs",
        summary,
        totalInserted: Object.values(summary).reduce((s, n) => s + n, 0),
        ...(errors.length ? { errors } : {}),
      });
    }

    // ── users ─────────────────────────────────────────────────────
    if (action === "users") {
      const { users } = req.body || {};
      if (!users) {
        return res.status(400).json({
          error: "users array required. Format: [{ name, email, role }]",
        });
      }

      console.log(`👥 Batch users: ${users.length} users`);
      const results = await batchUpsertUsers(db, users);
      return res.status(200).json({
        success:  true,
        action:   "users",
        inserted: results.inserted,
        updated:  results.updated,
        ...(results.errors.length ? { errors: results.errors } : {}),
      });
    }

    return res.status(400).json({
      error: `Unknown action: "${action}". Use "jobs" or "users".`,
    });

  } catch (err) {
    console.error("❌ Batch error:", err.message);
    return res.status(500).json({ error: "Batch operation failed", detail: err.message });
  }
};
