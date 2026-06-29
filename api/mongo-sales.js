// ============================================================
//  api/mongo-sales.js  —  Vercel Serverless Function
//  GET  /api/mongo-sales  → returns repsRaw for dashboard
//  POST /api/mongo-sales  (via mongo-batch?action=sales) → upserts
// ============================================================

const { MongoClient } = require('mongodb');

const MONGO_URI    = process.env.MONGO_URI;
const BATCH_SECRET = process.env.BATCH_SECRET;
const DB_NAME      = 'sar-in-air-sales';
const COLLECTION   = 'sales_performance';

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

module.exports = async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, x-batch-secret');

  if (req.method === 'OPTIONS') return res.status(200).end();

  const db = await getDB();

  // ── GET: dashboard fetches live data ──────────────────────
  if (req.method === 'GET') {
    const doc = await db.collection(COLLECTION).findOne(
      {},
      { sort: { pushedAt: -1 } }   // latest push
    );

    if (!doc) {
      return res.status(404).json({ error: 'No sales performance data found' });
    }

    return res.status(200).json({
      success  : true,
      repsRaw  : doc.repsRaw,
      months   : doc.months,
      pushedAt : doc.pushedAt
    });
  }

  // ── POST: Apps Script pushes new data ─────────────────────
  if (req.method === 'POST') {
    const secret = req.headers['x-batch-secret'];
    if (!BATCH_SECRET || secret !== BATCH_SECRET) {
      return res.status(401).json({ error: 'Unauthorized' });
    }

    const { repsRaw, months, pushedAt } = req.body || {};
    if (!repsRaw || !months) {
      return res.status(400).json({ error: 'repsRaw and months are required' });
    }

    // Replace with latest data (upsert by a fixed key)
    await db.collection(COLLECTION).replaceOne(
      { _id: 'current' },
      { _id: 'current', repsRaw, months, pushedAt: pushedAt || new Date().toISOString() },
      { upsert: true }
    );

    return res.status(200).json({
      success : true,
      reps    : repsRaw.length,
      months  : months.length,
      pushedAt
    });
  }

  return res.status(405).json({ error: 'Method not allowed' });
};
