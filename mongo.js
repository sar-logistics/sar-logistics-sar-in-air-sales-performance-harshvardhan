// ─────────────────────────────────────────────────────────────────
//  api/mongo.js  —  Vercel Serverless Function
//  Handles:
//    1. MongoDB connection (reused across warm invocations)
//    2. Google ID token verification
//    3. User auth — checks users collection, updates lastLogin
//
//  Endpoint:  POST /api/mongo
//  Body:      { idToken: "<google_id_token>" }
//  Returns:   { success, user: { name, email, role, photoUrl } }
// ─────────────────────────────────────────────────────────────────

const https           = require("https");
const { MongoClient } = require("mongodb");

const MONGO_URI        = process.env.MONGO_URI;
const GOOGLE_CLIENT_ID = process.env.GOOGLE_CLIENT_ID;
const DB_NAME          = "sar-in-air-sales";
const COLLECTION_USERS = "users";

// Reuse connection across warm Lambda invocations
let cachedClient = null;

async function getDB() {
  if (cachedClient) return cachedClient.db(DB_NAME);
  cachedClient = new MongoClient(MONGO_URI, {
    connectTimeoutMS: 10000,
    serverSelectionTimeoutMS: 10000,
  });
  await cachedClient.connect();
  console.log("✅ MongoDB connected →", DB_NAME);
  return cachedClient.db(DB_NAME);
}

// Verify Google ID token via Google's public tokeninfo endpoint
function verifyGoogleToken(idToken) {
  return new Promise((resolve, reject) => {
    const url = `https://oauth2.googleapis.com/tokeninfo?id_token=${encodeURIComponent(idToken)}`;
    https.get(url, (res) => {
      let raw = "";
      res.on("data", (chunk) => (raw += chunk));
      res.on("end", () => {
        try {
          const payload = JSON.parse(raw);
          if (payload.error) return reject(new Error(`Google: ${payload.error}`));
          resolve(payload);
        } catch (e) {
          reject(new Error("Failed to parse Google response"));
        }
      });
    }).on("error", (e) => reject(new Error(`Google request failed: ${e.message}`)));
  });
}

module.exports = async function handler(req, res) {
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Access-Control-Allow-Methods", "POST, OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type");

  if (req.method === "OPTIONS") return res.status(200).end();
  if (req.method !== "POST") {
    return res.status(405).json({ error: "Method not allowed. Use POST." });
  }

  const { idToken } = req.body || {};
  if (!idToken) {
    return res.status(400).json({ error: "idToken is required in request body." });
  }

  try {
    // 1. Verify with Google
    const payload = await verifyGoogleToken(idToken);

    // 2. Check audience
    if (payload.aud !== GOOGLE_CLIENT_ID) {
      console.warn("Token aud mismatch:", payload.aud);
      return res.status(401).json({ error: "Token audience mismatch. Invalid client." });
    }

    // 3. Extract email
    const email = payload.email?.toLowerCase().trim();
    if (!email) {
      return res.status(401).json({ error: "No email found in Google token." });
    }

    // 4. Look up user in MongoDB
    const db   = await getDB();
    const user = await db.collection(COLLECTION_USERS).findOne(
      { email, isActive: true },
      { projection: { name: 1, email: 1, role: 1, photoUrl: 1 } }
    );

    if (!user) {
      console.warn("Unauthorized access attempt:", email);
      return res.status(403).json({
        error:   "Access denied",
        message: "Your account is not authorized. Contact Harshvardhan Rawat.",
      });
    }

    // 5. Update lastLogin + photoUrl
    await db.collection(COLLECTION_USERS).updateOne(
      { email },
      {
        $set: {
          lastLogin: new Date(),
          ...(payload.picture ? { photoUrl: payload.picture } : {}),
        },
      }
    );

    // 6. Return user to dashboard
    console.log("✅ Auth success:", email, "| role:", user.role);
    return res.status(200).json({
      success: true,
      user: {
        name:     user.name,
        email:    user.email,
        role:     user.role,
        photoUrl: payload.picture || user.photoUrl || "",
      },
    });

  } catch (err) {
    console.error("❌ Auth error:", err.message);
    return res.status(500).json({ error: "Authentication failed", detail: err.message });
  }
};
