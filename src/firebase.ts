import admin from 'firebase-admin';

if (!process.env.FIREBASE_SERVICE_ACCOUNT) {
  throw new Error('FIREBASE_SERVICE_ACCOUNT não definido');
}

const raw = JSON.parse(process.env.FIREBASE_SERVICE_ACCOUNT);

// 🔥 CONVERSÃO CRÍTICA
raw.private_key = raw.private_key.replace(/\\n/g, '\n');

if (!admin.apps.length) {
  admin.initializeApp({
    credential: admin.credential.cert(raw),
  });
}

export const db = admin.firestore();